"""SeriesSwarm - Anonymous Matchmaking Switchboard."""

import os
import sys
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, SeriesAPI, Switchboard, CommandHandler
from agents import generate_vcard
from ai_utils import AIUtils

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # INFO level to see all important messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SeriesSwarm:
    """Main application class."""

    def __init__(self):
        """Initialize components."""
        # Load config
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topic = os.getenv('KAFKA_TOPIC_IN', 'hackathon-inbound')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')
        self.kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
        self.kafka_sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        self.kafka_sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        self.series_api_key = os.getenv('SERIES_API_KEY')
        self.sender_number = os.getenv('SENDER_NUMBER')

        # Validate
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER required")
        if not self.series_api_key:
            raise ValueError("SERIES_API_KEY required")
        if not self.sender_number:
            raise ValueError("SENDER_NUMBER required")

        # Initialize components
        logger.info("Initializing SeriesSwarm...")
        
        self.consumer = KafkaConsumer(
            self.kafka_broker,
            self.kafka_topic,
            group_id=self.kafka_group_id,
            sasl_username=self.kafka_sasl_username,
            sasl_password=self.kafka_sasl_password,
            client_id=self.kafka_client_id
        )
        
        self.switchboard = Switchboard()
        self.commands = CommandHandler()
        self.api = SeriesAPI(self.series_api_key)
        self.ai_utils = AIUtils()  # For profile extraction
        self.running = False
        
        # Only process messages from these two phone numbers
        self.allowed_phones = {
            '+16673522441',  # Your number
            '+13479546596'   # Teammate's number
        }
        logger.info(f"Allowed phone numbers: {self.allowed_phones}")

        logger.info("SeriesSwarm initialized")

    def send(self, chat_id: Optional[int], phone: str, text: str, 
             attachment=None, filename="", mime_type="") -> bool:
        """Send a message."""
        result = self.api.send_message(
            send_from=self.sender_number,
            chat_id=chat_id,
            phone_numbers=[phone] if not chat_id else None,
            text=text,
            attachment=attachment,
            filename=filename,
            mime_type=mime_type
        )
        return result is not None

    def process_message(self, message: Dict[str, Any]) -> bool:
        """Process an incoming message."""
        try:
            value = message.get('value', {})
            data = value.get('data', {})
            
            # Only process message.received events
            event_type = value.get('event_type')
            if event_type != 'message.received':
                return True  # Skip silently

            # EARLY FILTER: Extract from_phone FIRST and check if it's allowed
            # This prevents processing ANY messages from other teams/users
            from_phone = data.get('from_phone')
            
            if not from_phone:
                return True  # Skip silently
            
            # Skip our own messages immediately
            if from_phone == self.sender_number:
                return True  # Skip silently

            # CRITICAL FILTER: Only process messages from allowed phone numbers
            # This is the main filter to limit consumption to only you and your teammate
            if from_phone not in self.allowed_phones:
                return True  # Skip silently - don't log messages from other teams
            
            # Now we know it's from an allowed number, so log and process
            logger.info(f"Message from allowed number {from_phone}")
            logger.info(f"   Full data: {data}")

            text = data.get('text', '').strip()
            to_phone = data.get('to_phone')
            chat_handles = data.get('chat_handles', [])
            chat_id = self.api.get_chat_id(value)

            # EARLY FILTER: Check if this message is for our number BEFORE any processing
            # This prevents processing messages from other teams/users
            our_number_in_chat = False
            if to_phone == self.sender_number:
                our_number_in_chat = True
            else:
                # Check chat_handles for our number
                for handle in chat_handles:
                    if isinstance(handle, dict):
                        handle_id = handle.get('identifier')
                        if handle_id == self.sender_number:
                            our_number_in_chat = True
                            break

            # Check if user is already paired - if so, allow messages from any number they text
            is_paired = self.switchboard.is_paired(from_phone)
            logger.info(f"Pairing check: {from_phone} is_paired={is_paired}, our_number_in_chat={our_number_in_chat}")
            
            # If not paired AND message not for our number, skip immediately
            if not is_paired and not our_number_in_chat:
                logger.warning(f"SKIPPING - message not for our number ({self.sender_number}). From: {from_phone}, is_paired={is_paired}, our_number_in_chat={our_number_in_chat}")
                return True

            # Only log details for messages we're actually processing
            logger.info(f"Message details - from: {from_phone}, to: {to_phone}, text: '{text[:50] if text else '(empty)'}', chat_id: {chat_id}")
            logger.info(f"   Chat handles: {chat_handles}")
            
            if is_paired:
                logger.info(f"User {from_phone} is paired - processing message regardless of number")
                # Store chat_id for paired users
                if chat_id:
                    self.switchboard.store_chat_id(from_phone, chat_id)
                    logger.info(f"   Stored chat_id {chat_id} for {from_phone}")
                
                if not text:
                    logger.info("Message has no text, skipping")
                    return True
                
                logger.info(f"Processing message from paired user {from_phone}: '{text[:50]}'")
                
                # Check for commands first (even for paired users)
                cmd = self.commands.parse_command(text)
                if cmd:
                    logger.info(f"   Command detected: {cmd[0]}")
                    return self.handle_command(from_phone, chat_id, cmd[0], cmd[1])
                
                # If not a command, relay the message
                logger.info(f"   Not a command, relaying to partner...")
                result = self.relay_message(from_phone, text)
                if result:
                    logger.info(f"   Successfully relayed message")
                else:
                    logger.error(f"   Failed to relay message - no partner found!")
                return result

            # For new/unpaired users: ONLY process messages for the 646 number (+16463230991)
            our_number_in_chat = False
            
            # Check if to_phone matches our configured number
            if to_phone == self.sender_number:
                our_number_in_chat = True
                logger.info(f"   to_phone matches our number: {self.sender_number}")
            
            # Check chat_handles - ONLY accept if identifier matches our configured number
            for handle in chat_handles:
                if isinstance(handle, dict):
                    handle_id = handle.get('identifier')
                    # ONLY process if this handle matches our configured number
                    if handle_id == self.sender_number:
                        our_number_in_chat = True
                        logger.info(f"   Found our number in chat: {handle_id}")
                        break
            
            if not our_number_in_chat:
                logger.warning(f"SKIPPING - message not for our number ({self.sender_number}). From: {from_phone}, Handles: {chat_handles}, to_phone: {to_phone}")
                logger.warning(f"   This message will NOT be processed. User must text {self.sender_number} to match.")
                return True

            logger.info(f"PROCESSING message for {self.sender_number} from {from_phone}")

            if not text:
                logger.info("Message has no text, skipping")
                return True

            # Store chat_id
            if chat_id:
                self.switchboard.store_chat_id(from_phone, chat_id)

            logger.info(f"Message from {from_phone}: {text[:50]}")

            # Check for command
            logger.info(f"Checking if '{text}' is a command...")
            cmd = self.commands.parse_command(text)
            if cmd:
                logger.info(f"Command detected: {cmd[0]} (arg: {cmd[1]})")
                result = self.handle_command(from_phone, chat_id, cmd[0], cmd[1])
                logger.info(f"   Command handler returned: {result}")
                return result
            else:
                logger.info(f"   Not a command, treating as regular message")

            # Check if user has a complete profile
            existing_profile = self.switchboard.get_user_profile(from_phone)
            has_name = bool(existing_profile.get('name'))
            has_email = bool(existing_profile.get('email'))
            has_phone = bool(existing_profile.get('phone'))
            has_interests = bool(existing_profile.get('interests'))
            is_profile_incomplete = not has_name or not has_email or not has_phone or not has_interests
            
            if is_profile_incomplete:
                # Extract profile from their message using OpenAI
                logger.info(f"Profile incomplete for {from_phone}, extracting info from message...")
                extracted_profile = self.ai_utils.extract_user_profile(text, from_phone)
                
                # Store extracted profile (only update missing fields)
                if extracted_profile:
                    update_data = {}
                    if not has_name and extracted_profile.get('name'):
                        update_data['name'] = extracted_profile['name']
                    if not has_email and extracted_profile.get('email'):
                        update_data['email'] = extracted_profile['email']
                    if not has_phone and extracted_profile.get('phone'):
                        update_data['phone'] = extracted_profile['phone']
                    elif not has_phone:
                        # Use sender's phone as default if not provided
                        update_data['phone'] = from_phone
                    if not has_interests and extracted_profile.get('interests'):
                        update_data['interests'] = extracted_profile['interests']
                    
                    if update_data:
                        self.switchboard.store_user_profile(from_phone, update_data)
                        logger.info(f"Updated profile for {from_phone}: {update_data}")
                
                # Check what's still missing
                profile = self.switchboard.get_user_profile(from_phone)
                missing_info = []
                
                if not profile.get('name'):
                    missing_info.append("your name")
                if not profile.get('email'):
                    missing_info.append("your email")
                if not profile.get('phone'):
                    missing_info.append("your phone number")
                if not profile.get('interests'):
                    missing_info.append("your interests/hobbies")
                
                if missing_info:
                    missing_str = " and ".join(missing_info)
                    response = f"I'm still building your profile. I need {missing_str}. Could you share that? (Or type /match to skip and match now)"
                else:
                    # Profile is now complete
                    name = profile.get('name', 'there')
                    response = f"Perfect, {name}! Your profile is complete. Type /match to find someone to chat with!"
                
                self.send(chat_id, from_phone, response)
                return True
            else:
                # Profile is complete - prompt to match
                self.send(chat_id, from_phone, "Type /match to find someone to chat with!")
                return True

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def handle_command(self, phone: str, chat_id: Optional[int], cmd: str, arg: Optional[str]) -> bool:
        """Handle a command."""
        logger.info(f"Command from {phone}: {cmd} {arg or ''}")

        if cmd == 'match':
            if self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "Already connected! Type /end to disconnect.")
                return True

            logger.info(f"Finding match for {phone}...")
            logger.info(f"   Current queue: {self.switchboard.waiting_queue}")
            logger.info(f"   Active pairs: {list(self.switchboard.active_pairs.keys())}")
            
            partner = self.switchboard.find_match(phone)
            if partner:
                msg = "Connected! Say hi. (Type /end to disconnect, /reveal to share contact)"
                logger.info(f"Sending match notification to {phone} (chat_id: {chat_id})")
                result1 = self.send(chat_id, phone, msg)
                logger.info(f"   Result: {result1}")
                partner_chat = self.switchboard.get_chat_id(partner)
                if partner_chat:
                    logger.info(f"Sending match notification to {partner} (chat_id: {partner_chat})")
                    result2 = self.send(partner_chat, partner, msg)
                    logger.info(f"   Result: {result2}")
                    logger.info(f"Matched {phone} with {partner} - both notified")
                else:
                    logger.warning(f"Matched {phone} with {partner} but partner has no chat_id!")
                    # Try to send anyway with phone number
                    self.send(None, partner, msg)
            else:
                queue_size = len(self.switchboard.waiting_queue)
                logger.info(f"Sending 'searching' message to {phone} (chat_id: {chat_id})")
                result = self.send(chat_id, phone, f"Searching for a match... (Queue: {queue_size})")
                logger.info(f"   Result: {result}")
                logger.info(f"{phone} added to queue (size: {queue_size})")
            return True

        elif cmd == 'end':
            partner = self.switchboard.end_chat(phone)
            if partner:
                partner_chat = self.switchboard.get_chat_id(partner)
                self.send(partner_chat, partner, "Stranger disconnected. Type /match to find someone new.")
                self.send(chat_id, phone, "Disconnected. Type /match to find someone new.")
            else:
                self.send(chat_id, phone, "You're not connected to anyone.")
            return True

        elif cmd == 'reveal':
            if not self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "You're not connected. Type /match first.")
                return True

            both, partner = self.switchboard.handle_reveal(phone)
            if both and partner:
                self.exchange_contacts(phone, partner)
            elif partner:
                self.send(chat_id, phone, "Waiting for partner to accept...")
                partner_chat = self.switchboard.get_chat_id(partner)
                self.send(partner_chat, partner, "Partner wants to share contact! Type /reveal to accept.")
            return True

        else:
            response = self.commands.execute_command(self.switchboard, phone, cmd, arg)
            if response:
                self.send(chat_id, phone, response)
            return True

    def relay_message(self, from_phone: str, text: str) -> bool:
        """Relay message to partner."""
        partner = self.switchboard.get_partner(from_phone)
        if not partner:
            logger.error(f"No partner found for {from_phone} - cannot relay")
            return False

        partner_chat = self.switchboard.get_chat_id(partner)
        logger.info(f"Relaying '{text[:50]}' from {from_phone} to {partner} (chat_id: {partner_chat})")
        
        result = self.send(partner_chat, partner, f"Stranger: {text}")
        if result:
            logger.info(f"Successfully relayed message from {from_phone} to {partner}")
        else:
            logger.error(f"Failed to send relayed message to {partner}")
        return result

    def exchange_contacts(self, user1: str, user2: str) -> bool:
        """Exchange vCards between users."""
        try:
            profile1 = self.switchboard.get_user_profile(user1)
            profile2 = self.switchboard.get_user_profile(user2)

            # Ensure names
            if not profile1.get('name'):
                profile1['name'] = f"User {user1[-4:]}"
            if not profile2.get('name'):
                profile2['name'] = f"User {user2[-4:]}"
            
            # Ensure phone numbers (use profile phone if available, otherwise use user phone)
            if not profile1.get('phone'):
                profile1['phone'] = user1
            if not profile2.get('phone'):
                profile2['phone'] = user2

            vcard1 = generate_vcard(profile1)
            vcard2 = generate_vcard(profile2)

            chat1 = self.switchboard.get_chat_id(user1)
            chat2 = self.switchboard.get_chat_id(user2)

            # Send vCards
            self.send(chat1, user1, "Contact shared!", vcard2, "contact.vcf", "text/vcard")
            self.send(chat2, user2, "Contact shared!", vcard1, "contact.vcf", "text/vcard")

            logger.info(f"Exchanged contacts: {user1} <-> {user2}")
            return True

        except Exception as e:
            logger.error(f"Error exchanging contacts: {e}")
            return False

    def run(self):
        """Run the main loop."""
        self.running = True
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, 'running', False))

        logger.info("SeriesSwarm running...")

        try:
            poll_count = 0
            while self.running:
                try:
                    # Poll with shorter timeout for faster response (0.5 seconds)
                    message = self.consumer.consume(timeout=0.5)
                    poll_count += 1
                    if poll_count % 60 == 0:  # Log every 60 polls (30 seconds)
                        logger.info(f"Still polling... (polled {poll_count} times, waiting for messages)")
                    if message:
                        poll_count = 0  # Reset counter when we get a message
                        logger.info(f"Received message from Kafka")
                        self.process_message(message)
                except Exception as e:
                    logger.error(f"Error in consume loop: {e}", exc_info=True)
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            logger.info("SeriesSwarm stopped")


def main():
    """Entry point."""
    import fcntl
    import tempfile
    
    # Ensure only one instance is running
    lock_file = os.path.join(tempfile.gettempdir(), 'series-swarm.lock')
    try:
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info("Acquired lock - only one instance will run")
    except (IOError, OSError):
        logger.error("Another instance is already running! Exiting.")
        sys.exit(1)
    
    try:
        app = SeriesSwarm()
        app.run()
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
            os.unlink(lock_file)
        except:
            pass


if __name__ == '__main__':
    main()
