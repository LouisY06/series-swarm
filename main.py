"""SeriesSwarm - Anonymous Matchmaking Switchboard."""

import os
import sys
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, SeriesAPI, Switchboard, CommandHandler
from agents import generate_vcard
from state import StateManager, Status
from ai_utils import AIUtils
from collections import defaultdict

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
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
        self.state = StateManager()
        self.ai = AIUtils()
        self.running = False

        logger.info("SeriesSwarm initialized")

    def send(self, chat_id: Optional[int], phone: str, text: str, 
             attachment=None, filename="", mime_type="") -> bool:
        """Send a message. Always prefer chat_id to avoid creating new chats."""
        # Try to get chat_id from stored state if not provided
        if not chat_id:
            chat_id = self.state.get_chat_id(phone) or self.switchboard.get_chat_id(phone)
            if chat_id:
                logger.debug(f"Retrieved stored chat_id {chat_id} for {phone}")
        
        # CRITICAL: Never create new chats - only send if we have a chat_id
        # Creating new chats triggers Series API to send contact cards automatically
        if not chat_id:
            logger.error(f"Cannot send message to {phone}: no chat_id available. Message: {text[:50]}")
            return False
        
        logger.info(f"Sending message to {phone} (chat_id: {chat_id}): {text[:50]}")
        result = self.api.send_message(
            send_from=self.sender_number,
            chat_id=chat_id,
            phone_numbers=None,  # Never create new chats - always use chat_id
            text=text,
            attachment=attachment,
            filename=filename,
            mime_type=mime_type
        )
        if result:
            logger.info(f"Successfully sent message to {phone}")
        else:
            logger.error(f"Failed to send message to {phone}")
        return result is not None

    def process_message(self, message: Dict[str, Any]) -> bool:
        """Process an incoming message."""
        try:
            value = message.get('value', {})
            data = value.get('data', {})
            
            # Only process message.received events
            event_type = value.get('event_type')
            logger.debug(f"Received event type: {event_type}, full message: {message}")
            if event_type != 'message.received':
                logger.debug(f"Skipping non-message event: {event_type}")
                return True

            text = data.get('text', '').strip()
            from_phone = data.get('from_phone')
            chat_id = self.api.get_chat_id(value)
            logger.info(f"Processing message - from: {from_phone}, text: {text[:50]}, chat_id: {chat_id}")

            if not from_phone:
                return False

            # Skip our own messages
            if from_phone == self.sender_number:
                return True

            # HACKATHON FIX: Only process messages sent TO our number
            chat_handles = data.get('chat_handles', [])
            our_number_in_chat = False
            for handle in chat_handles:
                if handle.get('identifier') == self.sender_number:
                    our_number_in_chat = True
                    break
            
            if not our_number_in_chat:
                logger.debug(f"Skipping message not for our number")
                return True
            
            logger.info(f"Processing message for {self.sender_number}")

            if not text:
                return True

            # Store chat_id in both systems (critical for routing)
            if chat_id:
                self.switchboard.store_chat_id(from_phone, chat_id)
                self.state.store_chat_id(from_phone, chat_id)
                logger.info(f"Stored chat_id {chat_id} for {from_phone}")
            else:
                logger.warning(f"No chat_id in message from {from_phone} - this may cause issues")

            logger.info(f"Message from {from_phone}: {text[:50]}")

            # Check for command
            cmd = self.commands.parse_command(text)
            if cmd:
                return self.handle_command(from_phone, chat_id, cmd[0], cmd[1])

            # Handle based on user status
            status = self.state.get_status(from_phone)
            
            if status == Status.ONBOARDING:
                return self.handle_onboarding(from_phone, chat_id, text)
            elif status == Status.CHATTING:
                return self.handle_chat_message(from_phone, chat_id, text)
            else:
                # Default: prompt for onboarding or match
                if status == Status.IDLE:
                    return self.start_onboarding(from_phone, chat_id)
                else:
                    self.send(chat_id, from_phone, "👋 Type /match to find someone to chat with, or /help for commands.")
                return True

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def handle_command(self, phone: str, chat_id: Optional[int], cmd: str, arg: Optional[str]) -> bool:
        """Handle a command."""
        logger.info(f"Command from {phone}: {cmd} {arg or ''}")

        if cmd == 'match':
            status = self.state.get_status(phone)
            
            if status == Status.CHATTING:
                self.send(chat_id, phone, "You're already in a conversation! Type /end to disconnect.")
                return True
            
            if status == Status.ONBOARDING:
                self.send(chat_id, phone, "Please finish your intro first, then send /match.")
                return True
            
            if status != Status.READY:
                # Ensure they have an intro
                if not self.state.get_intro(phone):
                    return self.start_onboarding(phone, chat_id)
                self.state.set_status(phone, Status.READY)
            
            # Check waiting queue
            partner = self.state.pop_from_queue()
            
            if partner:
                # Match found!
                self.match_two_users(phone, partner)
            else:
                # Add to queue
                self.state.set_status(phone, Status.SEARCHING)
                self.state.add_to_queue(phone)
                self.send(chat_id, phone, "🔎 Searching for a match...")
            
            return True

        elif cmd == 'card':
            return self.handle_card(phone, chat_id)
        
        elif cmd == 'end':
            partner = self.state.get_partner(phone)
            if partner:
                # Clear from both systems
                self.state.clear_partner(phone)
                self.switchboard.end_chat(phone)
                self.state.set_status(phone, Status.READY)
                self.state.set_status(partner, Status.READY)
                
                partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
                if partner_chat:
                    self.send(partner_chat, partner, "🚫 Stranger disconnected. Type /match to find someone new.")
                self.send(chat_id, phone, "✅ Disconnected. Type /match to find someone new.")
            else:
                self.send(chat_id, phone, "❌ You're not connected to anyone.")
            return True

        elif cmd == 'reveal':
            if not self.state.get_partner(phone) and not self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "❌ You're not connected. Type /match first.")
                return True

            both, partner = self.switchboard.handle_reveal(phone)
            if both and partner:
                self.exchange_contacts(phone, partner)
            elif partner:
                self.send(chat_id, phone, "🔒 Waiting for partner to accept...")
                partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
                if partner_chat:
                    self.send(partner_chat, partner, "👀 Partner wants to share contact! Type /reveal to accept.")
            return True

        else:
            response = self.commands.execute_command(self.switchboard, phone, cmd, arg)
            if response:
                self.send(chat_id, phone, response)
            return True

    def start_onboarding(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Start onboarding for a new user."""
        self.state.set_status(user_id, Status.ONBOARDING)
        welcome_msg = """Welcome! I help you meet someone and slowly build a profile of them based only on your chat.

First, send me a short intro about yourself and what you are looking for here."""
        self.send(chat_id, user_id, welcome_msg)
        return True

    def handle_onboarding(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle onboarding intro."""
        # Store intro
        self.state.set_intro(user_id, text)
        self.state.set_status(user_id, Status.READY)
        
        response = """Got it. When you want to meet someone, send /match.

Anytime you want, send /help for commands."""
        self.send(chat_id, user_id, response)
        logger.info(f"User {user_id} completed onboarding")
        return True

    def match_two_users(self, user_a: str, user_b: str):
        """Match two users."""
        logger.info(f"Matching {user_a} with {user_b}")
        
        # Set partners and status
        self.state.set_partner(user_a, user_b)
        self.state.set_status(user_a, Status.CHATTING)
        self.state.set_status(user_b, Status.CHATTING)
        
        # Also update switchboard for compatibility
        self.switchboard.active_pairs[user_a] = user_b
        self.switchboard.active_pairs[user_b] = user_a
        
        # Send connection messages - use send() which will retrieve chat_id from stored state
        # This avoids creating new chats if chat_id wasn't passed
        msg = "🎉 Connected! Say hi. (Type /end to disconnect, /card to see their profile)"
        
        # Get chat_ids but let send() handle retrieval if None
        chat_a = self.state.get_chat_id(user_a) or self.switchboard.get_chat_id(user_a)
        chat_b = self.state.get_chat_id(user_b) or self.switchboard.get_chat_id(user_b)
        
        if chat_a is None:
            logger.warning(f"No chat_id stored for {user_a} - will create new chat if send() can't find it")
        if chat_b is None:
            logger.warning(f"No chat_id stored for {user_b} - will create new chat if send() can't find it")
        
        # send() will try to retrieve chat_id from state if None, but if still None, it creates a new chat
        # This is the problem - we should require chat_id before matching
        self.send(chat_a, user_a, msg)
        self.send(chat_b, user_b, msg)
        logger.info(f"Matched {user_a} with {user_b}")

    def handle_chat_message(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle a chat message between matched users."""
        partner = self.state.get_partner(user_id)
        if not partner:
            self.state.set_status(user_id, Status.READY)
            self.send(chat_id, user_id, "You're not in a conversation. Send /match to meet someone.")
            return True
        
        # Record message
        self.state.record_message(user_id, partner, text)
        
        # Update profiles if needed
        total_messages = self.state.get_message_count(user_id, partner)
        self.ai.ensure_profile_updated(self.state, user_id, partner, total_messages)
        self.ai.ensure_profile_updated(self.state, partner, user_id, total_messages)
        
        # Forward to partner
        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        if partner_chat:
            self.send(partner_chat, partner, text)
        else:
            logger.error(f"Cannot relay message to {partner}: chat_id not found")
            return False
        
        logger.info(f"Relayed message from {user_id} to {partner} (total: {total_messages})")
        return True

    def handle_card(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Handle /card command - show profile of the other person."""
        partner = self.state.get_partner(user_id)
        
        if not partner:
            self.send(chat_id, user_id, "You are not in a conversation. Send /match to meet someone.")
            return True
        
        profile = self.state.get_profile(user_id, partner)
        
        if profile["level"] == 0:
            self.send(chat_id, user_id, 
                "I have not learned enough yet. Keep talking to unlock their profile.")
        else:
            msg = "Here is what I have learned about them from your conversation so far:\n\n" + profile["resume"]
            self.send(chat_id, user_id, msg)
        
        return True

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

            vcard1 = generate_vcard(profile1)
            vcard2 = generate_vcard(profile2)

            chat1 = self.switchboard.get_chat_id(user1)
            chat2 = self.switchboard.get_chat_id(user2)

            # Send vCards
            self.send(chat1, user1, "💖 Contact shared!", vcard2, "contact.vcf", "text/vcard")
            self.send(chat2, user2, "💖 Contact shared!", vcard1, "contact.vcf", "text/vcard")

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
        logger.info(f"Listening on topic: {self.kafka_topic}, group: {self.kafka_group_id}")

        try:
            poll_count = 0
            while self.running:
                message = self.consumer.consume(timeout=1.0)
                poll_count += 1
                if poll_count % 60 == 0:  # Log every 60 seconds
                    logger.info(f"Still polling... (poll #{poll_count})")
                if message:
                    logger.info(f"Received Kafka message: {message}")
                    self.process_message(message)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            logger.info("SeriesSwarm stopped")


def main():
    """Entry point."""
    try:
        app = SeriesSwarm()
        app.run()
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
