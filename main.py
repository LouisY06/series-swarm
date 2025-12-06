"""Main event loop for SeriesSwarm Matchmaking Switchboard."""

import os
import sys
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, SeriesAPI
from core.switchboard import Switchboard
from core.commands import CommandHandler
from agents.connector import generate_vcard

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SeriesSwarm:
    """Main application class for SeriesSwarm matchmaking switchboard system."""

    def __init__(self):
        """Initialize SeriesSwarm with Kafka consumer, producer, and switchboard."""
        # Get configuration from environment
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topic_in = os.getenv('KAFKA_TOPIC_IN', 'hackathon-inbound')
        self.kafka_topic_out = os.getenv('KAFKA_TOPIC_OUT', 'hackathon-outbound')
        # Use a unique group ID with timestamp to avoid offset issues
        base_group_id = os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')
        import time
        self.kafka_group_id = f"{base_group_id}-{int(time.time())}"
        self.kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
        self.kafka_sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        self.kafka_sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        self.series_api_key = os.getenv('SERIES_API_KEY')
        self.series_api_url = os.getenv('SERIES_API_URL', 'https://api.series.im')
        self.sender_number = os.getenv('SENDER_NUMBER')

        # Validate required configuration
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER environment variable is required")
        if not self.series_api_key:
            raise ValueError("SERIES_API_KEY environment variable is required")
        if not self.sender_number:
            raise ValueError("SENDER_NUMBER environment variable is required")

        # Initialize components
        logger.info("Initializing SeriesSwarm components...")
        self.consumer = KafkaConsumer(
            self.kafka_broker,
            self.kafka_topic_in,
            group_id=self.kafka_group_id,
            sasl_username=self.kafka_sasl_username,
            sasl_password=self.kafka_sasl_password,
            client_id=self.kafka_client_id
        )
        self.producer = KafkaProducer(
            self.kafka_broker,
            self.kafka_topic_out,
            sasl_username=self.kafka_sasl_username,
            sasl_password=self.kafka_sasl_password,
            client_id=self.kafka_client_id
        )
        self.switchboard = Switchboard()
        self.command_handler = CommandHandler()
        self.series_api = SeriesAPI(self.series_api_key, self.series_api_url)

        self.running = False
        logger.info("SeriesSwarm initialized successfully")

    def send_message(self, chat_id: Optional[int], recipient_phone: Optional[str], text: str, attachment: Optional[Any] = None, filename: str = "", mime_type: str = "") -> bool:
        """
        Send a message via Series API.

        Args:
            chat_id: Existing chat ID (if available)
            recipient_phone: Recipient phone number (if no chat_id)
            text: Message text
            attachment: Optional attachment (BytesIO)
            filename: Attachment filename
            mime_type: Attachment MIME type

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            if chat_id:
                result = self.series_api.send_message_with_attachment(
                    send_from=self.sender_number,
                    chat_id=chat_id,
                    text=text,
                    attachment=attachment,
                    filename=filename,
                    mime_type=mime_type
                )
            elif recipient_phone:
                result = self.series_api.send_message_with_attachment(
                    send_from=self.sender_number,
                    phone_numbers=[recipient_phone],
                    text=text,
                    attachment=attachment,
                    filename=filename,
                    mime_type=mime_type
                )
            else:
                logger.error("Cannot send message: no chat_id or recipient_phone")
                return False

            return result is not None
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single message through the switchboard.

        Args:
            message: Message dictionary from Kafka consumer

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            message_value = message.get('value', {})
            data = message_value.get('data', {})
            text = data.get('text', message_value.get('text', ''))
            from_phone = data.get('from_phone')
            chat_id = self.series_api.get_chat_id_from_message(message_value)

            # Only process message.received events
            event_type = message_value.get('event_type')
            if event_type != 'message.received':
                logger.debug(f"Skipping event type: {event_type}")
                return True

            if not from_phone:
                logger.warning("No from_phone in message, skipping")
                return False

            if not text:
                logger.warning("Empty message text, skipping")
                return True

            # Store chat_id for this user
            if chat_id:
                self.switchboard.store_chat_id(from_phone, chat_id)

            text = text.strip()
            logger.info(f"Processing message from {from_phone}: {text[:50]}...")

            # Check if it's a command
            cmd_result = self.command_handler.parse_command(text)
            if cmd_result:
                cmd_name, cmd_arg = cmd_result
                return self.handle_command(from_phone, chat_id, cmd_name, cmd_arg, message_value)

            # Check if user is paired
            if self.switchboard.is_paired(from_phone):
                # Relay message to partner
                return self.relay_message(from_phone, text, chat_id, message_value)
            else:
                # User is new or waiting - try to find match
                return self.handle_new_user(from_phone, text, chat_id, message_value)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def handle_command(self, user_phone: str, chat_id: Optional[int], cmd_name: str, cmd_arg: Optional[str], message_value: Dict[str, Any]) -> bool:
        """Handle user commands."""
        logger.info(f"Command from {user_phone}: {cmd_name} {cmd_arg or ''}")

        # Commands that modify switchboard state
        if cmd_name == 'next':
            partner = self.switchboard.end_chat(user_phone)
            if partner:
                # Notify partner
                partner_chat_id = self.switchboard.get_chat_id(partner)
                self.send_message(partner_chat_id, partner, "🚫 Stranger disconnected. Type 'hi' to find a new match.")
                # Find new match for user
                self.find_and_notify_match(user_phone, chat_id)
            else:
                # User wasn't paired, just find match
                self.find_and_notify_match(user_phone, chat_id)
            return True

        elif cmd_name == 'reveal':
            both_agreed, partner = self.switchboard.handle_reveal(user_phone)
            if both_agreed and partner:
                # Both agreed! Send vCards
                return self.exchange_contacts(user_phone, partner, message_value)
            elif partner:
                # Waiting for partner
                self.send_message(chat_id, user_phone, "🔒 Waiting for partner to accept reveal...")
                partner_chat_id = self.switchboard.get_chat_id(partner)
                self.send_message(partner_chat_id, partner, "👀 Your partner wants to share numbers! Type '/reveal' to accept.")
            else:
                self.send_message(chat_id, user_phone, "❌ You're not currently paired with anyone.")
            return True

        else:
            # Commands that return text responses
            response = self.command_handler.execute_command(self.switchboard, user_phone, cmd_name, cmd_arg)
            if response:
                self.send_message(chat_id, user_phone, response)
            return True

    def relay_message(self, from_phone: str, text: str, chat_id: Optional[int], message_value: Dict[str, Any]) -> bool:
        """Relay a message from one user to their partner."""
        partner = self.switchboard.get_partner(from_phone)
        if not partner:
            logger.warning(f"User {from_phone} claims to be paired but no partner found")
            return False

        # Get partner's chat_id
        partner_chat_id = self.switchboard.get_chat_id(partner)
        relay_text = f"Stranger: {text}"
        
        # Send to partner's chat_id if available, otherwise use phone
        success = self.send_message(partner_chat_id, partner, relay_text)
        
        if success:
            logger.info(f"Relayed message from {from_phone} to {partner}")
        else:
            logger.error(f"Failed to relay message from {from_phone} to {partner}")

        return success

    def handle_new_user(self, user_phone: str, text: str, chat_id: Optional[int], message_value: Dict[str, Any]) -> bool:
        """Handle a new user or user in waiting queue."""
        if self.switchboard.is_waiting(user_phone):
            # User is already waiting
            self.send_message(chat_id, user_phone, "🔎 Still searching for a match... Hang tight!")
            return True
        else:
            # New user - try to find match
            return self.find_and_notify_match(user_phone, chat_id)

    def find_and_notify_match(self, user_phone: str, chat_id: Optional[int]) -> bool:
        """Find a match for user and notify both parties."""
        partner = self.switchboard.find_match(user_phone)
        
        if partner:
            # Matched!
            connected_msg = "🎉 You are connected! Say hi. (Type '/next' to skip, '/reveal' to share numbers)"
            self.send_message(chat_id, user_phone, connected_msg)
            
            # Notify partner
            partner_chat_id = self.switchboard.get_chat_id(partner)
            self.send_message(partner_chat_id, partner, connected_msg)
            
            logger.info(f"Matched {user_phone} with {partner}")
            return True
        else:
            # Added to queue
            self.send_message(chat_id, user_phone, "🔎 Searching for a match... Hang tight.")
            return True

    def exchange_contacts(self, user1_phone: str, user2_phone: str, message_value: Dict[str, Any]) -> bool:
        """Exchange vCards between two users when both agree to reveal."""
        try:
            # Get user profiles
            user1_profile = self.switchboard.get_user_profile(user1_phone)
            user2_profile = self.switchboard.get_user_profile(user2_phone)

            # Generate vCards
            user1_vcard = generate_vcard(user1_profile)
            user2_vcard = generate_vcard(user2_profile)

            # Get chat IDs
            user1_chat_id = self.switchboard.get_chat_id(user1_phone)
            user2_chat_id = self.switchboard.get_chat_id(user2_phone)

            # Send vCards
            success1 = self.send_message(
                user1_chat_id, user1_phone,
                "💖 It's a match! Contact card sent.",
                attachment=user2_vcard,
                filename="contact.vcf",
                mime_type="text/vcard"
            )

            success2 = self.send_message(
                user2_chat_id, user2_phone,
                "💖 It's a match! Contact card sent.",
                attachment=user1_vcard,
                filename="contact.vcf",
                mime_type="text/vcard"
            )

            if success1 and success2:
                logger.info(f"Exchanged contacts between {user1_phone} and {user2_phone}")
                return True
            else:
                logger.error(f"Failed to exchange contacts: success1={success1}, success2={success2}")
                return False

        except Exception as e:
            logger.error(f"Error exchanging contacts: {e}", exc_info=True)
            return False

    def run(self):
        """Run the main event loop."""
        self.running = True
        logger.info("Starting SeriesSwarm matchmaking switchboard...")

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            while self.running:
                # Consume message
                message = self.consumer.consume(timeout=1.0)

                if message:
                    self.process_message(message)
                # If no message, continue polling

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in event loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def shutdown(self):
        """Gracefully shutdown all components."""
        logger.info("Shutting down SeriesSwarm...")
        stats = self.switchboard.get_stats()
        logger.info(f"Final stats: {stats}")
        self.consumer.close()
        self.producer.close()
        logger.info("SeriesSwarm shutdown complete")


def main():
    """Main entry point."""
    try:
        app = SeriesSwarm()
        app.run()
    except Exception as e:
        logger.error(f"Failed to start SeriesSwarm: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
