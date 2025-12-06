"""SeriesSwarm - Anonymous Matchmaking Switchboard."""

import os
import sys
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, SeriesAPI, Switchboard, CommandHandler
from agents import generate_vcard

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
        self.running = False

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
                return True

            text = data.get('text', '').strip()
            from_phone = data.get('from_phone')
            chat_id = self.api.get_chat_id(value)

            if not from_phone:
                return False

            # Skip our own messages
            if from_phone == self.sender_number:
                return True

            if not text:
                return True

            # Store chat_id
            if chat_id:
                self.switchboard.store_chat_id(from_phone, chat_id)

            logger.info(f"Message from {from_phone}: {text[:50]}")

            # Check for command
            cmd = self.commands.parse_command(text)
            if cmd:
                return self.handle_command(from_phone, chat_id, cmd[0], cmd[1])

            # Relay if paired
            if self.switchboard.is_paired(from_phone):
                return self.relay_message(from_phone, text)

            # Otherwise, prompt to match
            self.send(chat_id, from_phone, "👋 Type /match to find someone to chat with!")
            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def handle_command(self, phone: str, chat_id: Optional[int], cmd: str, arg: Optional[str]) -> bool:
        """Handle a command."""
        logger.info(f"Command from {phone}: {cmd} {arg or ''}")

        if cmd == 'match':
            if self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "✅ Already connected! Type /end to disconnect.")
                return True

            partner = self.switchboard.find_match(phone)
            if partner:
                msg = "🎉 Connected! Say hi. (Type /end to disconnect, /reveal to share contact)"
                self.send(chat_id, phone, msg)
                partner_chat = self.switchboard.get_chat_id(partner)
                self.send(partner_chat, partner, msg)
                logger.info(f"Matched {phone} with {partner}")
            else:
                self.send(chat_id, phone, "🔎 Searching for a match...")
            return True

        elif cmd == 'end':
            partner = self.switchboard.end_chat(phone)
            if partner:
                partner_chat = self.switchboard.get_chat_id(partner)
                self.send(partner_chat, partner, "🚫 Stranger disconnected. Type /match to find someone new.")
                self.send(chat_id, phone, "✅ Disconnected. Type /match to find someone new.")
            else:
                self.send(chat_id, phone, "❌ You're not connected to anyone.")
            return True

        elif cmd == 'reveal':
            if not self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "❌ You're not connected. Type /match first.")
                return True

            both, partner = self.switchboard.handle_reveal(phone)
            if both and partner:
                self.exchange_contacts(phone, partner)
            elif partner:
                self.send(chat_id, phone, "🔒 Waiting for partner to accept...")
                partner_chat = self.switchboard.get_chat_id(partner)
                self.send(partner_chat, partner, "👀 Partner wants to share contact! Type /reveal to accept.")
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
            return False

        partner_chat = self.switchboard.get_chat_id(partner)
        self.send(partner_chat, partner, f"Stranger: {text}")
        logger.info(f"Relayed message from {from_phone} to {partner}")
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

        try:
            while self.running:
                message = self.consumer.consume(timeout=1.0)
                if message:
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
