"""Main event loop for SeriesSwarm."""

import os
import sys
import json
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, Router, SeriesAPI

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SeriesSwarm:
    """Main application class for SeriesSwarm event-driven system."""

    def __init__(self):
        """Initialize SeriesSwarm with Kafka consumer, producer, and router."""
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
        self.kafka_sasl_password = os.getenv('KAFKA_SASL_PASSWORD')  # API Key for Confluent Cloud
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.series_api_key = os.getenv('SERIES_API_KEY')  # Series API key for sending messages
        self.series_api_url = os.getenv('SERIES_API_URL', 'https://api.series.im')
        self.sender_number = os.getenv('SENDER_NUMBER')  # Phone number to send from

        # Validate required configuration
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER environment variable is required")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        if not self.series_api_key:
            logger.warning("SERIES_API_KEY not set - will only produce to Kafka, not send via API")
        if not self.sender_number:
            logger.warning("SENDER_NUMBER not set - may not be able to send messages via API")

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
        self.router = Router(self.openai_api_key)
        
        # Initialize Series API client if API key is provided
        if self.series_api_key:
            self.series_api = SeriesAPI(self.series_api_key, self.series_api_url)
        else:
            self.series_api = None

        self.running = False
        logger.info("SeriesSwarm initialized successfully")

    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single message through the pipeline.

        Args:
            message: Message dictionary from Kafka consumer

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            message_value = message.get('value', {})
            
            # Extract text from Series API message structure
            # Series API sends: {"data": {"text": "..."}, "event_type": "message.received"}
            data = message_value.get('data', {})
            text = data.get('text', message_value.get('text', ''))
            
            # Only process message.received events
            event_type = message_value.get('event_type')
            if event_type != 'message.received':
                logger.debug(f"Skipping event type: {event_type}")
                return True
            
            # Create a normalized message for routing
            routing_message = {
                'text': text,
                'content': text,
                'data': data,
                'event_type': event_type,
                'from_phone': data.get('from_phone'),
            }
            
            logger.info(f"Processing message from {data.get('from_phone', 'unknown')}: {text[:50] if text else 'N/A'}...")

            # Route message to appropriate agent
            agent_func = self.router.route(routing_message)
            if agent_func is None:
                logger.warning("No agent function returned from router")
                return False

            # Prepare agent data from message
            # Use the data from Series API structure, but ensure text is available
            agent_data = data.copy() if data else message_value.copy()
            
            # Ensure text field is available for agents that need it
            if 'text' not in agent_data and text:
                agent_data['text'] = text
            if 'content' not in agent_data and text:
                agent_data['content'] = text

            # Special handling for agents that need API key (audio and secretary for AI parsing)
            if agent_func.__name__ in ['generate_audio_briefing', 'generate_calendar_invite']:
                agent_data = agent_data.copy()
                agent_data['openai_api_key'] = self.openai_api_key

            # Execute agent function
            logger.info(f"Executing agent function: {agent_func.__name__}")
            result = agent_func(agent_data)

            # Determine filename and MIME type based on agent
            filename, mime_type = self._get_attachment_info(agent_func.__name__)
            
            # Send via Series API if available (preferred method)
            api_success = False
            if self.series_api and self.sender_number:
                chat_id = self.series_api.get_chat_id_from_message(message_value)
                recipient_phone = self.series_api.get_recipient_phone(message_value)
                
                if chat_id:
                    # Send to existing chat
                    api_result = self.series_api.send_message_with_attachment(
                        send_from=self.sender_number,
                        chat_id=chat_id,
                        text=f"Here's your {self._get_asset_name(agent_func.__name__)}",
                        attachment=result,
                        filename=filename,
                        mime_type=mime_type
                    )
                    api_success = api_result is not None
                elif recipient_phone:
                    # Create new chat and send
                    api_result = self.series_api.send_message_with_attachment(
                        send_from=self.sender_number,
                        phone_numbers=[recipient_phone],
                        text=f"Here's your {self._get_asset_name(agent_func.__name__)}",
                        attachment=result,
                        filename=filename,
                        mime_type=mime_type
                    )
                    api_success = api_result is not None
                else:
                    logger.warning("Could not determine chat_id or recipient_phone, falling back to Kafka")
            
            # Also produce to Kafka (for logging/monitoring)
            kafka_success = self.producer.produce(
                value=result,
                key=message.get('key'),
                headers={
                    'agent': agent_func.__name__,
                    'content-type': mime_type,
                }
            )

            if api_success:
                logger.info(f"Successfully sent {self._get_asset_name(agent_func.__name__)} via Series API")
            elif self.series_api:
                logger.warning(f"Failed to send via Series API, produced to Kafka instead")
            
            if kafka_success:
                logger.info(f"Successfully produced result from {agent_func.__name__} to Kafka")

            return api_success or kafka_success

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def _get_content_type(self, agent_name: str) -> str:
        """Get content type based on agent name."""
        content_types = {
            'generate_ticket': 'application/pdf',
            'generate_calendar_invite': 'text/calendar',
            'generate_vcard': 'text/vcard',
            'generate_audio_briefing': 'audio/mpeg',
        }
        return content_types.get(agent_name, 'application/octet-stream')
    
    def _get_attachment_info(self, agent_name: str) -> tuple:
        """Get filename and MIME type for attachment based on agent name."""
        info = {
            'generate_ticket': ('ticket.pdf', 'application/pdf'),
            'generate_calendar_invite': ('event.ics', 'text/calendar'),
            'generate_vcard': ('contact.vcf', 'text/vcard'),
            'generate_audio_briefing': ('audio.mp3', 'audio/mpeg'),
        }
        return info.get(agent_name, ('attachment', 'application/octet-stream'))
    
    def _get_asset_name(self, agent_name: str) -> str:
        """Get human-readable asset name based on agent name."""
        names = {
            'generate_ticket': 'ticket',
            'generate_calendar_invite': 'calendar invite',
            'generate_vcard': 'contact card',
            'generate_audio_briefing': 'audio briefing',
        }
        return names.get(agent_name, 'asset')

    def run(self):
        """Run the main event loop."""
        self.running = True
        logger.info("Starting SeriesSwarm event loop...")

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

