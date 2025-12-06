"""Main event loop for SeriesSwarm."""

import os
import sys
import logging
import signal
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, Router

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
        self.openai_api_key = os.getenv('OPENAI_API_KEY')

        # Validate required configuration
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER environment variable is required")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")

        # Initialize components
        logger.info("Initializing SeriesSwarm components...")
        self.consumer = KafkaConsumer(self.kafka_broker, self.kafka_topic_in)
        self.producer = KafkaProducer(self.kafka_broker, self.kafka_topic_out)
        self.router = Router(self.openai_api_key)

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
            logger.info(f"Processing message: {message_value.get('text', 'N/A')[:50]}...")

            # Route message to appropriate agent
            agent_func = self.router.route(message_value)
            if agent_func is None:
                logger.warning("No agent function returned from router")
                return False

            # Prepare agent data from message
            agent_data = message_value.get('data', message_value)

            # Special handling for audio agent (needs API key)
            if agent_func.__name__ == 'generate_audio_briefing':
                agent_data = agent_data.copy()
                agent_data['openai_api_key'] = self.openai_api_key

            # Execute agent function
            logger.info(f"Executing agent function: {agent_func.__name__}")
            result = agent_func(agent_data)

            # Prepare output message
            output_message = {
                'original_message': message_value,
                'agent': agent_func.__name__,
                'result_type': type(result).__name__,
            }

            # Produce result to outbound topic
            # For binary data (BytesIO), we'll send it directly
            success = self.producer.produce(
                value=result,
                key=message.get('key'),
                headers={
                    'agent': agent_func.__name__,
                    'content-type': self._get_content_type(agent_func.__name__),
                }
            )

            if success:
                logger.info(f"Successfully produced result from {agent_func.__name__}")
            else:
                logger.error(f"Failed to produce result from {agent_func.__name__}")

            return success

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

