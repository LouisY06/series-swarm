"""Kafka producer wrapper for SeriesSwarm."""

import json
import logging
from typing import Optional, Dict, Any, Union
from io import BytesIO
from confluent_kafka import Producer, KafkaError

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Wrapper around confluent-kafka Producer for publishing messages to Kafka topics."""

    def __init__(self, broker: str, topic: str, sasl_username: Optional[str] = None,
                 sasl_password: Optional[str] = None, client_id: Optional[str] = None):
        """
        Initialize Kafka producer.

        Args:
            broker: Kafka broker address (e.g., 'localhost:9092')
            topic: Topic name to produce to
            sasl_username: SASL username for authentication (for Confluent Cloud)
            sasl_password: SASL password/API key for authentication (for Confluent Cloud)
            client_id: Client ID for the producer
        """
        self.broker = broker
        self.topic = topic

        config = {
            'bootstrap.servers': broker,
        }

        # Add client ID if provided
        if client_id:
            config['client.id'] = client_id

        # Add SASL authentication if credentials provided (Confluent Cloud)
        if sasl_username and sasl_password:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': sasl_username,
                'sasl.password': sasl_password,
            })
            logger.info("SASL authentication enabled for producer")

        self.producer = Producer(config)
        logger.info(f"Kafka producer initialized for topic: {topic}")

    def produce(self, value: Union[Dict[str, Any], BytesIO, bytes], key: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Produce a message to the topic.

        Args:
            value: Message value - can be dict (JSON), BytesIO, or bytes
            key: Optional message key
            headers: Optional message headers

        Returns:
            True if message was queued successfully, False otherwise
        """
        try:
            # Serialize value based on type
            if isinstance(value, BytesIO):
                serialized_value = value.getvalue()
            elif isinstance(value, bytes):
                serialized_value = value
            elif isinstance(value, dict):
                serialized_value = json.dumps(value).encode('utf-8')
            else:
                logger.error(f"Unsupported value type: {type(value)}")
                return False

            # Prepare headers
            kafka_headers = []
            if headers:
                kafka_headers = [(k.encode('utf-8'), v.encode('utf-8')) for k, v in headers.items()]

            # Produce message
            self.producer.produce(
                self.topic,
                value=serialized_value,
                key=key.encode('utf-8') if key else None,
                headers=kafka_headers if kafka_headers else None,
                callback=self._delivery_callback
            )

            # Trigger delivery callbacks
            self.producer.poll(0)
            return True

        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return False

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def flush(self, timeout: float = 10.0):
        """Flush pending messages."""
        self.producer.flush(timeout=timeout)
        logger.info("Producer flushed")

    def close(self):
        """Close the producer."""
        self.flush()
        logger.info("Kafka producer closed")

