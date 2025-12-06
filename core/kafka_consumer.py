"""Kafka consumer wrapper for SeriesSwarm."""

import json
import logging
from typing import Optional, Dict, Any
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Wrapper around confluent-kafka Consumer for consuming messages from Kafka topics."""

    def __init__(self, broker: str, topic: str, group_id: str = 'series-swarm-group',
                 sasl_username: Optional[str] = None, sasl_password: Optional[str] = None,
                 client_id: Optional[str] = None):
        """
        Initialize Kafka consumer.

        Args:
            broker: Kafka broker address (e.g., 'localhost:9092')
            topic: Topic name to consume from
            group_id: Consumer group ID
            sasl_username: SASL username for authentication (for Confluent Cloud)
            sasl_password: SASL password/API key for authentication (for Confluent Cloud)
            client_id: Client ID for the consumer
        """
        self.broker = broker
        self.topic = topic
        self.group_id = group_id

        config = {
            'bootstrap.servers': broker,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Changed back to 'earliest' to see all messages
            'enable.auto.commit': False,  # Disable auto-commit to manually control offset
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
            logger.info("SASL authentication enabled for consumer")

        self.consumer = Consumer(config)
        self.consumer.subscribe([topic])
        logger.info(f"Kafka consumer initialized for topic: {topic}")

    def consume(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Consume a single message from the topic.

        Args:
            timeout: Timeout in seconds for polling

        Returns:
            Dictionary with 'key', 'value', and 'headers' if message received, None otherwise
        """
        try:
            msg = self.consumer.poll(timeout=timeout)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                    return None
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    return None

            # Deserialize message
            try:
                value = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to deserialize message: {e}")
                return None

            key = msg.key().decode('utf-8') if msg.key() else None
            headers = {k: v.decode('utf-8') for k, v in (msg.headers() or [])}

            result = {
                'key': key,
                'value': value,
                'headers': headers,
                'partition': msg.partition(),
                'offset': msg.offset(),
            }

            logger.info(f"Consumed message from partition {msg.partition()}, offset {msg.offset()}")
            return result

        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            return None

    def close(self):
        """Close the consumer."""
        self.consumer.close()
        logger.info("Kafka consumer closed")

