"""Kafka consumer for SeriesSwarm."""

import json
import logging
import time
from typing import Optional, Dict, Any
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Wrapper for Confluent Kafka consumer."""

    def __init__(
        self,
        broker: str,
        topic: str,
        group_id: str = 'series-swarm-group',
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        client_id: Optional[str] = None
    ):
        """Initialize Kafka consumer."""
        self.broker = broker
        self.topic = topic
        self.group_id = group_id

        # Use unique group ID to avoid stale consumer state
        unique_group = f"{group_id}-{int(time.time())}"
        
        config = {
            'bootstrap.servers': broker,
            'group.id': unique_group,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        
        logger.info(f"Using consumer group: {unique_group}")

        if client_id:
            config['client.id'] = client_id

        if sasl_username and sasl_password:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': sasl_username,
                'sasl.password': sasl_password,
            })
            logger.info("SASL authentication enabled")

        self.consumer = Consumer(config)
        self.consumer.subscribe([topic])
        logger.info(f"Consumer initialized for topic: {topic}")

    def consume(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Consume a single message."""
        try:
            msg = self.consumer.poll(timeout=timeout)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                logger.error(f"Consumer error: {msg.error()}")
                return None

            try:
                value = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.debug(f"Skipping non-JSON message: {e}")
                return None  # Skip binary messages silently

            return {
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'value': value,
                'partition': msg.partition(),
                'offset': msg.offset(),
            }

        except Exception as e:
            logger.error(f"Error consuming: {e}")
            return None

    def close(self):
        """Close the consumer."""
        self.consumer.close()
        logger.info("Consumer closed")

