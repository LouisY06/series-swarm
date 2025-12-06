"""Kafka producer for SeriesSwarm."""

import json
import logging
from typing import Optional, Dict, Any
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Wrapper for Confluent Kafka producer."""

    def __init__(
        self,
        broker: str,
        topic: str,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        client_id: Optional[str] = None
    ):
        """Initialize Kafka producer."""
        self.broker = broker
        self.topic = topic

        config = {
            'bootstrap.servers': broker,
        }

        if client_id:
            config['client.id'] = client_id

        if sasl_username and sasl_password:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': sasl_username,
                'sasl.password': sasl_password,
            })
            logger.info("SASL authentication enabled for producer")

        self.producer = Producer(config)
        logger.info(f"Producer initialized for topic: {topic}")

    def produce(self, message: Dict[str, Any], key: Optional[str] = None):
        """Produce a message to the topic."""
        try:
            value = json.dumps(message).encode('utf-8')
            self.producer.produce(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=value
            )
            self.producer.flush()
            logger.debug(f"Produced message to {self.topic}")
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def close(self):
        """Close the producer."""
        self.producer.flush()
        logger.info("Producer closed")

