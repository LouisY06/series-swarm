"""Core modules for SeriesSwarm."""

from .kafka_consumer import KafkaConsumer
from .kafka_producer import KafkaProducer
from .router import Router
from .series_api import SeriesAPI

__all__ = ['KafkaConsumer', 'KafkaProducer', 'Router', 'SeriesAPI']

