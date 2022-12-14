import uuid
from collections import defaultdict
from typing import DefaultDict, List

from source.handlers.topic_handler import TopicHandler
from source.models.message import Message
from source.models.producer import Producer
from source.models.consumer import Consumer
from source.models.topic import Topic
from source.settings import logger


class Queue:
    """Queue DS to store messages"""

    def __init__(self):
        self._topic_processor: DefaultDict[str, TopicHandler] = defaultdict(TopicHandler)
        self._producer: DefaultDict[str, Producer] = defaultdict(Producer)
        self._consumer: DefaultDict[str, Consumer] = defaultdict(Consumer)

    def create_topic(self, topic_name: str) -> Topic:
        """create topic"""
        topic = Topic(topic_name=topic_name)
        self._topic_processor[topic.get_topic_name] = TopicHandler(topic)
        logger.info(f'Topic created by name {topic.get_topic_name}')
        return topic

    def create_producer(self, producer_name) -> Producer:
        """create producer"""
        producer = Producer(producer_name)
        self._producer[producer.get_producer_name] = producer
        logger.info(f'Producer created {producer.get_producer_name}')
        return producer

    def create_consumer(self, consumer_name: str, sleep_time: int = 10) -> Consumer:
        """create consumer"""
        consumer = Consumer(consumer_name=consumer_name, sleep_time=sleep_time)
        self._consumer[consumer_name] = consumer
        logger.info(f'Producer created {consumer.get_consumer_name}')
        return consumer

    def publish(self, producer_name: str, topic: Topic, message: Message):
        """Publish message to topic"""
        self._producer[producer_name].publish(topic, message)

    def subscriber(self, topic_name: str, consumer_name: str):
        topic_handler = self._topic_processor[topic_name]
        topic = topic_handler.get_topic
        consumer = self._consumer[consumer_name]
        topic.add_subscriber(consumer)
        topic_handler.start_worker(consumer)
        logger.info(f'Consumer {consumer.get_consumer_name} subscribed to topic {topic.get_topic_name}')
