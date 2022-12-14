from collections import defaultdict
from typing import DefaultDict

from source.handlers.consumer_worker import ConsumerWorker
from source.models.consumer import Consumer
from source.models.topic import Topic


class TopicHandler:
    """Topic Handler"""

    def __init__(self, topic: Topic):
        self._topic = topic
        self._consumer_worker: DefaultDict[str, ConsumerWorker] = defaultdict(ConsumerWorker)

    @property
    def get_topic(self):
        return self._topic

    def start_worker(self, consumer: Consumer):
        """worker start"""
        worker = ConsumerWorker(self._topic, consumer)
        self._consumer_worker[consumer.get_consumer_name] = worker
        worker.start()
