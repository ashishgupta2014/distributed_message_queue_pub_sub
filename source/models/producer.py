from source.models.message import Message
from source.models.topic import Topic
from source.settings import logger


class Producer:
    """Producer"""

    def __init__(self, producer_name: str):
        self._name = producer_name

    @staticmethod
    def publish(topic: Topic, message: Message):
        """Publish message to topic"""
        topic.add_message(message)
        logger.info(f'{message.get_message()} Published to {topic.get_topic_name}')

    @property
    def get_producer_name(self):
        return self._name
