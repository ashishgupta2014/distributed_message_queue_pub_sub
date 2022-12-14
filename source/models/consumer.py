
from source.models.message import Message
from source.settings import logger


class Consumer:
    """Consumer"""

    def __init__(self, consumer_name: str, sleep_time: int):
        self._name = consumer_name
        self._sleep_time = sleep_time
        self._seek = 0

    @property
    def get_consumer_name(self):
        return self._name

    @property
    def get_consumer_sleep_time(self):
        return self._sleep_time

    @property
    def get_seek(self):
        return self._seek

    def consumed(self, next_seek: int, message: Message):
        logger.info(f'Consumer ({self._name}) Message ({message.get_message()}) consumed from {self._seek} and next seek {next_seek}')
        self._seek = next_seek
