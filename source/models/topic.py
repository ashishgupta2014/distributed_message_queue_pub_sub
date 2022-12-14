from typing import List

from source.models.message import Message
from source.models.consumer import Consumer


class Topic:
    """Topic"""

    def __init__(self, topic_name: str):
        self._name: str = topic_name
        self._messages: List[Message] = list()
        self._subscribers: List[Consumer] = list()

    def add_message(self, msg: Message) -> None:
        self._messages.append(msg)

    def add_subscriber(self, consumer: Consumer):
        self._subscribers.append(consumer)

    @property
    def get_topic_name(self):
        return self._name

    def get_message(self, seek):
        if 0 <= seek < len(self._messages):
            return self._messages[seek]
        return None
