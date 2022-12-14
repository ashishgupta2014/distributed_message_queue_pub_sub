import threading
import time

from source.models.consumer import Consumer
from source.models.topic import Topic
from source.settings import logger


class ConsumerWorker(threading.Thread):
    def __init__(self, topic: Topic, consumer: Consumer):
        threading.Thread.__init__(self, name=consumer.get_consumer_name)
        self._topic = topic
        self._consumer = consumer

    def run(self):
        logger.info(f'thread started {self._consumer.get_consumer_name}')
        self.process()

    def process(self):
        while True:
            time.sleep(self._consumer.get_consumer_sleep_time)
            seek = self._consumer.get_seek
            message = self._topic.get_message(seek)
            if message:
                self._consumer.consumed(next_seek=seek+1, message=message)
            else:
                logger.info(f'{self._consumer.get_consumer_name} Waiting for Producer to produce next message '
                            f'at seek {seek}')





