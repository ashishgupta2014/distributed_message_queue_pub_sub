from source.models.message import Message
from source.models.queue import Queue


def main():
    """Execute"""
    queue: Queue = Queue()
    topic1 = queue.create_topic(topic_name="topic1")
    topic2 = queue.create_topic(topic_name="topic2")

    queue.create_producer(producer_name="producer1")
    queue.create_producer(producer_name="producer2")

    queue.create_consumer(consumer_name="consumer1")
    queue.create_consumer(consumer_name="consumer2")
    queue.create_consumer(consumer_name="consumer3")
    queue.create_consumer(consumer_name="consumer4")
    queue.create_consumer(consumer_name="consumer5")

    queue.subscriber(topic_name="topic1", consumer_name="consumer1")
    queue.subscriber(topic_name="topic1", consumer_name="consumer2")
    queue.subscriber(topic_name="topic1", consumer_name="consumer3")
    queue.subscriber(topic_name="topic1", consumer_name="consumer4")
    queue.subscriber(topic_name="topic1", consumer_name="consumer5")

    queue.subscriber(topic_name="topic2", consumer_name="consumer1")
    queue.subscriber(topic_name="topic2", consumer_name="consumer3")
    queue.subscriber(topic_name="topic2", consumer_name="consumer4")

    queue.publish(producer_name="producer1", message=Message("Message 1"), topic=topic1)
    queue.publish(producer_name="producer1", message=Message("Message 2"), topic=topic1)
    queue.publish(producer_name="producer2", message=Message("Message 3"), topic=topic1)
    queue.publish(producer_name="producer1", message=Message("Message 4"), topic=topic2)
    queue.publish(producer_name="producer2", message=Message("Message 5"), topic=topic2)


if __name__ == "__main__":
    main()
