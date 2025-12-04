import functools
from concurrent.futures import ThreadPoolExecutor

from rocketmq.v5.log import logger

from .message_listener import ConsumeResult


class Consumption:

    def __init__(self, message_listener, consumption_thread_count, consume_result_callback, client_id, backoff_policy):
        self.__fifo = False
        self.__message_listener = message_listener
        self.__consumption_executor = ThreadPoolExecutor(max_workers=consumption_thread_count,
                                                         thread_name_prefix=f"{client_id}_message_consumption_thread")
        self.__consume_result_callback = consume_result_callback
        self.__backoff_policy = backoff_policy

    def execute_consume(self, messages, message_queue, consumer_group, client_metrics):
        for message in messages:
            self.__consumption_executor.submit(functools.partial(self.__consume, message=message, message_queue=message_queue, consumer_group=consumer_group, client_metrics=client_metrics))

    def __consume(self, message, message_queue, consumer_group, client_metrics):
        consume_context = None
        try:
            consume_context = client_metrics.consume_before(consumer_group, message)
            if message.corrupted:
                logger.error(
                    f"message is corrupted for consumption, prepare to discard it, topic: {message.topic}, message_id: {message.message_id}")
                client_metrics.consume_after(consume_context, False)
                self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue)
            consume_result = self.__message_listener.consume(message)
        except Exception as e:
            logger.error(f"message listener raised an exception while consuming messages, topic: {message.topic}, message_id: {message.message_id}, {e}")
            consume_result = ConsumeResult.FAILURE
        client_metrics.consume_after(consume_context, consume_result == ConsumeResult.SUCCESS)
        self.__consume_result_callback(consume_result, message, message_queue)

    @property
    def fifo(self):
        return False

    @property
    def backoff_policy(self):
        return self.__backoff_policy

    @backoff_policy.setter
    def backoff_policy(self, backoff_policy):
        self.__backoff_policy = backoff_policy
