import abc
from enum import Enum

from rocketmq.v5.model import Message


class ConsumeResult(Enum):
    SUCCESS = 0  # Consume message successfully.
    FAILURE = 1  # Failed to consume message.


class MessageListener(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def consume(self, message: Message) -> ConsumeResult:
        pass
