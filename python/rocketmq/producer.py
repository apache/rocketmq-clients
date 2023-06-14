from rocketmq.client import Client
from rocketmq.client_config import ClientConfig
from typing import Set
from rocketmq import logger

from rocketmq.message import Message

from protocol.service_pb2 import SendMessageRequest

class Producer(Client):
    def __init__(self, client_config: ClientConfig, topics: Set[str]):
        super().__init__(client_config, topics)

    def start_up(self):
        super().start_up()


    async def send_message(self, message: str):
        # TODO: construct rpc params
        req = SendMessageRequest()
        req.messages.extend(message)
        self.__client_manager.send_message(self.__endpoints, req, 10)


if __name__ == "__main__":
    
    message = Message(topic="topic", body="message body".encode())
    client_config = ClientConfig(endpoints="endpoint")
    producer = Producer(client_config, topics=("topic"))
    producer.send_message("test")

