from rocketmq.client_manager import ClientManager
from rocketmq.client_id_encoder import ClientIdEncoder
from rocketmq.client_config import ClientConfig
from typing import Set
from threading import Lock
from rocketmq import logger
import asyncio

from protocol.service_pb2 import HeartbeatRequest, QueryRouteRequest

class Client:
    def __init__(self, client_config : ClientConfig, topics : Set[str]):
        
        self.__client_config = client_config
        self.__cliend_id = ClientIdEncoder.generate()
        self.__endpoints = client_config.endpoints()
        self.__topics = topics
        self.__topic_route_cache = {}

        self.__sessions_table = {}
        self.__sessionsLock = Lock()

        self.__client_manager = ClientManager(self)

    def start_up(self):
        logger.info("Begin to start the rocketmq client, clientId=%s", self.__cliend_id)

        # get topic route
        for topic in self.__topics:
            self.__topic_route_cache[topic] = asyncio.run(self.fetch_topic_route(topic))
        logger.info("Fetch topic route data from remote successfully during startup, clientId=%s, topics=%s",self.__cliend_id, self.__topics)

        
    # return topic data
    async def fetch_topic_route(self, topic):
        # TODO: construct rpc params
        req = QueryRouteRequest()
        response = await self.__client_manager.query_route(self.__endpoints, req, 10)


    async def do_heartbeat(self):
        # TODO: construct rpc params
        req = HeartbeatRequest()
        response = await self.__client_manager.heartbeat(self.__endpoints, req, 10)