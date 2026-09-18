# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import threading

from rocketmq.grpc_protocol import Code, QueryRouteRequest
from rocketmq.v5.log import logger
from rocketmq.v5.model import TopicRouteData
from rocketmq.v5.util import ConcurrentMap, MessagingResultChecker

from .scheduler import ClientScheduler


class ClientRouteManager:
    """Manages topic route cache, periodic route updates, and endpoint change detection.

    Caches topic-to-broker route data, periodically refreshes it, and
    detects route changes to trigger telemetry stream setup for new endpoints.
    """

    def __init__(self, client):
        self.__client = client
        # {topic, TopicRouteData}
        self.__topic_route_cache = ConcurrentMap()
        self.__scheduler = None

    def update_topic_route(self, topic):
        event = threading.Event()
        callback = functools.partial(
            self.__query_topic_route_async_callback, topic=topic, event=event
        )
        future = self.__client.rpc_client.query_topic_route_async(
            self.__client.client_configuration.rpc_endpoints,
            self.__topic_route_req(topic),
            metadata=self.__client.sign(),
            timeout=self.__client.client_configuration.request_timeout,
        )
        future.add_done_callback(callback)
        event.wait()
        return self.__topic_route_cache.get(topic)

    def retrieve_topic_route_data(self, topic):
        route = self.__topic_route_cache.get(topic)
        if route:
            return route
        route = self.update_topic_route(topic)
        if route:
            logger.info(f"{self.__client} update topic:{topic} route success.")
            return route
        raise Exception(f"failed to fetch topic:{topic} route.")

    def remove_topic_route_data(self, topic):
        self.__topic_route_cache.remove(topic)

    def get_all_endpoints(self):
        endpoints_map = {}
        all_route = self.__topic_route_cache.values()
        for topic_route in all_route:
            endpoints_map.update(topic_route.all_endpoints())
        return endpoints_map

    def start_scheduler(self, io_loop):
        self.__scheduler = ClientScheduler(
            f"{self.__client.client_id}_update_topic_route_schedule_thread",
            self.__do_update_topic_route_cache,
            10, 30,
            io_loop,
        )
        self.__scheduler.start_scheduler()
        logger.info("start topic route scheduler success.")

    def stop_scheduler(self):
        if self.__scheduler:
            self.__scheduler.stop_scheduler()
            self.__scheduler = None

    def clear(self):
        self.__topic_route_cache.clear()

    def __do_update_topic_route_cache(self):
        logger.debug(f"{self.__client} run update topic route in scheduler.")
        for topic in self.__topic_route_cache.keys():
            self.__update_topic_route_async(topic)

    def __query_topic_route_async_callback(self, future, topic, event=None):
        try:
            res = future.result()
            self.__handle_topic_route_res(res, topic)
        except Exception as e:
            logger.error(f"query topic raise exception, {e}")
        finally:
            if event:
                event.set()

    def __update_topic_route_async(self, topic):
        callback = functools.partial(
            self.__query_topic_route_async_callback, topic=topic
        )
        future = self.__client.rpc_client.query_topic_route_async(
            self.__client.client_configuration.rpc_endpoints,
            self.__topic_route_req(topic),
            metadata=self.__client.sign(),
            timeout=self.__client.client_configuration.request_timeout,
        )
        future.add_done_callback(callback)

    def __topic_route_req(self, topic):
        req = QueryRouteRequest()
        req.topic.name = topic
        req.topic.resource_namespace = self.__client.client_configuration.namespace
        req.endpoints.CopyFrom(self.__client.client_configuration.rpc_endpoints.endpoints)
        return req

    def __handle_topic_route_res(self, res, topic):
        if res:
            MessagingResultChecker.check(res.status)
            if res.status.code == Code.OK:
                topic_route = TopicRouteData(res.message_queues)
                logger.info(
                    f"{self.__client} update topic:{topic} route, route info: {topic_route}"
                )
                self.__check_topic_route_endpoints_changed(topic, topic_route)
                self.__topic_route_cache.put(topic, topic_route)
                self.__client.update_queue_selector(topic, topic_route)
        else:
            raise Exception(f"query topic route exception, topic:{topic}")

    def __check_topic_route_endpoints_changed(self, topic, route):
        old_route = self.__topic_route_cache.get(topic)
        if old_route is None or old_route != route:
            logger.info(
                f"topic:{topic} route changed for {self.__client}. old route is {old_route}, new route is {route}"
            )
        all_endpoints = self.get_all_endpoints()
        topic_route_endpoints = route.all_endpoints()
        diff = set(topic_route_endpoints.keys()).difference(set(all_endpoints.keys()))
        for address in diff:
            endpoints = topic_route_endpoints[address]
            self.__client.on_new_endpoints(endpoints)
