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

import asyncio
import functools
import os
import threading
from asyncio import InvalidStateError
from concurrent.futures import ThreadPoolExecutor

from grpc.aio import AioRpcError
from rocketmq.grpc_protocol import ClientType, Code, QueryRouteRequest
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.client.metrics import ClientMetrics
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.model import TopicRouteData
from rocketmq.v5.util import (ClientId, ConcurrentMap, MessagingResultChecker,
                              Misc, Signature)


class Client:

    def __init__(
        self, client_configuration, topics, client_type: ClientType, tls_enable=False
    ):
        if client_configuration is None:
            raise IllegalArgumentException("clientConfiguration should not be null.")
        self.__client_configuration = client_configuration
        self.__client_type = client_type
        self.__client_id = ClientId().client_id
        # {topic, topicRouteData}
        self.__topic_route_cache = ConcurrentMap()
        self.__rpc_client = RpcClient(tls_enable)
        self.__client_metrics = ClientMetrics(self.__client_id, client_configuration)
        self.__topic_route_scheduler = None
        self.__heartbeat_scheduler = None
        self.__sync_setting_scheduler = None
        self.__clear_idle_rpc_channels_scheduler = None
        self.__topic_route_scheduler_threading_event = None
        self.__heartbeat_scheduler_threading_event = None
        self.__sync_setting_scheduler_threading_event = None
        self.__clear_idle_rpc_channels_threading_event = None
        if topics is not None:
            self.__topics = set(
                filter(lambda topic: Misc.is_valid_topic(topic), topics)
            )
        else:
            self.__topics = set()
        self.__client_callback_executor = None
        self.__is_running = False
        self.__client_thread_task_enabled = False
        self.__had_shutdown = False

    def startup(self):
        try:
            if self.__had_shutdown is True:
                raise Exception(
                    f"client:{self.__client_id} had shutdown, can't startup again."
                )

            try:
                # pre update topic route for producer or consumer
                for topic in self.__topics:
                    self.__update_topic_route(topic)
            except Exception as e:
                # ignore this exception and retrieve again when calling send or receive
                logger.warn(
                    f"update topic exception when client startup, ignore it, try it again in scheduler. exception: {e}"
                )
            self.__start_scheduler()
            self.__start_async_rpc_callback_handler()
            self.__is_running = True
            self._start_success()
        except Exception as e:
            self.__is_running = False
            self.__stop_client_threads()
            self._start_failure()
            logger.error(f"client:{self.__client_id} startup exception:  {e}")
            raise e

    def shutdown(self):
        if self.is_running is False:
            raise IllegalStateException(f"client:{self.__client_id} is not running.")

        if self.__had_shutdown is True:
            raise IllegalStateException(f"client:{self.__client_id} had shutdown.")

        try:
            self.__stop_client_threads()
            self.__notify_client_termination()
            self.__rpc_client.stop()
            self.__topic_route_cache.clear()
            self.__topics.clear()
            self.__had_shutdown = True
            self.__is_running = False
        except Exception as e:
            logger.error(f"{self.__str__()} shutdown exception: {e}")
            raise e

    """ abstract """

    def _start_success(self):
        """each subclass implements its own actions after a successful startup"""
        pass

    def _start_failure(self):
        """each subclass implements its own actions after a startup failure"""
        pass

    def _sync_setting_req(self, endpoints):
        """each subclass implements its own telemetry settings scheme"""
        pass

    def _heartbeat_req(self):
        """each subclass implements its own heartbeat request"""
        pass

    def _notify_client_termination_req(self):
        """each subclass implements its own client termination request"""
        pass

    def _update_queue_selector(self, topic, topic_route):
        """each subclass implements its own queue selector"""
        pass

    """ scheduler """

    def __start_scheduler(self):
        # start 4 schedulers in different threads, each thread use the same asyncio event loop.
        try:
            # update topic route every 30 seconds
            self.__client_thread_task_enabled = True
            self.__topic_route_scheduler = threading.Thread(
                target=self.__schedule_update_topic_route_cache,
                name="update_topic_route_schedule_thread",
            )
            self.__topic_route_scheduler_threading_event = threading.Event()
            self.__topic_route_scheduler.start()
            logger.info("start topic route scheduler success.")

            # send heartbeat to all endpoints every 10 seconds
            self.__heartbeat_scheduler = threading.Thread(
                target=self.__schedule_heartbeat, name="heartbeat_schedule_thread"
            )
            self.__heartbeat_scheduler_threading_event = threading.Event()
            self.__heartbeat_scheduler.start()
            logger.info("start heartbeat scheduler success.")

            # send client setting to all endpoints every 5 seconds
            self.__sync_setting_scheduler = threading.Thread(
                target=self.__schedule_update_setting,
                name="sync_setting_schedule_thread",
            )
            self.__sync_setting_scheduler_threading_event = threading.Event()
            self.__sync_setting_scheduler.start()
            logger.info("start sync setting scheduler success.")

            # clear unused grpc channel(>30 minutes) every 60 seconds
            self.__clear_idle_rpc_channels_scheduler = threading.Thread(
                target=self.__schedule_clear_idle_rpc_channels,
                name="clear_idle_rpc_channel_schedule_thread",
            )
            self.__clear_idle_rpc_channels_threading_event = threading.Event()
            self.__clear_idle_rpc_channels_scheduler.start()
            logger.info("start clear idle rpc channels scheduler success.")
        except Exception as e:
            logger.info(f"start scheduler exception: {e}")
            self.__stop_client_threads()
            raise e

    def __schedule_update_topic_route_cache(self):
        asyncio.set_event_loop(self._rpc_channel_io_loop())
        while True:
            if self.__client_thread_task_enabled is True:
                self.__topic_route_scheduler_threading_event.wait(30)
                logger.debug(f"{self.__str__()} run update topic route in scheduler.")
                # update topic route for each topic in cache
                topics = self.__topic_route_cache.keys()
                for topic in topics:
                    try:
                        if self.__client_thread_task_enabled is True:
                            self.__update_topic_route_async(topic)
                    except Exception as e:
                        logger.error(
                            f"{self.__str__()} scheduler update topic:{topic} route raise exception: {e}"
                        )
            else:
                break
        logger.info(
            f"{self.__str__()} stop scheduler for update topic route cache success."
        )

    def __schedule_heartbeat(self):
        asyncio.set_event_loop(self._rpc_channel_io_loop())
        while True:
            if self.__client_thread_task_enabled is True:
                self.__heartbeat_scheduler_threading_event.wait(10)
                logger.debug(f"{self.__str__()} run send heartbeat in scheduler.")
                all_endpoints = self.__get_all_endpoints().values()
                try:
                    for endpoints in all_endpoints:
                        if self.__client_thread_task_enabled is True:
                            self.__heartbeat_async(endpoints)
                except Exception as e:
                    logger.error(
                        f"{self.__str__()} scheduler send heartbeat raise exception: {e}"
                    )
            else:
                break
        logger.info(f"{self.__str__()} stop scheduler for heartbeat success.")

    def __schedule_update_setting(self):
        asyncio.set_event_loop(self._rpc_channel_io_loop())
        while True:
            if self.__client_thread_task_enabled is True:
                self.__sync_setting_scheduler_threading_event.wait(5)
                logger.debug(f"{self.__str__()} run update setting in scheduler.")
                try:
                    all_endpoints = self.__get_all_endpoints().values()
                    for endpoints in all_endpoints:
                        if self.__client_thread_task_enabled is True:
                            self.__setting_write(endpoints)
                except Exception as e:
                    logger.error(
                        f"{self.__str__()} scheduler set setting raise exception: {e}"
                    )
            else:
                break
        logger.info(f"{self.__str__()} stop scheduler for update setting success.")

    def __schedule_clear_idle_rpc_channels(self):
        while True:
            if self.__client_thread_task_enabled is True:
                self.__clear_idle_rpc_channels_threading_event.wait(60)
                logger.debug(
                    f"{self.__str__()} run scheduler for clear idle rpc channels."
                )
                try:
                    if self.__client_thread_task_enabled is True:
                        self.__rpc_client.clear_idle_rpc_channels()
                except Exception as e:
                    logger.error(
                        f"{self.__str__()} run scheduler for clear idle rpc channels: {e}"
                    )
            else:
                break
        logger.info(
            f"{self.__str__()} stop scheduler for clear idle rpc channels success."
        )

    """ callback handler for async method """

    def __start_async_rpc_callback_handler(self):
        # to handle callback when using async method such as send_async(), receive_async().
        # switches user's callback thread from RpcClient's _io_loop_thread to client's client_callback_worker_thread
        try:
            workers = os.cpu_count()
            self.__client_callback_executor = ThreadPoolExecutor(max_workers=workers,
                                                                 thread_name_prefix=f"client_callback_worker-{self.__client_id}")
            logger.info(f"{self.__str__()} start callback executor success. max_workers:{workers}")
        except Exception as e:
            print(f"{self.__str__()} start async rpc callback raise exception: {e}")
            raise e

    @staticmethod
    def __handle_callback(callback_result):
        if callback_result.is_success:
            callback_result.future.set_result(callback_result.result)
        else:
            callback_result.future.set_exception(callback_result.result)

    """ protect """

    def _retrieve_topic_route_data(self, topic):
        route = self.__topic_route_cache.get(topic)
        if route is not None:
            return route
        else:
            route = self.__update_topic_route(topic)
            if route is not None:
                logger.info(f"{self.__str__()} update topic:{topic} route success.")
                self.__topics.add(topic)
                return route
            else:
                raise Exception(f"failed to fetch topic:{topic} route.")

    def _remove_unused_topic_route_data(self, topic):
        self.__topic_route_cache.remove(topic)
        self.__topics.remove(topic)

    def _sign(self):
        return Signature.metadata(self.__client_configuration, self.__client_id)

    def _rpc_channel_io_loop(self):
        return self.__rpc_client.get_channel_io_loop()

    def _submit_callback(self, callback_result):
        self.__client_callback_executor.submit(Client.__handle_callback, callback_result)

    """ private """

    # topic route #

    def __update_topic_route(self, topic):
        event = threading.Event()
        callback = functools.partial(
            self.__query_topic_route_async_callback, topic=topic, event=event
        )
        future = self.__rpc_client.query_topic_route_async(
            self.__client_configuration.rpc_endpoints,
            self.__topic_route_req(topic),
            metadata=self._sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.add_done_callback(callback)
        event.wait()
        return self.__topic_route_cache.get(topic)

    def __update_topic_route_async(self, topic):
        callback = functools.partial(
            self.__query_topic_route_async_callback, topic=topic
        )
        future = self.__rpc_client.query_topic_route_async(
            self.__client_configuration.rpc_endpoints,
            self.__topic_route_req(topic),
            metadata=self._sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.add_done_callback(callback)

    def __query_topic_route_async_callback(self, future, topic, event=None):
        try:
            res = future.result()
            self.__handle_topic_route_res(res, topic)
        except Exception as e:
            raise e
        finally:
            if event is not None:
                event.set()

    def __topic_route_req(self, topic):
        req = QueryRouteRequest()
        req.topic.name = topic
        req.topic.resource_namespace = self.__client_configuration.namespace
        req.endpoints.CopyFrom(self.__client_configuration.rpc_endpoints.endpoints)
        return req

    def __handle_topic_route_res(self, res, topic):
        if res is not None:
            MessagingResultChecker.check(res.status)
            if res.status.code == Code.OK:
                topic_route = TopicRouteData(res.message_queues)
                logger.info(
                    f"{self.__str__()} update topic:{topic} route, route info: {topic_route.__str__()}"
                )
                # if topic route has new endpoint, connect
                self.__check_topic_route_endpoints_changed(topic, topic_route)
                self.__topic_route_cache.put(topic, topic_route)
                # producer or consumer update its queue selector
                self._update_queue_selector(topic, topic_route)
                return topic_route
        else:
            raise Exception(f"query topic route exception, topic:{topic}")

    # heartbeat #

    def __heartbeat_async(self, endpoints):
        req = self._heartbeat_req()
        callback = functools.partial(self.__heartbeat_callback, endpoints=endpoints)
        future = self.__rpc_client.heartbeat_async(
            endpoints,
            req,
            metadata=self._sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.add_done_callback(callback)

    def __heartbeat_callback(self, future, endpoints):
        try:
            res = future.result()
            if res is not None and res.status.code == Code.OK:
                logger.info(
                    f"{self.__str__()} send heartbeat to {endpoints.__str__()} success."
                )
            else:
                if res is not None:
                    logger.error(
                        f"{self.__str__()} send heartbeat to {endpoints.__str__()} error, code:{res.status.code}, message:{res.status.message}."
                    )
                else:
                    logger.error(
                        f"{self.__str__()} send heartbeat to {endpoints.__str__()} error, response is none."
                    )
        except Exception as e:
            logger.error(
                f"{self.__str__()} send heartbeat to {endpoints.__str__()} exception, e: {e}"
            )
            raise e

    # sync settings #

    def __retrieve_telemetry_stream_stream_call(self, endpoints, rebuild=False):
        try:
            self.__rpc_client.telemetry_stream(
                endpoints, self, self._sign(), rebuild, timeout=60 * 60 * 24 * 365
            )
        except Exception as e:
            logger.error(
                f"{self.__str__()} rebuild stream_steam_call to {endpoints.__str__()} exception: {e}"
                if rebuild
                else f"{self.__str__()} create stream_steam_call to {endpoints.__str__()} exception: {e}"
            )

    def __setting_write(self, endpoints):
        req = self._sync_setting_req(endpoints)
        callback = functools.partial(self.__setting_write_callback, endpoints=endpoints)
        future = self.__rpc_client.telemetry_write_async(endpoints, req)
        logger.debug(f"{self.__str__()} send setting to {endpoints.__str__()}, {req}")
        future.add_done_callback(callback)

    def __setting_write_callback(self, future, endpoints):
        try:
            future.result()
            logger.info(
                f"{self.__str__()} send setting to {endpoints.__str__()} success."
            )
        except InvalidStateError as e:
            logger.warn(
                f"{self.__str__()} send setting to {endpoints.__str__()} occurred InvalidStateError: {e}"
            )
            self.__retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)
        except AioRpcError as e:
            logger.warn(
                f"{self.__str__()} send setting to {endpoints.__str__()} occurred AioRpcError: {e}"
            )
            self.__retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)
        except Exception as e:
            logger.error(
                f"{self.__str__()} send setting to {endpoints.__str__()} exception: {e}"
            )
            self.__retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)

    # metrics #

    def reset_metric(self, metric):
        self.__client_metrics.reset_metrics(metric)

    # client termination #

    def __client_termination(self, endpoints):
        req = self._notify_client_termination_req()
        future = self.__rpc_client.notify_client_termination(
            endpoints,
            req,
            metadata=self._sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.result()

    # others ##

    def __get_all_endpoints(self):
        endpoints_map = {}
        all_route = self.__topic_route_cache.values()
        for topic_route in all_route:
            endpoints_map.update(topic_route.all_endpoints())
        return endpoints_map

    def __check_topic_route_endpoints_changed(self, topic, route):
        old_route = self.__topic_route_cache.get(topic)
        if old_route is None or old_route != route:
            logger.info(
                f"topic:{topic} route changed for {self.__str__()}. old route is {old_route}, new route is {route}"
            )
        all_endpoints = self.__get_all_endpoints()  # the existing endpoints
        topic_route_endpoints = (
            route.all_endpoints()
        )  # the latest endpoints for topic route
        diff = set(topic_route_endpoints.keys()).difference(
            set(all_endpoints.keys())
        )  # the diff between existing and latest
        # create grpc channel, stream_stream_call for new endpoints, send setting to new endpoints
        for address in diff:
            endpoints = topic_route_endpoints[address]
            self.__retrieve_telemetry_stream_stream_call(endpoints)
            self.__setting_write(endpoints)

    def __notify_client_termination(self):
        all_endpoints = self.__get_all_endpoints()
        for endpoints in all_endpoints.values():
            try:
                self.__client_termination(endpoints)
            except Exception as e:
                logger.error(f"notify client termination to {endpoints} exception: {e}")

    def __stop_client_threads(self):
        self.__client_thread_task_enabled = False
        if self.__topic_route_scheduler is not None:
            if self.__topic_route_scheduler_threading_event is not None:
                self.__topic_route_scheduler_threading_event.set()
                self.__topic_route_scheduler.join()

        if self.__heartbeat_scheduler is not None:
            if self.__heartbeat_scheduler_threading_event is not None:
                self.__heartbeat_scheduler_threading_event.set()
                self.__heartbeat_scheduler.join()

        if self.__sync_setting_scheduler is not None:
            if self.__sync_setting_scheduler_threading_event is not None:
                self.__sync_setting_scheduler_threading_event.set()
                self.__sync_setting_scheduler.join()

        if self.__clear_idle_rpc_channels_scheduler is not None:
            if self.__clear_idle_rpc_channels_threading_event is not None:
                self.__clear_idle_rpc_channels_threading_event.set()
                self.__clear_idle_rpc_channels_scheduler.join()

        if self.__client_callback_executor is not None:
            self.__client_callback_executor.shutdown()

        self.__topic_route_scheduler = None
        self.__topic_route_scheduler_threading_event = None
        self.__heartbeat_scheduler = None
        self.__heartbeat_scheduler_threading_event = None
        self.__sync_setting_scheduler = None
        self.__sync_setting_scheduler_threading_event = None
        self.__clear_idle_rpc_channels_scheduler = None
        self.__clear_idle_rpc_channels_threading_event = None
        self.__client_callback_executor = None

    """ property """

    @property
    def is_running(self):
        return self.__is_running

    @property
    def client_id(self):
        return self.__client_id

    @property
    def topics(self):
        return self.__topics

    @property
    def client_configuration(self):
        return self.__client_configuration

    @property
    def client_type(self):
        return self.__client_type

    @property
    def rpc_client(self):
        return self.__rpc_client

    @property
    def client_metrics(self):
        return self.__client_metrics
