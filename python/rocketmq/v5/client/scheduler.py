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
import threading

from rocketmq.v5.log import logger


class ClientScheduler:
    def __init__(self, thread_name, target, initial_delay, delay, loop=None):
        self.__scheduler_thread = threading.Thread(target=functools.partial(self.__run_func, target=target,
                                                                            initial_delay=max(0.0, initial_delay), delay=max(0.0, delay),
                                                                            loop=loop), name=thread_name, daemon=True)
        self.__scheduler_thread_event = threading.Event()
        self.__scheduler_thread_enabled = False

    def start_scheduler(self):
        try:
            if self.__scheduler_thread.is_alive():
                logger.warning(f"scheduler thread {self.__scheduler_thread.name} is already running")
                return
            self.__scheduler_thread_event.clear()
            self.__scheduler_thread_enabled = True
            self.__scheduler_thread.start()
        except Exception as e:
            logger.error(f"start scheduler raise exception, {e}")
            raise e

    def stop_scheduler(self): # noqa
        try:
            if self.__scheduler_thread is not None and self.__scheduler_thread_event is not None:
                self.__scheduler_thread_enabled = False
                self.__scheduler_thread_event.set()
                self.__scheduler_thread.join()
                self.__scheduler_thread = None
                self.__scheduler_thread_event = None
        except Exception as e:
            logger.error(f"stop scheduler raise exception, {e}")
            raise e

    def __run_func(self, target, initial_delay, delay, loop):
        if not loop:
            asyncio.set_event_loop(loop)
        delay_time = initial_delay
        while self.__scheduler_thread_enabled:
            timed_out = not self.__scheduler_thread_event.wait(delay_time)
            if timed_out and self.__scheduler_thread_enabled:
                try:
                    target()
                except Exception as e:
                    logger.error(
                        f"{self.__scheduler_thread.name} run function raise exception: {e}"
                    )
            delay_time = delay
        logger.info(f"stop scheduler: {self.__scheduler_thread.name}")
