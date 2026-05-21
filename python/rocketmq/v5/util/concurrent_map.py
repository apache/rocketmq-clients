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

import threading


class ConcurrentMap:

    def __init__(self):
        self._lock = threading.Lock()
        self._map = {}

    def get(self, key, default=None):
        with self._lock:
            return self._map.get(key, default)

    def put(self, key, value):
        with self._lock:
            self._map[key] = value

    def remove(self, key):
        with self._lock:
            if key in self._map:
                old = self._map[key]
                del self._map[key]
                return old
            return None

    def update(self, m):
        with self._lock:
            self._map.update(m)

    def contains(self, key):
        with self._lock:
            return key in self._map

    def keys(self):
        with self._lock:
            return list(self._map.keys())

    def values(self):
        with self._lock:
            return list(self._map.values())

    def items(self):
        with self._lock:
            return list(self._map.items())

    def put_if_absent(self, key, value):
        with self._lock:
            return self._map.setdefault(key, value)

    def clear(self):
        with self._lock:
            self._map.clear()
