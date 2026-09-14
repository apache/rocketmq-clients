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
    """Thread-safe dictionary wrapper using a reentrant lock.

    Provides atomic get/put/remove operations for shared state access
    across multiple threads (e.g., route cache, assignment cache).
    """

    def __init__(self):
        """Create an empty concurrent map."""
        self._lock = threading.Lock()
        self._map = {}

    def get(self, key, default=None):
        """Get a value by key, returning ``default`` if not found."""
        with self._lock:
            return self._map.get(key, default)

    def put(self, key, value):
        """Insert or update a key-value pair."""
        with self._lock:
            self._map[key] = value

    def remove(self, key):
        """Remove a key and return its value, or ``None`` if not found."""
        with self._lock:
            if key in self._map:
                old = self._map[key]
                del self._map[key]
                return old
            return None

    def update(self, m):
        """Merge all key-value pairs from dict ``m`` into this map."""
        with self._lock:
            self._map.update(m)

    def contains(self, key):
        """Check if the map contains the given key."""
        with self._lock:
            return key in self._map

    def keys(self):
        """Return a list of all keys (snapshot at call time)."""
        with self._lock:
            return list(self._map.keys())

    def values(self):
        """Return a list of all values (snapshot at call time)."""
        with self._lock:
            return list(self._map.values())

    def items(self):
        """Return a list of all key-value pairs (snapshot at call time)."""
        with self._lock:
            return list(self._map.items())

    def put_if_absent(self, key, value):
        """Insert ``key`` with ``value`` only if ``key`` is not already present.

        Returns:
            The existing value if ``key`` was present, otherwise ``value``.
        """
        with self._lock:
            return self._map.setdefault(key, value)

    def clear(self):
        """Remove all entries from the map."""
        with self._lock:
            self._map.clear()
