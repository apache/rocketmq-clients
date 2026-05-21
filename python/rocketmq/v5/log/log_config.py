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

import logging.config
import os

__DIR = f'{os.path.expanduser("~/logs/rocketmq_python/")}'

__LOG_CONFIG = {
    "version": 1.0,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(message)s"},
    },
    "handlers": {
        # 'console': {
        #     'level': 'DEBUG',
        #     'class': 'logging.StreamHandler',
        #     'formatter': 'standard'
        # },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": f"{__DIR}/rocketmq_client.log",
            "maxBytes": 1024 * 1024 * 100,  # 100MB
            "backupCount": 10,
        },
    },
    "loggers": {
        "rocketmq-python-client": {
            "handlers": ["file"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

os.makedirs(__DIR, exist_ok=True)

logging.config.dictConfig(__LOG_CONFIG)
logger = logging.getLogger("rocketmq-python-client")
