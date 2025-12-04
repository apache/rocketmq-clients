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

from rocketmq.v5.util import Misc
from setuptools import find_packages, setup

setup(
    name='rocketmq-python-client',
    version=Misc.SDK_VERSION,
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.5.0",
        "grpcio-tools>=1.5.0",
        'protobuf',
        "opentelemetry-api>=1.33.0",
        "opentelemetry-sdk>=1.33.0",
        "opentelemetry-exporter-otlp>=1.33.0"
    ],
    python_requires='>=3.7',
)
