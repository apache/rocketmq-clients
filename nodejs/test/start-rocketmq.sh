#!/bin/bash
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

# Download rocketmq binary and start it
curl https://dist.apache.org/repos/dist/release/rocketmq/5.1.3/rocketmq-all-5.1.3-bin-release.zip -o rocketmq-all-5.1.3-bin-release.zip
unzip rocketmq-all-5.1.3-bin-release.zip
cd rocketmq-all-5.1.3-bin-release

nohup sh bin/mqnamesrv &
sleep 10
tail -n 5 ~/logs/rocketmqlogs/namesrv.log

nohup sh bin/mqbroker -n localhost:9876 --enable-proxy &
sleep 10
tail -n 5 ~/logs/rocketmqlogs/proxy.log 

# Create Topics
sh bin/mqadmin statsAll -n localhost:9876
sh bin/mqadmin topicList -n localhost:9876 -c DefaultCluster
# Normal Message
sh bin/mqadmin updatetopic -n localhost:9876 -t TopicTestForNormal -c DefaultCluster
# FIFO Message
sh bin/mqadmin updatetopic -n localhost:9876 -t TopicTestForFifo -c DefaultCluster -a +message.type=FIFO
# Delay Message
sh bin/mqadmin updatetopic -n localhost:9876 -t TopicTestForDelay -c DefaultCluster -a +message.type=DELAY
# Transaction Message
sh bin/mqadmin updatetopic -n localhost:9876 -t TopicTestForTransaction -c DefaultCluster -a +message.type=TRANSACTION
