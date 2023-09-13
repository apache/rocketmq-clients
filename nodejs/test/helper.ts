/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { SessionCredentials } from '../src/client';

export const endpoints = process.env.ROCKETMQ_NODEJS_CLIENT_ENDPOINTS ?? 'localhost:8081';
export const topics = {
  normal: 'TopicTestForNormal',
  fifo: 'TopicTestForFifo',
  delay: 'TopicTestForDelay',
  transaction: 'TopicTestForTransaction',
};

export const consumerGroup = process.env.ROCKETMQ_NODEJS_CLIENT_GROUP ?? 'nodejs-unittest-group';

export let sessionCredentials: SessionCredentials | undefined;
if (process.env.ROCKETMQ_NODEJS_CLIENT_KEY && process.env.ROCKETMQ_NODEJS_CLIENT_SECRET) {
  sessionCredentials = {
    accessKey: process.env.ROCKETMQ_NODEJS_CLIENT_KEY,
    accessSecret: process.env.ROCKETMQ_NODEJS_CLIENT_SECRET,
  };
}
