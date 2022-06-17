/*
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
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include "rocketmq/Logger.h"

#include "rocketmq/MessageListener.h"
#include "spdlog/spdlog.h"

#include "rocketmq/DefaultMQPushConsumer.h"

using namespace rocketmq;

class SampleMQMessageListener : public FifoMessageListener {
public:
  ConsumeMessageResult consumeMessage(const MQMessageExt& message) override {
    SPDLOG_INFO("Consume message[Topic={}, MessageId={}] OK", message.getTopic(), message.getMsgId());
    std::cout << "Consume Message[MsgId=" << message.getMsgId() << "] OK. Body Size: " << message.getBody().size()
              << std::endl;
    // std::this_thread::sleep_for(std::chrono::seconds(1));
    return ConsumeMessageResult::SUCCESS;
  }
};

int main(int argc, char* argv[]) {

  Logger& logger = getLogger();
  logger.setLevel(Level::Debug);
  logger.init();

  const char* group_id = "GID_lingchu_test_order";
  const char* topic = "lingchu_test_order_topic";
  const char* resource_namespace = "MQ_INST_1080056302921134_BXyTLppt";

  DefaultMQPushConsumer push_consumer(group_id);
  push_consumer.setResourceNamespace(resource_namespace);
  push_consumer.setCredentialsProvider(std::make_shared<ConfigFileCredentialsProvider>());
  push_consumer.setNamesrvAddr("120.25.100.131:8081");
  FifoMessageListener* listener = new SampleMQMessageListener();
  push_consumer.setInstanceName("instance_0");
  push_consumer.subscribe(topic, "*");
  push_consumer.registerMessageListener(listener);
  push_consumer.setConsumeThreadCount(4);
  push_consumer.start();

  std::this_thread::sleep_for(std::chrono::minutes(30));

  push_consumer.shutdown();
  return EXIT_SUCCESS;
}
