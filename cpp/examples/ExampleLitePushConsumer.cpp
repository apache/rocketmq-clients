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
#include <system_error>
#include <thread>

#include "gflags/gflags.h"
#include "rocketmq/LitePushConsumer.h"
#include "rocketmq/Logger.h"
#include "rocketmq/OffsetOption.h"

using namespace ROCKETMQ_NAMESPACE;

DEFINE_string(topic, "LiteTopic", "Bind topic to which lite topics belong");
DEFINE_string(access_point, "127.0.0.1:8081", "Service access URL, provided by your service provider");
DEFINE_string(group, "LitePushConsumer", "GroupId, created through your instance management console");
DEFINE_string(access_key, "", "Your access key ID");
DEFINE_string(access_secret, "", "Your access secret");
DEFINE_bool(tls, false, "Use HTTP2 with TLS/SSL");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto& logger = getLogger();
  logger.setConsoleLevel(Level::Info);
  logger.setLevel(Level::Info);
  logger.init();

  auto listener = [](const Message& message) {
    std::cout << "Received a message[topic=" << message.topic()
              << ", liteTopic=" << message.liteTopic()
              << ", MsgId=" << message.id()
              << ", body=" << message.body() << "]" << std::endl;
    return ConsumeResult::SUCCESS;
  };

  CredentialsProviderPtr credentials_provider;
  if (!FLAGS_access_key.empty() && !FLAGS_access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(FLAGS_access_key, FLAGS_access_secret);
  }

  // Build LitePushConsumer
  auto lite_consumer = LitePushConsumer::newBuilder()
                           .bindTopic(FLAGS_topic)
                           .withGroup(FLAGS_group)
                           .withConfiguration(Configuration::newBuilder()
                                                  .withEndpoints(FLAGS_access_point)
                                                  .withRequestTimeout(std::chrono::seconds(3))
                                                  .withCredentialsProvider(credentials_provider)
                                                  .withSsl(FLAGS_tls)
                                                  .build())
                           .withConsumeThreads(4)
                           .withListener(listener)
                           .build();

  std::cout << "LitePushConsumer started, group=" << FLAGS_group
            << ", bindTopic=" << FLAGS_topic << std::endl;

  // Subscribe to lite topics
  std::error_code ec;
  lite_consumer.subscribeLite("lite-topic-1", ec);
  if (ec) {
    std::cerr << "Failed to subscribe lite-topic-1: " << ec.message() << std::endl;
  } else {
    std::cout << "Subscribed to lite-topic-1" << std::endl;
  }

  lite_consumer.subscribeLite("lite-topic-2", OffsetOption::lastOffset(), ec);
  if (ec) {
    std::cerr << "Failed to subscribe lite-topic-2: " << ec.message() << std::endl;
  } else {
    std::cout << "Subscribed to lite-topic-2 with LAST offset" << std::endl;
  }

  // Print current lite topic set
  auto topic_set = lite_consumer.getLiteTopicSet();
  std::cout << "Current lite topic set size: " << topic_set.size() << std::endl;
  for (const auto& topic : topic_set) {
    std::cout << "  - " << topic << std::endl;
  }

  // Keep running
  std::this_thread::sleep_for(std::chrono::minutes(30));

  // Shutdown
  lite_consumer.shutdown();
  std::cout << "LitePushConsumer shutdown" << std::endl;

  return EXIT_SUCCESS;
}
