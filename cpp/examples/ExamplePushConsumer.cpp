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
#include <thread>

#include "gflags/gflags.h"
#include "rocketmq/Logger.h"
#include "rocketmq/PushConsumer.h"

using namespace ROCKETMQ_NAMESPACE;

DEFINE_string(topic, "NormalTopic", "Topic to which messages are published");
DEFINE_string(access_point, "127.0.0.1:8081", "Service access URL, provided by your service provider");
DEFINE_string(group, "PushConsumer", "GroupId, created through your instance management console");
DEFINE_string(access_key, "", "Your access key ID");
DEFINE_string(access_secret, "", "Your access secret");
DEFINE_bool(tls, false, "Use HTTP2 with TLS/SSL");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto& logger = getLogger();
  logger.setConsoleLevel(Level::Info);
  logger.setLevel(Level::Info);
  logger.init();

  std::string tag = "*";

  auto listener = [](const Message& message) {
    std::cout << "Received a message[topic=" << message.topic() << ", MsgId=" << message.id() << "]" << std::endl;
    return ConsumeResult::SUCCESS;
  };

  CredentialsProviderPtr credentials_provider;
  if (!FLAGS_access_key.empty() && !FLAGS_access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(FLAGS_access_key, FLAGS_access_secret);
  }

  // In most case, you don't need to create too many consumers, singletion pattern is recommended.
  auto push_consumer = PushConsumer::newBuilder()
                           .withGroup(FLAGS_group)
                           .withConfiguration(Configuration::newBuilder()
                                                  .withEndpoints(FLAGS_access_point)
                                                  .withRequestTimeout(std::chrono::seconds(3))
                                                  .withCredentialsProvider(credentials_provider)
                                                  .withSsl(FLAGS_tls)
                                                  .build())
                           .withConsumeThreads(4)
                           .withListener(listener)
                           .subscribe(FLAGS_topic, tag)
                           .build();

  std::this_thread::sleep_for(std::chrono::minutes(30));

  return EXIT_SUCCESS;
}
