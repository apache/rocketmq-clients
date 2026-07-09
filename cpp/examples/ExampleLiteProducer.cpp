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
#include "rocketmq/Producer.h"

using namespace ROCKETMQ_NAMESPACE;

DEFINE_string(topic, "LiteTopic", "Parent topic for lite messages");
DEFINE_string(access_point, "127.0.0.1:8081", "Service access URL, provided by your service provider");
DEFINE_string(access_key, "", "Your access key ID");
DEFINE_string(access_secret, "", "Your access secret");
DEFINE_bool(tls, false, "Use HTTP2 with TLS/SSL");
DEFINE_int32(message_count, 5, "Number of lite messages to send");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto& logger = getLogger();
  logger.setConsoleLevel(Level::Info);
  logger.setLevel(Level::Info);
  logger.init();

  std::cout << "=== Lite Producer Example ===" << std::endl;

  CredentialsProviderPtr credentials_provider;
  if (!FLAGS_access_key.empty() && !FLAGS_access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(FLAGS_access_key, FLAGS_access_secret);
  }

  auto producer = Producer::newBuilder()
                      .withConfiguration(Configuration::newBuilder()
                                             .withEndpoints(FLAGS_access_point)
                                             .withRequestTimeout(std::chrono::seconds(3))
                                             .withCredentialsProvider(credentials_provider)
                                             .withSsl(FLAGS_tls)
                                             .build())
                      .build();

  std::cout << "Producer started successfully" << std::endl;

  // Send lite topic messages
  for (int i = 1; i <= FLAGS_message_count; ++i) {
    std::string lite_topic_name = "lite-topic-" + std::to_string(i);
    std::string message_body = "This is a lite message for Apache RocketMQ, index=" + std::to_string(i);

    auto message = Message::newBuilder()
                       .withTopic(FLAGS_topic)
                       .withBody(message_body)
                       .withKeys({"key-" + std::to_string(i)})
                       .withLiteTopic(lite_topic_name)
                       .build();

    std::error_code ec;
    auto send_receipt = producer.send(std::move(message), ec);

    if (ec) {
      std::cerr << "Failed to send message " << i << ": " << ec.message() << std::endl;
    } else {
      std::cout << "Message " << i << " sent successfully" << std::endl;
      std::cout << "  - Topic: " << FLAGS_topic << std::endl;
      std::cout << "  - Lite Topic: " << lite_topic_name << std::endl;
      std::cout << "  - Message ID: " << send_receipt.message_id << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::cout << "All messages sent!" << std::endl;

  return EXIT_SUCCESS;
}
