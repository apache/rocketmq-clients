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

#include "gflags/gflags.h"
#include "rocketmq/Logger.h"
#include "rocketmq/SimpleConsumer.h"

using namespace ROCKETMQ_NAMESPACE;

DEFINE_string(topic, "standard_topic_sample", "Topic to which messages are published");
DEFINE_string(access_point, "121.196.167.124:8081", "Service access URL, provided by your service provider");
DEFINE_string(group, "CID_standard_topic_sample", "GroupId, created through your instance management console");
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

  CredentialsProviderPtr credentials_provider;
  if (!FLAGS_access_key.empty() && !FLAGS_access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(FLAGS_access_key, FLAGS_access_secret);
  }

  // In most case, you don't need to create too many consumers, singletion pattern is recommended.
  auto simple_consumer = SimpleConsumer::newBuilder()
                             .withGroup(FLAGS_group)
                             .withConfiguration(Configuration::newBuilder()
                                                    .withEndpoints(FLAGS_access_point)
                                                    .withCredentialsProvider(credentials_provider)
                                                    .withSsl(FLAGS_tls)
                                                    .build())
                             .subscribe(FLAGS_topic, tag)
                             .build();
  std::vector<MessageConstSharedPtr> messages;
  std::error_code ec;
  simple_consumer.receive(4, std::chrono::seconds(3), ec, messages);

  if (ec) {
    std::cerr << "Failed to receive messages. Cause: " << ec.message() << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "Received " << messages.size() << " messages" << std::endl;
  std::size_t i = 0;
  for (const auto& message : messages) {
    std::cout << "Received a message[topic=" << message->topic() << ", message-id=" << message->id()
              << ", receipt-handle='" << message->extension().receipt_handle << "']" << std::endl;

    std::error_code ec;
    if (++i % 2 == 0) {
      simple_consumer.ack(*message, ec);
      if (ec) {
        std::cerr << "Failed to ack message. Cause: " << ec.message() << std::endl;
      }
    } else {
      simple_consumer.changeInvisibleDuration(*message, std::chrono::milliseconds(100), ec);
      if (ec) {
        std::cerr << "Failed to change invisible duration of message. Cause: " << ec.message() << std::endl;
      }
    }
  }

  return EXIT_SUCCESS;
}