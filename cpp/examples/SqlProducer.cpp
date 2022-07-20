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
#include <algorithm>
#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <system_error>

#include "gflags/gflags.h"
#include "rocketmq/Producer.h"
#include "rocketmq/RocketMQ.h"

using namespace ROCKETMQ_NAMESPACE;

const std::string& alphaNumeric() {
  static std::string alpha_numeric("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  return alpha_numeric;
}

std::string randomString(std::string::size_type len) {
  std::string result;
  result.reserve(len);
  std::random_device rd;
  std::mt19937 generator(rd());
  std::string source(alphaNumeric());
  std::string::size_type generated = 0;
  while (generated < len) {
    std::shuffle(source.begin(), source.end(), generator);
    std::string::size_type delta = std::min({len - generated, source.length()});
    result.append(source.substr(0, delta));
    generated += delta;
  }
  return result;
}

DEFINE_string(topic, "lingchu_normal_topic", "Topic to which messages are published");
DEFINE_string(access_point, "121.196.167.124:8081", "Service access URL, provided by your service provider");
DEFINE_int32(message_body_size, 4096, "Message body size");
DEFINE_uint32(total, 256, "Number of sample messages to publish");

// Publish messages with various tags, keys that may be filtered with SQL-expression.
int main(int argc, char* argv[]) {
  // Set log level for file/console sinks
  Logger& logger = getLogger();
  logger.setLevel(Level::Info);
  logger.setConsoleLevel(Level::Info);
  logger.init();

  auto producer = Producer::newBuilder()
                      .withConfiguration(Configuration::newBuilder().withEndpoints(FLAGS_access_point).build())
                      .build();

  std::string body = randomString(FLAGS_message_body_size);

  try {
    for (std::size_t i = 0; i < FLAGS_total; ++i) {
      std::error_code ec;
      MessageConstPtr message;
      switch (i % 3) {
        case 0: {
          message = Message::newBuilder()
                        .withTopic(FLAGS_topic)
                        .withTag("TagA")
                        .withKeys({"Key-" + std::to_string(i)})
                        .withBody(body)
                        .build();
          break;
        }
        case 1: {
          message = Message::newBuilder()
                        .withTopic(FLAGS_topic)
                        .withTag("TagB")
                        .withKeys({"Key-" + std::to_string(i)})
                        .withBody(body)
                        .build();
          break;
        }
        case 2: {
          message = Message::newBuilder()
                        .withTopic(FLAGS_topic)
                        .withTag("TagC")
                        .withKeys({"Key-" + std::to_string(i)})
                        .withBody(body)
                        .build();
          break;
        }
      }
      auto send_receipt = producer.send(std::move(message), ec);
      if (ec) {
        std::cerr << "Failed to send message. Cause: " << ec.message() << std::endl;
      } else {
        std::cout << "Publish message[MsgId=" << send_receipt.message_id << "] to " << FLAGS_topic << " OK."
                  << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } catch (...) {
    std::cerr << "Ah...No!!!" << std::endl;
  }
  return EXIT_SUCCESS;
}