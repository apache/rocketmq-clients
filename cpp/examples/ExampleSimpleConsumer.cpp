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

#include "rocketmq/SimpleConsumer.h"

using namespace ROCKETMQ_NAMESPACE;

int main(int argc, char* argv[]) {
  const char* group = "ExampleSimpleGroup";
  const char* topic = "cpp_sdk_standard";
  const char* name_server = "11.166.42.94:8081";
  std::string tag = "*";

  auto simple_consumer = SimpleConsumer::newBuilder()
                             .withGroup(group)
                             .withConfiguration(Configuration::newBuilder().withEndpoints(name_server).build())
                             .subscribe(topic, tag)
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