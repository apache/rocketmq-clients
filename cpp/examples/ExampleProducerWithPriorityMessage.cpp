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

#include "rocketmq/Producer.h"
#include "rocketmq/Message.h"

using namespace rocketmq;

int main() {
  // Create producer configuration with topic
  auto producer = Producer::newBuilder()
                      .withConfiguration(Configuration::newBuilder()
                                             .withEndpoints("127.0.0.1:9876")
                                             .build())
                      .withTopics({"PriorityMessageTopic"})
                      .build();
  
  // Send priority messages with different priority levels
  for (int i = 0; i < 5; ++i) {
    // Create a priority message
    auto message = Message::newBuilder()
                       .withTopic("PriorityMessageTopic")
                       .withTag("PriorityTag")
                       .withKeys({"key1", "key2"})
                       .withBody("This is a priority message with level " + std::to_string(i))
                       .withPriority(i)  // Set priority level (higher value = higher priority)
                       .build();
    
    // Send the message
    std::error_code ec;
    auto send_receipt = producer.send(std::move(message), ec);
    if (ec) {
      std::cerr << "Failed to send message: " << ec.message() << std::endl;
    } else {
      std::cout << "Sent priority message with level " << i 
                << ", message ID: " << send_receipt.message_id << std::endl;
    }
    
    // Small delay between sends
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  
  std::cout << "Producer finished sending messages" << std::endl;
  return 0;
}
