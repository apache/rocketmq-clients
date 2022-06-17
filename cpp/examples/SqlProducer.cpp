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
#include "rocketmq/DefaultMQProducer.h"
#include <iostream>

using namespace rocketmq;

int main(int argc, char* argv[]) {

  Logger& logger = getLogger();
  logger.setLevel(Level::Debug);
  logger.init();

  DefaultMQProducer producer("PID_sample");
  producer.setNamesrvAddr("11.167.164.105:9876");

  MQMessage message;
  message.setTopic("TestTopic");
  try {
    producer.start();
    for (int i = 0; i < 8; ++i) {
      std::string body = std::to_string(i);
      message.setBody(body);
      message.setProperty("a", std::to_string(i % 5));
      switch (i % 3) {
        case 0:
          message.setTags("TagA");
          break;
        case 1:
          message.setTags("TagB");
          break;
        case 2:
          message.setTags("TagC");
          break;
      }
      SendResult sendResult = producer.send(message);
      std::cout << "Message sent with msgId=" << sendResult.getMsgId()
                << ", Queue=" << sendResult.getMessageQueue().simpleName() << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } catch (...) {
    std::cerr << "Ah...No!!!" << std::endl;
  }
  producer.shutdown();
  return EXIT_SUCCESS;
}