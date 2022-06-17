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
#include "rocketmq/PushConsumer.h"
#include "spdlog/spdlog.h"

using namespace ROCKETMQ_NAMESPACE;

int main(int argc, char* argv[]) {
  const char* topic = "cpp_sdk_standard";
  const char* name_server = "11.166.42.94:8081";
  const char* group = "GID_cpp_sdk_standard";
  std::string tag = "*";

  auto listener = [](const Message& message) { return ConsumeResult::SUCCESS; };

  auto push_consumer = PushConsumer::newBuilder()
                           .withGroup(group)
                           .withConfiguration(Configuration::newBuilder()
                                                  .withEndpoints(name_server)
                                                  .withRequestTimeout(std::chrono::seconds(3))
                                                  .build())
                           .withConsumeThreads(4)
                           .withListener(listener)
                           .subscribe(topic, tag)
                           .build();

  std::this_thread::sleep_for(std::chrono::minutes(30));

  return EXIT_SUCCESS;
}
