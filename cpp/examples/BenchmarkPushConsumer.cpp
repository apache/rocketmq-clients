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
#include "rocketmq/DefaultMQPushConsumer.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

using namespace rocketmq;

class CounterMessageListener : public StandardMessageListener {
public:
  explicit CounterMessageListener(std::atomic_long& counter) : counter_(counter) {
  }

  ConsumeMessageResult consumeMessage(const std::vector<MQMessageExt>& msgs) override {
    counter_.fetch_add(msgs.size());
    return ConsumeMessageResult::SUCCESS;
  }

private:
  std::atomic_long& counter_;
};

int main(int argc, char* argv[]) {

  Logger& logger = getLogger();
  logger.setLevel(Level::Debug);
  logger.init();

  std::atomic_long counter(0);

  DefaultMQPushConsumer push_consumer("CID_sample");
  MessageListener* listener = new CounterMessageListener(counter);

  push_consumer.setGroupName("CID_sample");
  push_consumer.setInstanceName("CID_sample_member_0");
  push_consumer.subscribe("TopicTest", "*");
  push_consumer.setNamesrvAddr("11.167.164.105:9876");
  push_consumer.registerMessageListener(listener);
  push_consumer.start();

  std::atomic_bool stopped(false);
  std::thread report_thread([&counter, &stopped]() {
    while (!stopped) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      long qps;
      while (true) {
        qps = counter.load(std::memory_order_relaxed);
        if (counter.compare_exchange_weak(qps, 0, std::memory_order_relaxed)) {
          break;
        }
      }
      std::cout << "QPS: " << qps << std::endl;
    }
  });

  std::this_thread::sleep_for(std::chrono::minutes(30));
  stopped.store(true);

  if (report_thread.joinable()) {
    report_thread.join();
  }

  push_consumer.shutdown();
  return EXIT_SUCCESS;
}
