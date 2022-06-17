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
#include "rocketmq/State.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

ROCKETMQ_NAMESPACE_BEGIN

class ExecutorImpl {
public:
  ExecutorImpl() : state_(State::CREATED) {
  }

  virtual ~ExecutorImpl() {
    switch (state_.load(std::memory_order_relaxed)) {
      case CREATED:
      case STOPPING:
      case STOPPED:
        break;

      case STARTING:
      case STARTED:
        state_.store(State::STOPPED);
        if (worker_.joinable()) {
          worker_.join();
        }
        break;
    }
  }

  void submit(const std::function<void(void)>& task) {
    if (State::STOPPED == state_.load(std::memory_order_relaxed)) {
      return;
    }

    {
      std::unique_lock<std::mutex> lock(task_mtx_);
      tasks_.push_back(task);
    }
    cv_.notify_one();
  }

  void start() {
    State expected = State::CREATED;
    if (state_.compare_exchange_strong(expected, State::STARTING)) {
      worker_ = std::thread(std::bind(&ExecutorImpl::loop, this));
      state_.store(State::STARTED);
    }
  }

  void stop() {
    state_.store(State::STOPPED);
    if (worker_.joinable()) {
      worker_.join();
    }
  }

private:
  void loop() {
    while (state_.load(std::memory_order_relaxed) != State::STOPPED) {
      std::function<void(void)> func;
      {
        std::unique_lock<std::mutex> lk(task_mtx_);
        if (!tasks_.empty()) {
          func = tasks_.back();
        }
      }

      if (func) {
        func();
      } else {
        std::unique_lock<std::mutex> lk(task_mtx_);
        cv_.wait_for(lk, std::chrono::seconds(3),
                     [&]() { return state_.load(std::memory_order_relaxed) == State::STOPPED || !tasks_.empty(); });
      }
    }
  }

  std::atomic<State> state_;
  std::vector<std::function<void(void)>> tasks_;
  std::mutex task_mtx_;
  std::condition_variable cv_;
  std::thread worker_;
};

class SampleMQMessageListener : public StandardMessageListener {
public:
  ConsumeMessageResult consumeMessage(const std::vector<MQMessageExt>& msgs) override {
    std::lock_guard<std::mutex> lk(console_mtx_);
    for (const MQMessageExt& msg : msgs) {
      std::cout << "Topic=" << msg.getTopic() << ", MsgId=" << msg.getMsgId() << ", Body=" << msg.getBody()
                << std::endl;
    }
    return ConsumeMessageResult::SUCCESS;
  }

private:
  std::mutex console_mtx_;
};

ROCKETMQ_NAMESPACE_END

int main(int argc, char* argv[]) {
  using namespace ROCKETMQ_NAMESPACE;
  Logger& logger = getLogger();
  logger.setLevel(Level::Debug);
  logger.init();

  DefaultMQPushConsumer push_consumer("TestGroup");
  MessageListener* listener = new SampleMQMessageListener;

  auto pool = new ExecutorImpl;
  pool->start();
  push_consumer.setCustomExecutor(std::bind(&ExecutorImpl::submit, pool, std::placeholders::_1));
  push_consumer.setGroupName("TestGroup");
  push_consumer.setInstanceName("CID_sample_member_0");
  push_consumer.subscribe("TestTopic", "*");
  push_consumer.setNamesrvAddr("11.167.164.105:9876");
  push_consumer.registerMessageListener(listener);
  push_consumer.start();

  std::this_thread::sleep_for(std::chrono::minutes(30));
  pool->stop();
  delete pool;

  push_consumer.shutdown();
  return EXIT_SUCCESS;
}
