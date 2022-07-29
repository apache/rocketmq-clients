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
#include <cstddef>
#include <mutex>
#include <thread>

#include "CountdownLatch.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class CountdownLatchTest : public testing::Test {
public:
  void SetUp() override {
    countdown_latch_ = absl::make_unique<CountdownLatch>(permit_);
  }

  void TearDown() override {
  }

protected:
  const std::size_t permit_{2};
  std::unique_ptr<CountdownLatch> countdown_latch_;
  absl::Mutex mtx_;
};

TEST_F(CountdownLatchTest, testAwait) {
  int count = 0;

  auto lambda = [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      absl::MutexLock lk(&mtx_);
      count++;
    }
    countdown_latch_->countdown();
  };

  std::vector<std::thread> threads;
  for (std::size_t i = 0; i < permit_; i++) {
    auto t = std::thread(lambda);
    threads.push_back(std::move(t));
  }

  countdown_latch_->await();

  ASSERT_EQ(count, permit_);

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

ROCKETMQ_NAMESPACE_END