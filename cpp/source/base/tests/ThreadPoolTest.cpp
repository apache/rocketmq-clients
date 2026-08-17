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
#include "ThreadPoolImpl.h"
#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/RocketMQ.h"
#include "gtest/gtest.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <thread>

ROCKETMQ_NAMESPACE_BEGIN

class ThreadPoolTest : public testing::Test {
public:
  ThreadPoolTest() = default;

  void SetUp() override {
    pool_ = absl::make_unique<ThreadPoolImpl>(2);
    pool_->start();
    completed = false;
  }

  void TearDown() override {
    pool_->shutdown();
  }

protected:
  std::unique_ptr<ThreadPool> pool_;
  absl::Mutex mtx;
  absl::CondVar cv;
  bool completed{false};
};

TEST_F(ThreadPoolTest, testBasics) {

  auto task = [this](int cnt) {
    for (int i = 0; i < cnt; i++) {
      std::cout << std::this_thread::get_id() << ": It works" << std::endl;
    }
    {
      absl::MutexLock lk(&mtx);
      if (!completed) {
        completed = true;
        cv.SignalAll();
      }
    }
  };

  for (int i = 0; i < 3; i++) {
    pool_->submit(std::bind(task, 3));
  }

  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.Wait(&mtx);
    }
  }
}

// Regression: shutting the pool down from within one of its own worker threads
// must not attempt to join the calling thread (self-join deadlocks and raises
// std::system_error EDEADLK). This mirrors the PushConsumer teardown race where
// ~PushConsumerImpl runs on a consume worker and drives ThreadPoolImpl::shutdown().
TEST_F(ThreadPoolTest, shutdownFromWorkerThreadDoesNotThrowTest) {
  std::atomic<bool> threw{false};
  std::atomic<bool> finished{false};

  pool_->submit([&]() {
    try {
      pool_->shutdown();
    } catch (...) {
      threw.store(true);
    }
    finished.store(true);
  });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!finished.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(finished.load());
  EXPECT_FALSE(threw.load());

  // Give the detached worker a moment to unwind out of io_context::run()
  // before the fixture destroys the pool (test-only synchronization).
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

ROCKETMQ_NAMESPACE_END
