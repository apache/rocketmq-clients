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
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>

#include "SchedulerImpl.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

class SchedulerTest : public testing::Test {
public:
  SchedulerTest() : scheduler(std::make_shared<SchedulerImpl>()) {
  }

  void SetUp() override {
    scheduler->start();
  }

  void TearDown() override {
    scheduler->shutdown();
  }

protected:
  SchedulerSharedPtr scheduler;
};

TEST_F(SchedulerTest, testSingleShot) {
  absl::Mutex mtx;
  absl::CondVar cv;
  int callback_fire_count{0};
  auto callback = [&]() {
    absl::MutexLock lock(&mtx);
    cv.Signal();
    callback_fire_count++;
  };

  scheduler->schedule(callback, "single-shot", std::chrono::milliseconds(10), std::chrono::milliseconds(0));

  // Wait till callback is executed.
  {
    absl::MutexLock lock(&mtx);
    if (!callback_fire_count) {
      cv.Wait(&mtx);
    }
  }
}

TEST_F(SchedulerTest, testCancel) {
  absl::Mutex mtx;
  absl::CondVar cv;
  int callback_fire_count{0};
  auto callback = [&]() {
    absl::MutexLock lock(&mtx);
    cv.Signal();
    callback_fire_count++;
  };

  std::uint32_t task_id =
      scheduler->schedule(callback, "test-cancel", std::chrono::milliseconds(100), std::chrono::milliseconds(100));
  scheduler->cancel(task_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_EQ(0, callback_fire_count);
}

TEST_F(SchedulerTest, testPeriodicShot) {
  absl::Mutex mtx;
  absl::CondVar cv;
  int callback_fire_count{0};
  auto callback = [&]() {
    absl::MutexLock lock(&mtx);
    cv.Signal();
    callback_fire_count++;
  };

  std::uintptr_t task_id =
      scheduler->schedule(callback, "periodic-task", std::chrono::milliseconds(10), std::chrono::milliseconds(100));
  // Wait till callback is executed.
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  ASSERT_TRUE(callback_fire_count >= 4);
  scheduler->cancel(task_id);
}

TEST_F(SchedulerTest, testSingleShotWithZeroDelay) {
  absl::Mutex mtx;
  absl::CondVar cv;
  int callback_fire_count{0};
  auto callback = [&]() {
    absl::MutexLock lock(&mtx);
    cv.Signal();
    callback_fire_count++;
  };

  scheduler->schedule(callback, "single-shot-with-0-delay", std::chrono::milliseconds(0), std::chrono::milliseconds(0));

  // Wait till callback is executed.
  {
    absl::MutexLock lock(&mtx);
    if (!callback_fire_count) {
      cv.Wait(&mtx);
    }
  }
}

ROCKETMQ_NAMESPACE_END