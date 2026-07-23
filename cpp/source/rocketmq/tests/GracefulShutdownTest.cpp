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
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "ClientManagerMock.h"
#include "PushConsumerImpl.h"
#include "ThreadPoolImpl.h"
#include "absl/memory/memory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/ConsumeResult.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

std::shared_ptr<PushConsumerImpl> createConsumer(const std::string& group = "test-group") {
  auto consumer = std::make_shared<PushConsumerImpl>(group);
  auto client_manager = std::make_shared<testing::NiceMock<ClientManagerMock>>();
  consumer->clientManager(client_manager);
  return consumer;
}

} // namespace

TEST(GracefulShutdownTest, inflightCounterInitiallyZero) {
  auto consumer = createConsumer();
  EXPECT_EQ(0, consumer->inflightReceiveRequestCount());
}

TEST(GracefulShutdownTest, inflightCounterIncrementDecrement) {
  auto consumer = createConsumer();
  consumer->incrementInflightReceiveRequests();
  consumer->incrementInflightReceiveRequests();
  EXPECT_EQ(2, consumer->inflightReceiveRequestCount());

  consumer->decrementInflightReceiveRequests();
  EXPECT_EQ(1, consumer->inflightReceiveRequestCount());

  consumer->decrementInflightReceiveRequests();
  EXPECT_EQ(0, consumer->inflightReceiveRequestCount());
}

TEST(GracefulShutdownTest, threadPoolGracefulShutdownDrainsTasks) {
  auto pool = absl::make_unique<ThreadPoolImpl>(2);
  pool->start();

  std::atomic<int> completed{0};
  for (int i = 0; i < 10; i++) {
    pool->submit([&completed]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      completed.fetch_add(1);
    });
  }

  // Graceful shutdown should wait for all tasks to complete
  pool->gracefulShutdown();
  EXPECT_EQ(10, completed.load());
}

TEST(GracefulShutdownTest, threadPoolGracefulShutdownRejectsNewTasks) {
  auto pool = absl::make_unique<ThreadPoolImpl>(1);
  pool->start();

  std::atomic<int> completed{0};
  pool->submit([&completed]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    completed.fetch_add(1);
  });

  // Give time for the first task to start
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  pool->gracefulShutdown();

  // After graceful shutdown, new tasks should be rejected
  pool->submit([&completed]() {
    completed.fetch_add(1);
  });

  // Only the first task should have completed
  EXPECT_EQ(1, completed.load());
}

TEST(GracefulShutdownTest, shutdownOnNonStartedConsumerIsNoOp) {
  auto consumer = createConsumer();
  // Consumer is in CREATED state, shutdown should be a no-op
  consumer->shutdown();
  EXPECT_EQ(0, consumer->inflightReceiveRequestCount());
}

ROCKETMQ_NAMESPACE_END
