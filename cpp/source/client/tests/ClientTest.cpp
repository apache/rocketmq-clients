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
#include <iostream>
#include <memory>

#include "ClientMock.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientTest : public testing::Test {
public:
  void SetUp() override {
    client_ = std::make_shared<testing::NiceMock<ClientMock>>();
    ON_CALL(*client_, active).WillByDefault(testing::Invoke([]() {
      std::cout << "active() is invoked" << std::endl;
      return true;
    }));
  }

  void TearDown() override {
  }

protected:
  std::shared_ptr<testing::NiceMock<ClientMock>> client_;
};

TEST_F(ClientTest, testActive) {
  EXPECT_TRUE(client_->active());
}

ROCKETMQ_NAMESPACE_END