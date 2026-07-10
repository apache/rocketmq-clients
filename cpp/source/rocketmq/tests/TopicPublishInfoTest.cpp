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
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "MixAll.h"
#include "ProducerImpl.h"
#include "Protocol.h"
#include "TopicPublishInfo.h"
#include "TopicRouteData.h"
#include "absl/types/optional.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

rmq::MessageQueue createMessageQueue(const std::string& topic, int id, const std::string& broker_name,
                                     int32_t broker_id, rmq::Permission perm) {
  rmq::MessageQueue mq;
  mq.mutable_topic()->set_name(topic);
  mq.set_id(id);
  mq.mutable_broker()->set_name(broker_name);
  mq.mutable_broker()->set_id(broker_id);
  mq.mutable_broker()->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);
  auto* addr = mq.mutable_broker()->mutable_endpoints()->add_addresses();
  addr->set_host("10.0.0." + std::to_string(broker_id + 1));
  addr->set_port(8080);
  mq.set_permission(perm);
  return mq;
}

}  // namespace

class TopicPublishInfoTest : public testing::Test {
protected:
  void SetUp() override {
    producer_ = std::make_shared<ProducerImpl>();
    producer_->state(State::STARTED);
    producer_->maxAttemptTimes(3);

    // 4 writable queues across 2 brokers
    std::vector<rmq::MessageQueue> queues;
    for (int i = 0; i < 4; i++) {
      queues.push_back(createMessageQueue("test-topic", i, "broker-" + std::to_string(i % 2),
                                          MixAll::MASTER_BROKER_ID, rmq::Permission::READ_WRITE));
    }
    route_data_ = std::make_shared<TopicRouteData>(queues);
  }

  std::shared_ptr<ProducerImpl> producer_;
  TopicRouteDataPtr route_data_;
};

TEST_F(TopicPublishInfoTest, selectMessageQueuesReturnsResultsTest) {
  TopicPublishInfo info(producer_, "test-topic", route_data_);
  std::vector<rmq::MessageQueue> result;
  EXPECT_TRUE(info.selectMessageQueues(absl::nullopt, result));
  EXPECT_FALSE(result.empty());
}

TEST_F(TopicPublishInfoTest, selectMessageQueuesWithMessageGroupTest) {
  TopicPublishInfo info(producer_, "test-topic", route_data_);

  // Same message group should always select the same queue
  std::vector<rmq::MessageQueue> result1, result2;
  EXPECT_TRUE(info.selectMessageQueues(absl::make_optional<std::string>("group-A"), result1));
  EXPECT_TRUE(info.selectMessageQueues(absl::make_optional<std::string>("group-A"), result2));
  ASSERT_EQ(1u, result1.size());
  ASSERT_EQ(1u, result2.size());
  EXPECT_EQ(result1[0].id(), result2[0].id());
}

TEST_F(TopicPublishInfoTest, selectMessageQueuesSkipsNonWritableTest) {
  // Build route with queue 0 set to READ-only
  std::vector<rmq::MessageQueue> queues;
  for (int i = 0; i < 4; i++) {
    rmq::Permission perm = (i == 0) ? rmq::Permission::READ : rmq::Permission::READ_WRITE;
    queues.push_back(createMessageQueue("test-topic", i, "broker-" + std::to_string(i % 2),
                                        MixAll::MASTER_BROKER_ID, perm));
  }
  auto route = std::make_shared<TopicRouteData>(queues);
  TopicPublishInfo info(producer_, "test-topic", route);

  auto filtered = info.getMessageQueueList();
  for (const auto& q : filtered) {
    EXPECT_TRUE(writable(q.permission()));
  }
  // Only 3 writable queues should remain
  EXPECT_EQ(3u, filtered.size());
}

TEST_F(TopicPublishInfoTest, selectMessageQueuesSkipsNonMasterBrokerTest) {
  // Build route with queue 0 on a non-master broker
  std::vector<rmq::MessageQueue> queues;
  for (int i = 0; i < 4; i++) {
    int32_t broker_id = (i == 0) ? 1 : MixAll::MASTER_BROKER_ID;
    queues.push_back(createMessageQueue("test-topic", i, "broker-" + std::to_string(i % 2),
                                        broker_id, rmq::Permission::READ_WRITE));
  }
  auto route = std::make_shared<TopicRouteData>(queues);
  TopicPublishInfo info(producer_, "test-topic", route);

  auto filtered = info.getMessageQueueList();
  for (const auto& q : filtered) {
    EXPECT_EQ(MixAll::MASTER_BROKER_ID, q.broker().id());
  }
  // Queue 0 is on a non-master broker, so only 3 queues should remain
  EXPECT_EQ(3u, filtered.size());
}

TEST_F(TopicPublishInfoTest, selectMessageQueuesEmptyRouteReturnsFalseTest) {
  std::vector<rmq::MessageQueue> empty_queues;
  auto empty_route = std::make_shared<TopicRouteData>(empty_queues);
  TopicPublishInfo info(producer_, "test-topic", empty_route);

  std::vector<rmq::MessageQueue> result;
  EXPECT_FALSE(info.selectMessageQueues(absl::nullopt, result));
}

TEST_F(TopicPublishInfoTest, updateRouteDataRefreshesQueuesTest) {
  TopicPublishInfo info(producer_, "test-topic", route_data_);
  EXPECT_EQ(4u, info.getMessageQueueList().size());

  // New route with only 2 queues
  std::vector<rmq::MessageQueue> new_queues;
  for (int i = 0; i < 2; i++) {
    new_queues.push_back(createMessageQueue("test-topic", i, "broker-" + std::to_string(i),
                                            MixAll::MASTER_BROKER_ID, rmq::Permission::READ_WRITE));
  }
  auto new_route = std::make_shared<TopicRouteData>(new_queues);
  info.topicRouteData(new_route);
  EXPECT_EQ(2u, info.getMessageQueueList().size());
}

ROCKETMQ_NAMESPACE_END
