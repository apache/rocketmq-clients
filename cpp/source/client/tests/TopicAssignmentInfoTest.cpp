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
#include <apache/rocketmq/v2/definition.pb.h>

#include <iostream>

#include "TopicAssignmentInfo.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

class QueryAssignmentInfoTest : public testing::Test {
protected:
  std::string resource_namespace_{"mq://test"};
  std::string topic_{"TopicTest"};
  std::string broker_name_{"broker-a"};
  int broker_id_ = 0;
  int total_ = 16;
};

TEST_F(QueryAssignmentInfoTest, testCtor) {
  QueryAssignmentResponse response;
  for (int i = 0; i < total_; i++) {
    auto assignment = new rmq::Assignment;
    assignment->mutable_message_queue()->mutable_topic()->set_resource_namespace(resource_namespace_);
    assignment->mutable_message_queue()->mutable_topic()->set_name(topic_);
    assignment->mutable_message_queue()->set_id(i);
    assignment->mutable_message_queue()->set_permission(rmq::Permission::READ);
    auto broker = assignment->mutable_message_queue()->mutable_broker();
    broker->set_name(broker_name_);
    broker->set_id(broker_id_);
    broker->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);

    auto address = new rmq::Address;
    address->set_host("10.0.0.1");
    address->set_port(10911);
    broker->mutable_endpoints()->mutable_addresses()->AddAllocated(address);
    response.mutable_assignments()->AddAllocated(assignment);
  }
  response.mutable_status()->set_code(rmq::Code::OK);

  TopicAssignment assignment(response);
  EXPECT_EQ(total_, assignment.assignmentList().size());
  const auto& item = *assignment.assignmentList().begin();
  EXPECT_EQ(item.message_queue().broker().name(), broker_name_);
  EXPECT_EQ(item.message_queue().topic().name(), topic_);
  EXPECT_TRUE(item.message_queue().id() < 16);
}

TEST_F(QueryAssignmentInfoTest, testCtor2) {
  QueryAssignmentResponse response;
  for (int i = 0; i < total_; i++) {
    auto assignment = new rmq::Assignment;
    assignment->mutable_message_queue()->mutable_topic()->set_resource_namespace(resource_namespace_);
    assignment->mutable_message_queue()->mutable_topic()->set_name(topic_);
    assignment->mutable_message_queue()->set_id(i);
    assignment->mutable_message_queue()->set_permission(rmq::Permission::READ_WRITE);
    auto broker = assignment->mutable_message_queue()->mutable_broker();
    broker->set_name(broker_name_);
    broker->set_id(broker_id_);
    broker->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);

    auto address = new rmq::Address;
    address->set_host("10.0.0.1");
    address->set_port(10911);
    broker->mutable_endpoints()->mutable_addresses()->AddAllocated(address);
    response.mutable_assignments()->AddAllocated(assignment);
  }
  response.mutable_status()->set_code(rmq::Code::OK);

  TopicAssignment assignment(response);
  EXPECT_EQ(total_, assignment.assignmentList().size());
  const auto& item = *assignment.assignmentList().begin();
  EXPECT_EQ(item.message_queue().broker().name(), broker_name_);
  EXPECT_EQ(item.message_queue().topic().name(), topic_);
  EXPECT_TRUE(item.message_queue().id() < 16);
}

TEST_F(QueryAssignmentInfoTest, testCtor3) {
  QueryAssignmentResponse response;
  for (int i = 0; i < total_; i++) {
    auto assignment = new rmq::Assignment;
    assignment->mutable_message_queue()->mutable_topic()->set_resource_namespace(resource_namespace_);
    assignment->mutable_message_queue()->mutable_topic()->set_name(topic_);
    assignment->mutable_message_queue()->set_id(i);
    assignment->mutable_message_queue()->set_permission(rmq::Permission::NONE);
    auto broker = assignment->mutable_message_queue()->mutable_broker();
    broker->set_name(broker_name_);
    broker->set_id(broker_id_);
    broker->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);

    auto address = new rmq::Address;
    address->set_host("10.0.0.1");
    address->set_port(10911);
    broker->mutable_endpoints()->mutable_addresses()->AddAllocated(address);
    response.mutable_assignments()->AddAllocated(assignment);
  }
  response.mutable_status()->set_code(rmq::Code::OK);
  TopicAssignment assignment(response);
  EXPECT_TRUE(assignment.assignmentList().empty());
}

ROCKETMQ_NAMESPACE_END