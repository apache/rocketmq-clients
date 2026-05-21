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
#pragma once

#include "absl/hash/hash.h"
#include "fmt/format.h"

#include "apache/rocketmq/v2/definition.grpc.pb.h"
#include "apache/rocketmq/v2/definition.pb.h"
#include "apache/rocketmq/v2/service.grpc.pb.h"
#include "apache/rocketmq/v2/service.pb.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace rmq = apache::rocketmq::v2;

using ChangeInvisibleDurationRequest = rmq::ChangeInvisibleDurationRequest;
using ChangeInvisibleDurationResponse = rmq::ChangeInvisibleDurationResponse;
using QueryRouteRequest = rmq::QueryRouteRequest;
using QueryRouteResponse = rmq::QueryRouteResponse;
using SendMessageRequest = rmq::SendMessageRequest;
using SendMessageResponse = rmq::SendMessageResponse;
using QueryAssignmentRequest = rmq::QueryAssignmentRequest;
using QueryAssignmentResponse = rmq::QueryAssignmentResponse;
using ReceiveMessageRequest = rmq::ReceiveMessageRequest;
using ReceiveMessageResponse = rmq::ReceiveMessageResponse;
using AckMessageRequest = rmq::AckMessageRequest;
using AckMessageResponse = rmq::AckMessageResponse;
using HeartbeatRequest = rmq::HeartbeatRequest;
using HeartbeatResponse = rmq::HeartbeatResponse;
using EndTransactionRequest = rmq::EndTransactionRequest;
using EndTransactionResponse = rmq::EndTransactionResponse;
using RecallMessageRequest = rmq::RecallMessageRequest;
using RecallMessageResponse = rmq::RecallMessageResponse;
using RecoverOrphanedTransactionCommand = rmq::RecoverOrphanedTransactionCommand;
using PrintThreadStackTraceCommand = rmq::PrintThreadStackTraceCommand;
using ThreadStackTrace = rmq::ThreadStackTrace;
using VerifyMessageCommand = rmq::VerifyMessageCommand;
using VerifyMessageResult = rmq::VerifyMessageResult;
using TelemetryCommand = rmq::TelemetryCommand;
using ForwardMessageToDeadLetterQueueRequest = rmq::ForwardMessageToDeadLetterQueueRequest;
using ForwardMessageToDeadLetterQueueResponse = rmq::ForwardMessageToDeadLetterQueueResponse;
using NotifyClientTerminationRequest = rmq::NotifyClientTerminationRequest;
using NotifyClientTerminationResponse = rmq::NotifyClientTerminationResponse;

const char* protocolVersion();

bool writable(rmq::Permission p);

bool readable(rmq::Permission p);

bool operator<(const rmq::Resource& lhs, const rmq::Resource& rhs);

bool operator==(const rmq::Resource& lhs, const rmq::Resource& rhs);

bool operator<(const rmq::Broker& lhs, const rmq::Broker& rhs);

bool operator==(const rmq::Broker& lhs, const rmq::Broker& rhs);

bool operator<(const rmq::MessageQueue& lhs, const rmq::MessageQueue& rhs);

bool operator==(const rmq::MessageQueue& lhs, const rmq::MessageQueue& rhs);

std::string simpleNameOf(const rmq::MessageQueue& m);

bool operator==(const std::vector<rmq::MessageQueue>& lhs, const std::vector<rmq::MessageQueue>& rhs);

bool operator!=(const std::vector<rmq::MessageQueue>& lhs, const std::vector<rmq::MessageQueue>& rhs);

std::string urlOf(const rmq::MessageQueue& message_queue);

bool operator<(const rmq::Assignment& lhs, const rmq::Assignment& rhs);

bool operator==(const rmq::Assignment& lhs, const rmq::Assignment& rhs);

bool operator==(const std::vector<rmq::Assignment>& lhs, const std::vector<rmq::Assignment>& rhs);

ROCKETMQ_NAMESPACE_END