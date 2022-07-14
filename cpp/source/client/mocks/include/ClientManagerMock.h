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

#include <chrono>
#include <memory>
#include <system_error>

#include "gmock/gmock.h"

#include "ClientManager.h"
#include "RpcClient.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientManagerMock : public ClientManager {
public:
  MOCK_METHOD(void, start, (), (override));

  MOCK_METHOD(void, shutdown, (), (override));

  MOCK_METHOD(SchedulerSharedPtr, getScheduler, (), (override));

  MOCK_METHOD((std::shared_ptr<grpc::Channel>), createChannel, (const std::string&), (override));

  MOCK_METHOD(void, resolveRoute,
              (const std::string&, const Metadata&, const QueryRouteRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>&)),
              (override));

  MOCK_METHOD(void, heartbeat,
              (const std::string&, const Metadata&, const HeartbeatRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&, const HeartbeatResponse&)>&)),
              (override));

  MOCK_METHOD(std::shared_ptr<TelemetryBidiReactor>, telemetry, (const std::string&, std::weak_ptr<Client>),
              (override));

  MOCK_METHOD(bool, wrapMessage, (const rmq::Message&, MessageExt&), (override));

  MOCK_METHOD(void, ack,
              (const std::string&, const Metadata&, const AckMessageRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&)>&)),
              (override));

  MOCK_METHOD(void, changeInvisibleDuration,
              (const std::string&, const Metadata&, const ChangeInvisibleDurationRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&)>&)),
              (override));

  MOCK_METHOD(void, forwardMessageToDeadLetterQueue,
              (const std::string&, const Metadata&, const ForwardMessageToDeadLetterQueueRequest&,
               std::chrono::milliseconds,
               (const std::function<void(const InvocationContext<ForwardMessageToDeadLetterQueueResponse>*)>&)),
              (override));

  MOCK_METHOD(void, endTransaction,
              (const std::string&, const Metadata&, const EndTransactionRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&, const EndTransactionResponse&)>&)),
              (override));

  MOCK_METHOD(void, addClientObserver, (std::weak_ptr<Client>), (override));

  MOCK_METHOD(void, queryAssignment,
              (const std::string& target, const Metadata&, const QueryAssignmentRequest&, std::chrono::milliseconds,
               (const std::function<void(const std::error_code&, const QueryAssignmentResponse&)>&)),
              (override));

  MOCK_METHOD(void, receiveMessage,
              (const std::string&, const Metadata&, const ReceiveMessageRequest&, std::chrono::milliseconds,
               ReceiveMessageCallback),
              (override));

  MOCK_METHOD(bool, send, (const std::string&, const Metadata&, SendMessageRequest&, SendCallback), (override));

  MOCK_METHOD(std::error_code, notifyClientTermination,
              (const std::string&, const Metadata&, const NotifyClientTerminationRequest&, std::chrono::milliseconds),
              (override));

  MOCK_METHOD(State, state, (), (const override));

  MOCK_METHOD(void, submit, (std::function<void()>), (override));
};

ROCKETMQ_NAMESPACE_END