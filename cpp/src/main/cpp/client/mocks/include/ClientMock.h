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

#include "Client.h"
#include "RpcClient.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientMock : virtual public Client {
public:
  MOCK_METHOD(void, endpointsInUse, (absl::flat_hash_set<std::string>&), (override));

  MOCK_METHOD(void, heartbeat, (), (override));

  MOCK_METHOD(bool, active, (), (override));

  MOCK_METHOD(void, onRemoteEndpointRemoval, (const std::vector<std::string>&), (override));

  MOCK_METHOD(void, schedule, (const std::string&, const std::function<void()>&, std::chrono::milliseconds),
              (override));

  MOCK_METHOD(void, createSession, (const std::string&), (override));

  MOCK_METHOD(void, notifyClientTermination, (), (override));

  MOCK_METHOD(void, verify, (VerifyMessageCommand, (std::function<void(TelemetryCommand)>)), (override));

  MOCK_METHOD(void, recoverOrphanedTransaction, (const RecoverOrphanedTransactionCommand&), (override));
};

ROCKETMQ_NAMESPACE_END
