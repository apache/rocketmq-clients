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

#include <cassert>

#include "apache/rocketmq/v2/admin.grpc.pb.h"

#include "rocketmq/RocketMQ.h"

namespace rmq = apache::rocketmq::v2;

ROCKETMQ_NAMESPACE_BEGIN

enum ServerCallStatus : int8_t
{
  CREATE = 0,
  PROCESS = 1,
  FINISH = 2
};

class ServerCall {
public:
  ServerCall(rmq::Admin::AsyncService* async_service, rmq::Admin::Service* service,
             grpc::ServerCompletionQueue* completion_queue)
      : async_stub_(async_service), service_(service), completion_queue_(completion_queue),
        response_observer_(&context_), status_(ServerCallStatus::CREATE) {
    proceed();
  }

  void proceed() {
    switch (status_) {
      case CREATE: {
        status_ = PROCESS;
        async_stub_->RequestChangeLogLevel(&context_, &request_, &response_observer_, completion_queue_,
                                           completion_queue_, this);
        break;
      }
      case PROCESS: {
        // Create a new ServerCall to serve the next incoming request.
        new ServerCall(async_stub_, service_, completion_queue_);

        // Now that request_ is already filled with actual data from clients, invoke the actual process function
        const grpc::Status rpc_status = service_->ChangeLogLevel(&context_, &request_, &response_);

        status_ = FINISH;
        response_observer_.Finish(response_, rpc_status, this);
        break;
      }
      default: {
        assert(FINISH == status_);
        delete this;
      }
    }
  }

private:
  rmq::Admin::AsyncService* async_stub_;
  rmq::Admin::Service* service_;
  grpc::ServerCompletionQueue* completion_queue_;
  grpc::ServerContext context_;
  rmq::ChangeLogLevelRequest request_;
  rmq::ChangeLogLevelResponse response_;
  grpc::ServerAsyncResponseWriter<rmq::ChangeLogLevelResponse> response_observer_;
  ServerCallStatus status_;
};

ROCKETMQ_NAMESPACE_END