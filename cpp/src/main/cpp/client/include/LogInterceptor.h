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
#include "grpcpp/impl/codegen/client_interceptor.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class LogInterceptor : public grpc::experimental::Interceptor {
public:
  explicit LogInterceptor(grpc::experimental::ClientRpcInfo* client_rpc_info) : client_rpc_info_(client_rpc_info) {
  }

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

private:
  grpc::experimental::ClientRpcInfo* client_rpc_info_;
};

ROCKETMQ_NAMESPACE_END