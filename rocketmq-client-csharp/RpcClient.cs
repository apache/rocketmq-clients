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

using System.Threading.Tasks;
using apache.rocketmq.v1;
using grpc = global::Grpc.Core;

namespace org.apache.rocketmq {
    public class RpcClient : IRpcClient {
        public RpcClient(MessagingService.MessagingServiceClient client) {
            stub = client;
        }

        public async Task<QueryRouteResponse> queryRoute(QueryRouteRequest request, grpc::CallOptions callOptions) {
            var call = stub.QueryRouteAsync(request, callOptions);
            var response = await call.ResponseAsync;
            var status = call.GetStatus();
            if (status.StatusCode != grpc.StatusCode.OK) {
                //TODO: Something is wrong, raise an exception here.
            }
            return response;
        }

        private MessagingService.MessagingServiceClient stub;
    }
}