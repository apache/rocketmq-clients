<?php
/**
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

namespace Apache\Rocketmq;

class ClientConstants
{
    const CLIENT_VERSION = '5.0.0';
    const LANGUAGE = 'PHP';
    const LANGUAGE_PROTO = Language::PHP;
    const CLIENT_TYPE_PREFIX = 'php';
    const MASTER_BROKER_ID = 0;

    // Default gRPC timeout (30 seconds)
    const GRPC_DEFAULT_TIMEOUT = 30_000_000; // microseconds
    
    // Operation-specific timeouts (microseconds)
    const GRPC_SEND_MESSAGE_TIMEOUT = 10_000_000;      // 10s - Send message
    const GRPC_RECEIVE_MESSAGE_TIMEOUT = 60_000_000;   // 60s - Receive with long polling
    const GRPC_ACK_MESSAGE_TIMEOUT = 5_000_000;        // 5s - Acknowledge message
    const GRPC_HEARTBEAT_TIMEOUT = 5_000_000;          // 5s - Heartbeat
    const GRPC_QUERY_ROUTE_TIMEOUT = 10_000_000;       // 10s - Query route
    const GRPC_QUERY_ASSIGNMENT_TIMEOUT = 10_000_000;  // 10s - Query assignment
    const GRPC_END_TRANSACTION_TIMEOUT = 10_000_000;   // 10s - End transaction
    const GRPC_CHANGE_INVISIBLE_TIMEOUT = 5_000_000;   // 5s - Change invisible duration
    const GRPC_FORWARD_DLQ_TIMEOUT = 5_000_000;        // 5s - Forward to DLQ
    const GRPC_RECALL_MESSAGE_TIMEOUT = 10_000_000;    // 10s - Recall message
    const GRPC_SYNC_LITE_MESSAGE_TIMEOUT = 10_000_000;    // 10s - Sync lite subscript
}
