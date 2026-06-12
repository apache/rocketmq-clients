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

class MessageHookPoints
{
    const SEND = 'SEND';
    const RECEIVE = 'RECEIVE';
    const CONSUME = 'CONSUME';
    const ACK = 'ACK';
    const CHANGE_INVISIBLE_DURATION = 'CHANGE_INVISIBLE_DURATION';
    const COMMIT_TRANSACTION = 'COMMIT_TRANSACTION';
    const ROLLBACK_TRANSACTION = 'ROLLBACK_TRANSACTION';
    const FORWARD_TO_DLQ = 'FORWARD_TO_DLQ';
}
