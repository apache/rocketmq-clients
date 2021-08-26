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

package org.apache.rocketmq.client.tracing;

public class TracingAttribute {
    public static final String ARN = "arn";
    public static final String ACCESS_KEY = "ak";
    public static final String TOPIC = "topic";
    public static final String GROUP = "group";
    public static final String MSG_ID = "msgId";
    public static final String TAG = "tag";
    public static final String DELIVERY_TIMESTAMP = "deliveryTimestamp";
    public static final String AVAILABLE_TIMESTAMP = "availableTimestamp";
    public static final String COMMIT_ACTION = "commitAction";
    public static final String HOST = "host";
    public static final String KEYS = "keys";
    public static final String ATTEMPT = "attempt";
    public static final String MSG_TYPE = "msgType";
    public static final String BATCH_SIZE = "batchSize";
    public static final String CLIENT_ID = "clientId";

    private TracingAttribute() {
    }
}
