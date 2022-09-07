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

package org.apache.rocketmq.client.java.impl;

import io.grpc.Metadata;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;

public interface Client {
    /**
     * @return endpoints.
     */
    Endpoints getEndpoints();

    /**
     * Get the unique client identifier for each client.
     *
     * @return unique client identifier.
     */
    ClientId getClientId();

    /**
     * @return signature for tls
     */
    Metadata sign() throws Exception;

    /**
     * Send heart beat to remote {@link Endpoints}.
     */
    void doHeartbeat();

    /**
     * Sync settings to remote.
     */
    void syncSettings();

    /**
     * Do some stats for client.
     */
    void doStats();
}
