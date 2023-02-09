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
     * Retrieve Endpoints Information
     *
     * @return the endpoints associated with this client.
     */
    Endpoints getEndpoints();

    /**
     * Get Unique Client Identifier
     *
     * <p>Get the unique client identifier for each client.
     *
     * @return a unique client identifier.
     */
    ClientId getClientId();

    /**
     * Get TLS Signature
     *
     * @return the signature for TLS (Transport Layer Security).
     * @throws Exception if an error occurs during the signature generation process.
     */
    Metadata sign() throws Exception;

    /**
     * Check SSL Status
     *
     * <p>Check if SSL (Secure Sockets Layer) is enabled.
     *
     * @return a boolean value indicating whether SSL is enabled or not.
     */
    boolean isSslEnabled();

    /**
     * Send Heartbeat
     *
     * <p> Send a heartbeat to the remote endpoint.
     */
    void doHeartbeat();

    /**
     * Sync Settings
     *
     * <p>Synchronize client settings with the remote endpoint.
     */
    void syncSettings();

    /**
     * Do Statistics
     *
     * <p>Perform some statistics for the client.
     */
    void doStats();
}