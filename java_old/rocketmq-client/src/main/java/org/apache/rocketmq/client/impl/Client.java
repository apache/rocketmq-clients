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

package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.route.Endpoints;

public abstract class Client extends ClientConfig {
    /**
     * Constructor by specifying group.
     *
     * @param group client group.
     * @throws ClientException if there is any client error.
     */
    public Client(String group) throws ClientException {
        super(group);
    }

    /**
     * Send heart beat to remote {@link Endpoints}.
     */
    public abstract void doHeartbeat();

    /**
     * Check the status of remote {@link Endpoints}.
     */
    public abstract void doHealthCheck();

    /**
     * Do some stats for client.
     */
    public abstract void doStats();
}
