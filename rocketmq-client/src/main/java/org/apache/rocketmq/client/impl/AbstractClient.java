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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.rocketmq.client.exception.ClientException;

@SuppressWarnings("UnstableApiUsage")
public abstract class AbstractClient extends ClientConfig {
    private final ServiceImpl impl;

    public AbstractClient(String group) throws ClientException {
        super(group);
        this.impl = new ServiceImpl();
    }

    class ServiceImpl extends AbstractIdleService {
        @Override
        protected void startUp() throws Exception {
            AbstractClient.this.startUp();
        }

        @Override
        protected void shutDown() throws Exception {
            AbstractClient.this.shutDown();
        }
    }

    public abstract void startUp();

    public abstract void shutDown();

    public Service startAsync() {
        return impl.startAsync();
    }

    public void awaitRunning() {
        impl.awaitRunning();
    }

    public Service stopAsync() {
        return impl.stopAsync();
    }

    public void awaitTerminated() {
        impl.awaitTerminated();
    }

    public boolean isRunning() {
        return impl.isRunning();
    }
}