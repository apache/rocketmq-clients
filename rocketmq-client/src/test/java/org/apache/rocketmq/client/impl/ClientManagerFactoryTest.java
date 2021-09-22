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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientManagerFactoryTest extends TestBase {
    private final String clientManagerId = "managerId";

    private final String clientId0 = "0";
    private final String clientId1 = "1";

    private final Client client0;
    private final Client client1;

    public ClientManagerFactoryTest() throws ClientException {
        this.client0 = new Client(FAKE_GROUP_0) {
            @Override
            public String id() {
                return clientId0;
            }

            @Override
            public String getId() {
                return "client0";
            }

            @Override
            public void doHeartbeat() {
            }

            @Override
            public void doHealthCheck() {
            }

            @Override
            public void doStats() {
            }
        };
        this.client1 = new Client(FAKE_GROUP_1) {
            @Override
            public String id() {
                return clientId1;
            }

            @Override
            public String getId() {
                return "client1";
            }

            @Override
            public void doHeartbeat() {
            }

            @Override
            public void doHealthCheck() {
            }

            @Override
            public void doStats() {
            }
        };
    }

    @Test
    public void testUnregisterClient() throws InterruptedException, ClientException {
        ClientManagerFactory.getInstance().registerClient(clientManagerId, client0);
        ClientManagerFactory.getInstance().registerClient(clientManagerId, client1);
        assertFalse(ClientManagerFactory.getInstance().unregisterClient(clientManagerId, client0));
        assertTrue(ClientManagerFactory.getInstance().unregisterClient(clientManagerId, client1));
    }

    @Test(invocationCount = 6)
    public void testUnregisterClientConcurrently() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                2,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        final List<Boolean> clientManagerRemoved = new ArrayList<Boolean>();
        final CountDownLatch latch = new CountDownLatch(2);
        final Runnable task0 = new Runnable() {
            @Override
            public void run() {
                try {
                    ClientManagerFactory.getInstance().registerClient(clientManagerId, client0);
                    Thread.sleep(5);
                    final boolean removed = ClientManagerFactory.getInstance().unregisterClient(clientManagerId,
                                                                                                client0);
                    clientManagerRemoved.add(removed);
                    latch.countDown();
                } catch (Throwable ignore) {
                    // Ignore on purpose.
                }
            }
        };

        final Runnable task1 = new Runnable() {
            @Override
            public void run() {
                try {
                    ClientManagerFactory.getInstance().registerClient(clientManagerId, client1);
                    Thread.sleep(5);
                    final boolean removed = ClientManagerFactory.getInstance().unregisterClient(clientManagerId,
                                                                                                client1);
                    clientManagerRemoved.add(removed);
                    latch.countDown();
                } catch (Throwable ignore) {
                    // Ignore on purpose.
                }
            }
        };

        final Random random = new Random();
        if (random.nextInt() % 2 == 0) {
            executor.submit(task0);
            executor.submit(task1);
        } else {
            executor.submit(task1);
            executor.submit(task0);
        }
        final boolean await = latch.await(3, TimeUnit.SECONDS);
        if (!await) {
            Assert.fail("Timeout to wait shutdown of client manager.");
        }
        boolean removed = false;
        for (Boolean b : clientManagerRemoved) {
            removed = removed || b;
        }
        assertTrue(removed);
    }
}