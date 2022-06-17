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

package org.apache.rocketmq.client.remoting;

import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class CredentialsTest {

    @Test
    public void testConstructor() {
        String fakeAccessKey = "accessKey";
        String fakeSecretKey = "secretKey";
        String fakeSecurityToken = "securityToken";
        {
            try {
                new Credentials(fakeAccessKey, null);
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(null, fakeSecretKey);
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(fakeAccessKey, fakeSecretKey, null);
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(fakeAccessKey, null, fakeSecurityToken);
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(null, fakeSecretKey, fakeSecurityToken, System.currentTimeMillis());
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(fakeAccessKey, fakeSecretKey, null, System.currentTimeMillis());
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(fakeAccessKey, null, fakeSecurityToken, System.currentTimeMillis());
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
        {
            try {
                new Credentials(null, fakeSecretKey, fakeSecurityToken, System.currentTimeMillis());
                fail();
            } catch (NullPointerException ignore) {
                // ignore on purpose.
            }
        }
    }
}