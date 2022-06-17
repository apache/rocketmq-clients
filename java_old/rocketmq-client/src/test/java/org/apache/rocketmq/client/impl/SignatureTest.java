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

import static org.testng.Assert.assertTrue;

import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.remoting.StaticCredentialsProvider;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class SignatureTest extends TestBase {

    @Test
    public void testSignWithStaticCredentialsProvider() throws ClientException, UnsupportedEncodingException,
                                                               NoSuchAlgorithmException, InvalidKeyException {
        {
            final ClientConfig clientConfig = new ClientConfig(FAKE_GROUP_0);
            clientConfig.setCredentialsProvider(new StaticCredentialsProvider(FAKE_ACCESS_KEY, FAKE_SECRET_KEY));
            final Metadata metadata = Signature.sign(clientConfig);
            final Set<String> signKeys = metadata.keys();
            assertTrue(signKeys.contains(Signature.AUTHORIZATION_KEY));
            assertTrue(signKeys.contains(Signature.CLIENT_VERSION_KEY));
            assertTrue(signKeys.contains(Signature.LANGUAGE_KEY));
            assertTrue(signKeys.contains(Signature.PROTOCOL_VERSION));
            assertTrue(signKeys.contains(Signature.DATE_TIME_KEY));
            assertTrue(signKeys.contains(Signature.REQUEST_ID_KEY));
        }
        {
            final ClientConfig clientConfig = new ClientConfig(FAKE_GROUP_0);
            clientConfig.setCredentialsProvider(new StaticCredentialsProvider(FAKE_ACCESS_KEY, FAKE_SECRET_KEY,
                                                                              FAKE_SECURITY_TOKEN));
            final Metadata metadata = Signature.sign(clientConfig);
            final Set<String> signKeys = metadata.keys();
            assertTrue(signKeys.contains(Signature.AUTHORIZATION_KEY));
            assertTrue(signKeys.contains(Signature.SESSION_TOKEN_KEY));
            assertTrue(signKeys.contains(Signature.CLIENT_VERSION_KEY));
            assertTrue(signKeys.contains(Signature.LANGUAGE_KEY));
            assertTrue(signKeys.contains(Signature.PROTOCOL_VERSION));
            assertTrue(signKeys.contains(Signature.DATE_TIME_KEY));
            assertTrue(signKeys.contains(Signature.REQUEST_ID_KEY));
        }
    }
}
