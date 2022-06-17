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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.net.HttpURLConnection;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.utility.HttpTinyClient;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StsCredentialsProviderTest {
    @Mock
    private HttpTinyClient httpTinyClient;

    private final String successJson = "{\"AccessKeyId\" : \"FakeAccessKey\",\"AccessKeySecret\" : \"FakeKeySecret\","
                                       + "\"Expiration\" : \"2017-11-01T05:20:01Z\",\"SecurityToken\" : "
                                       + "\"FakeSecurityToken\",\"LastUpdated\" : \"2017-10-31T23:20:01Z\",\"Code\" :"
                                       + " \"Success\"}";

    @InjectMocks
    private StsCredentialsProvider provider;

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        this.provider = new StsCredentialsProvider("", httpTinyClient);
    }

    @Test
    public void testStsCredentialsDeserialization() {
        Gson gson = new GsonBuilder().create();
        final StsCredentialsProvider.StsCredentials credentials =
                gson.fromJson(successJson, StsCredentialsProvider.StsCredentials.class);
        assertEquals(credentials.getAccessKeyId(), "FakeAccessKey");
        assertEquals(credentials.getAccessKeySecret(), "FakeKeySecret");
        assertEquals(credentials.getExpiration(), "2017-11-01T05:20:01Z");
        assertEquals(credentials.getSecurityToken(), "FakeSecurityToken");
        assertEquals(credentials.getLastUpdated(), "2017-10-31T23:20:01Z");
        assertEquals(credentials.getLastUpdated(), "2017-10-31T23:20:01Z");
        assertEquals(credentials.getCode(), "Success");
    }

    @Test
    public void testGetCredentials() throws ClientException {
        final HttpTinyClient.HttpResult httpResult = new HttpTinyClient.HttpResult(HttpURLConnection.HTTP_OK,
                                                                                   successJson);
        SettableFuture<HttpTinyClient.HttpResult> future0 = SettableFuture.create();
        future0.set(httpResult);
        when(httpTinyClient.httpGet(anyString(), anyInt(), ArgumentMatchers.<ExecutorService>any()))
                .thenReturn(future0);
        final Credentials credentials = provider.getCredentials();
        assertEquals(credentials.getAccessKey(), "FakeAccessKey");
        assertEquals(credentials.getAccessSecret(), "FakeKeySecret");
        assertEquals(credentials.getSecurityToken(), "FakeSecurityToken");
        assertTrue(credentials.expiredSoon());
        provider.getCredentials();
        verify(httpTinyClient, times(2)).httpGet(anyString(), anyInt(), ArgumentMatchers.<ExecutorService>any());
    }

    @Test(expectedExceptions = ClientException.class)
    public void testGetCredentialsWithException() throws ClientException {
        String illegalDataFormatJson = "{\"AccessKeyId\" : \"FakeAccessKey\",\"AccessKeySecret\" : "
                                       + "\"FakeKeySecret\",\"Expiration\" : \"illegal-format\",\"SecurityToken\" : "
                                       + "\"FakeSecurityToken\",\"LastUpdated\" : \"2017-10-31T23:20:01Z\",\"Code\" :"
                                       + " \"Failure\"}";
        final HttpTinyClient.HttpResult httpResult =
                new HttpTinyClient.HttpResult(HttpURLConnection.HTTP_OK, illegalDataFormatJson);
        SettableFuture<HttpTinyClient.HttpResult> future0 = SettableFuture.create();
        future0.set(httpResult);
        when(httpTinyClient.httpGet(anyString(), anyInt(), ArgumentMatchers.<ExecutorService>any()))
                .thenReturn(future0);
        provider.getCredentials();
        fail();
    }

    @Test(expectedExceptions = ClientException.class)
    public void testGetCredentialsWithErrorHttpCode() throws ClientException {
        final HttpTinyClient.HttpResult httpResult =
                new HttpTinyClient.HttpResult(HttpURLConnection.HTTP_INTERNAL_ERROR, successJson);
        SettableFuture<HttpTinyClient.HttpResult> future0 = SettableFuture.create();
        future0.set(httpResult);
        when(httpTinyClient.httpGet(anyString(), anyInt(), ArgumentMatchers.<ExecutorService>any()))
                .thenReturn(future0);
        provider.getCredentials();
        fail();
    }

    @Test(expectedExceptions = ClientException.class)
    public void testGetCredentialsWithStsErrorCode() throws ClientException {
        String failureJson = "{\"AccessKeyId\" : \"FakeAccessKey\",\"AccessKeySecret\" : \"FakeKeySecret\","
                             + "\"Expiration\" : \"2017-11-01T05:20:01Z\",\"SecurityToken\" : "
                             + "\"FakeSecurityToken\",\"LastUpdated\" : \"2017-10-31T23:20:01Z\",\"Code\" :"
                             + " \"Failure\"}";
        final HttpTinyClient.HttpResult httpResult =
                new HttpTinyClient.HttpResult(HttpURLConnection.HTTP_OK, failureJson);
        SettableFuture<HttpTinyClient.HttpResult> future0 = SettableFuture.create();
        future0.set(httpResult);
        when(httpTinyClient.httpGet(anyString(), anyInt(), ArgumentMatchers.<ExecutorService>any()))
                .thenReturn(future0);
        provider.getCredentials();
        fail();
    }
}