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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.utility.HttpTinyClient;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StsCredentialsProvider implements CredentialsProvider {
    private static final Logger log = LoggerFactory.getLogger(StsCredentialsProvider.class);

    private static final int HTTP_TIMEOUT_MILLIS = 1000;
    private static final String RAM_ROLE_HOST = "100.100.100.200";
    private static final String RAM_ROLE_URL_PREFIX = "/latest/meta-data/Ram/security-credentials/";
    private final HttpTinyClient httpTinyClient;
    private Credentials credentials;
    private final String ramRole;
    private final ExecutorService stsRefresher;

    /**
     * Constructor for sts authorization.
     *
     * @param ramRole ram role name.
     */
    public StsCredentialsProvider(String ramRole) {
        this(ramRole, HttpTinyClient.getInstance());
    }

    /**
     * Constructor for injecting of mock {@link HttpTinyClient}.
     *
     * @param ramRole    ram role name.
     * @param tinyClient http tiny client.
     */
    StsCredentialsProvider(String ramRole, HttpTinyClient tinyClient) {
        this.httpTinyClient = tinyClient;
        this.ramRole = ramRole;
        this.credentials = null;
        this.stsRefresher = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("StsRefresher"));
    }

    @Override
    public Credentials getCredentials() throws ClientException {
        if (null == credentials) {
            final ListenableFuture<Credentials> future = refresh();
            try {
                return future.get();
            } catch (Throwable t) {
                throw new ClientException(ErrorCode.STS_TOKEN_GET_FAILURE, t);
            }
        }
        if (credentials.expiredSoon()) {
            refresh();
        }
        return credentials;
    }

    static class StsCredentials {
        public static String SUCCESS_CODE = "Success";

        @SerializedName("AccessKeyId")
        private final String accessKeyId;
        @SerializedName("AccessKeySecret")
        private final String accessKeySecret;
        @SerializedName("Expiration")
        private final String expiration;
        @SerializedName("SecurityToken")
        private final String securityToken;
        @SerializedName("LastUpdated")
        private final String lastUpdated;
        @SerializedName("Code")
        private final String code;

        public StsCredentials(String accessKeyId, String accessKeySecret, String expiration, String securityToken,
                              String lastUpdated, String code) {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
            this.expiration = expiration;
            this.securityToken = securityToken;
            this.lastUpdated = lastUpdated;
            this.code = code;
        }

        public String getAccessKeyId() {
            return this.accessKeyId;
        }

        public String getAccessKeySecret() {
            return this.accessKeySecret;
        }

        public String getExpiration() {
            return this.expiration;
        }

        public String getSecurityToken() {
            return this.securityToken;
        }

        public String getLastUpdated() {
            return this.lastUpdated;
        }

        public String getCode() {
            return this.code;
        }
    }

    private ListenableFuture<Credentials> refresh() {
        String url = MixAll.HTTP_PREFIX + RAM_ROLE_HOST + RAM_ROLE_URL_PREFIX + ramRole;
        final ListenableFuture<HttpTinyClient.HttpResult> future = httpTinyClient.httpGet(url, HTTP_TIMEOUT_MILLIS,
                                                                                          stsRefresher);
        return Futures.transform(future, new Function<HttpTinyClient.HttpResult, Credentials>() {
            @Override
            public Credentials apply(HttpTinyClient.HttpResult httpResult) {
                try {
                    if (httpResult.isOk()) {
                        final String content = httpResult.getContent();
                        Gson gson = new GsonBuilder().create();
                        final StsCredentials stsCredentials = gson.fromJson(content, StsCredentials.class);
                        final String expiration = stsCredentials.getExpiration();
                        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                        long expiredTimeMillis = dateFormat.parse(expiration).getTime();
                        final String code = stsCredentials.getCode();
                        if (StsCredentials.SUCCESS_CODE.equals(code)) {
                            credentials = new Credentials(stsCredentials.getAccessKeyId(),
                                                          stsCredentials.getAccessKeySecret(),
                                                          stsCredentials.getSecurityToken(), expiredTimeMillis);
                            return credentials;

                        }
                        log.error("Failed to fetch sts token, ramRole={}, code={}", ramRole, code);
                    } else {
                        log.error("Failed to fetch sts token, ramRole={}, httpCode={}", ramRole, httpResult.getCode());
                    }
                } catch (Throwable t) {
                    log.error("Exception raise while refreshing sts token, ramRole={}", ramRole, t);
                    throw new RuntimeException(t);
                }
                throw new RuntimeException("Failed to fetch sts token");
            }
        });
    }
}
