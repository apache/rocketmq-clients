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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Credentials {
    private static final long TOLERANCE_MILLIS = 1000;

    private final String accessKey;
    private final String accessSecret;
    private final String securityToken;
    private final long expiredTimeMillis;

    public Credentials(String accessKey, String accessSecret) {
        this.accessKey = checkNotNull(accessKey, "accessKey");
        this.accessSecret = checkNotNull(accessSecret, "accessSecret");
        this.securityToken = null;
        this.expiredTimeMillis = Long.MAX_VALUE;
    }

    public Credentials(String accessKey, String accessSecret, String securityToken) {
        this.accessKey = checkNotNull(accessKey, "accessKey");
        this.accessSecret = checkNotNull(accessSecret, "accessSecret");
        this.securityToken = checkNotNull(securityToken, "securityToken");
        this.expiredTimeMillis = Long.MAX_VALUE;
    }

    public Credentials(String accessKey, String accessSecret, String securityToken, long expiredTimeMillis) {
        this.accessKey = checkNotNull(accessKey, "accessKey");
        this.accessSecret = checkNotNull(accessSecret, "accessSecret");
        this.securityToken = checkNotNull(securityToken, "securityToken");
        this.expiredTimeMillis = expiredTimeMillis;
    }

    /**
     * Indicates the sts token is expired soon or not.
     *
     * <p> If token is expired already, return true.
     *
     * @return is expired soon or not.
     */
    public boolean expiredSoon() {
        return System.currentTimeMillis() + TOLERANCE_MILLIS > expiredTimeMillis;
    }

    public String getAccessKey() {
        return this.accessKey;
    }

    public String getAccessSecret() {
        return this.accessSecret;
    }

    public String getSecurityToken() {
        return this.securityToken;
    }
}
