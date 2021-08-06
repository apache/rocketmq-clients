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
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@Immutable
public class Credentials {
    private final String accessKey;
    private final String accessSecret;
    private final String securityToken;
    @Getter(AccessLevel.NONE)
    private final long expiredTimeMillis;

    public Credentials(String accessKey, String accessSecret) {
        checkNotNull(accessKey, "AccessKey is null, please set it.");
        checkNotNull(accessSecret, "SecretKey is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.securityToken = null;
        this.expiredTimeMillis = Long.MAX_VALUE;
    }

    public Credentials(String accessKey, String accessSecret, String securityToken, long expiredTimeMillis) {
        checkNotNull(accessKey, "AccessKey is null, please set it.");
        checkNotNull(accessSecret, "SecretKey is null, please set it.");
        checkNotNull(securityToken, "SessionToken is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.securityToken = securityToken;
        this.expiredTimeMillis = expiredTimeMillis;
    }

    public boolean expired() {
        return System.currentTimeMillis() > expiredTimeMillis;
    }
}
