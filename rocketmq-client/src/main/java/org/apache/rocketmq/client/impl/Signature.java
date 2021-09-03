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

import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Credentials;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.client.remoting.TlsHelper;
import org.apache.rocketmq.utility.MetadataUtils;

public class Signature {
    public static final String TENANT_ID_KEY = "x-mq-tenant-id";
    public static final String ARN_KEY = "x-mq-arn";
    public static final String AUTHORIZATION = "authorization";
    public static final String DATE_TIME_KEY = "x-mq-date-time";

    public static final String SESSION_TOKEN = "x-mq-session-token";

    public static final String REQUEST_ID_KEY = "x-mq-request-id";
    public static final String MQ_LANGUAGE = "x-mq-language";
    public static final String SDK_VERSION = "x-mq-client-version";
    public static final String SDK_PROTOCOL_VERSION = "x-mq-protocol";

    public static final String ALGORITHM_KEY = "MQv2-HMAC-SHA1";
    public static final String CREDENTIAL_KEY = "Credential";
    public static final String SIGNED_HEADERS_KEY = "SignedHeaders";
    public static final String SIGNATURE_KEY = "Signature";
    public static final String DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";

    private Signature() {
    }

    public static Metadata sign(ClientConfig config)
            throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException, ClientException {

        Metadata metadata = new Metadata();
        final String tenantId = config.getTenantId();
        if (StringUtils.isNotBlank(tenantId)) {
            metadata.put(Metadata.Key.of(TENANT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), tenantId);
        }

        metadata.put(Metadata.Key.of(MQ_LANGUAGE, Metadata.ASCII_STRING_MARSHALLER), "JAVA");
        metadata.put(Metadata.Key.of(SDK_PROTOCOL_VERSION, Metadata.ASCII_STRING_MARSHALLER),
                     MixAll.getProtocolVersion());
        metadata.put(Metadata.Key.of(SDK_VERSION, Metadata.ASCII_STRING_MARSHALLER), MetadataUtils.getVersion());

        final String arn = config.getNamespace();
        if (StringUtils.isNotBlank(arn)) {
            metadata.put(Metadata.Key.of(ARN_KEY, Metadata.ASCII_STRING_MARSHALLER), arn);
        }

        String dateTime = new SimpleDateFormat(DATE_TIME_FORMAT).format(new Date());
        metadata.put(Metadata.Key.of(DATE_TIME_KEY, Metadata.ASCII_STRING_MARSHALLER), dateTime);

        final CredentialsProvider provider = config.getCredentialsProvider();
        if (null == provider) {
            return metadata;
        }
        final Credentials credentials = provider.getCredentials();
        if (null == credentials) {
            return metadata;
        }

        final String accessKey = credentials.getAccessKey();
        final String accessSecret = credentials.getAccessSecret();

        if (StringUtils.isBlank(accessKey)) {
            return metadata;
        }

        if (StringUtils.isBlank(accessSecret)) {
            return metadata;
        }

        final String securityToken = credentials.getSecurityToken();
        if (StringUtils.isNotBlank(securityToken)) {
            metadata.put(Metadata.Key.of(SESSION_TOKEN, Metadata.ASCII_STRING_MARSHALLER), securityToken);
        }

        final String regionId = config.getRegionId();
        final String serviceName = config.getServiceName();

        String sign = TlsHelper.sign(accessSecret, dateTime);

        final String authorization = ALGORITHM_KEY
                                     + " "
                                     + CREDENTIAL_KEY
                                     + "="
                                     + accessKey
                                     + "/"
                                     + regionId
                                     + "/"
                                     + serviceName
                                     + ", "
                                     + SIGNED_HEADERS_KEY
                                     + "="
                                     + DATE_TIME_KEY
                                     + ", "
                                     + SIGNATURE_KEY
                                     + "="
                                     + sign;

        metadata.put(Metadata.Key.of(AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER), authorization);
        return metadata;
    }
}
