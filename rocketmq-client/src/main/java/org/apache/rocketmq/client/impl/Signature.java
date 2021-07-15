package org.apache.rocketmq.client.impl;

import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.remoting.AccessCredential;
import org.apache.rocketmq.client.remoting.CredentialsObservable;
import org.apache.rocketmq.client.remoting.TlsHelper;

public class Signature {
    public static final String TENANT_ID_KEY = "x-mq-tenant-id";
    public static final String ARN_KEY = "x-mq-arn";
    public static final String AUTHORIZATION = "authorization";
    public static final String DATE_TIME_KEY = "x-mq-date-time";

    public static final String REQUEST_ID_KEY = "x-mq-request-id";
    public static final String MQ_LANGUAGE = "x-mq-language";
    public static final String SDK_VERSION = "x-mq-sdk-version";
    public static final String SDK_PROTOCOL_VERSION = "x-mq-protocol-version";

    public static final String ALGORITHM_KEY = "MQv2-HMAC-SHA1";
    public static final String CREDENTIAL_KEY = "Credential";
    public static final String SIGNED_HEADERS_KEY = "SignedHeaders";
    public static final String SIGNATURE_KEY = "Signature";
    public static final String DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";

    private Signature() {
    }

    public static Metadata sign(CredentialsObservable observable)
            throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {

        Metadata metadata = new Metadata();
        final String tenantId = observable.getTenantId();
        if (StringUtils.isNotBlank(tenantId)) {
            metadata.put(Metadata.Key.of(TENANT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), tenantId);
        }

        metadata.put(Metadata.Key.of(MQ_LANGUAGE, Metadata.ASCII_STRING_MARSHALLER), "JAVA");
        // TODO
        metadata.put(Metadata.Key.of(REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), "JAVA");

        final String arn = observable.getArn();
        if (StringUtils.isNotBlank(arn)) {
            metadata.put(Metadata.Key.of(ARN_KEY, Metadata.ASCII_STRING_MARSHALLER), arn);
        }

        String dateTime = new SimpleDateFormat(DATE_TIME_FORMAT).format(new Date());
        metadata.put(Metadata.Key.of(DATE_TIME_KEY, Metadata.ASCII_STRING_MARSHALLER), dateTime);

        final AccessCredential accessCredential = observable.getAccessCredential();
        if (null == accessCredential) {
            return metadata;
        }

        final String accessKey = accessCredential.getAccessKey();
        final String accessSecret = accessCredential.getAccessSecret();

        if (StringUtils.isBlank(accessKey)) {
            return metadata;
        }

        if (StringUtils.isBlank(accessSecret)) {
            return metadata;
        }

        final String regionId = observable.getRegionId();
        final String serviceName = observable.getServiceName();

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
