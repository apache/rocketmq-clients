package org.apache.rocketmq.client.remoting;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Interceptor for all gRPC request.
 *
 * <p>Interceptor is responsible for authorization and RPC tracing.</p>
 */
@Slf4j
public class HeadersClientInterceptor implements ClientInterceptor {

    private static final String TENANT_ID_KEY = "x-mq-tenant-id";
    private static final String ARN_KEY = "x-mq-arn";
    private static final String AUTHORIZATION = "authorization";
    private static final String DATE_TIME_KEY = "x-mq-date-time";

    private static final String REQUEST_ID_KEY = "x-mq-request-id";
    private static final String MQ_LANGUAGE = "x-mq-language";
    private static final String SDK_VERSION = "x-mq-sdk-version";
    private static final String SDK_PROTOCOL_VERSION = "x-mq-protocol-version";

    private static final String ALGORITHM_KEY = "MQv2-HMAC-SHA1";
    private static final String CREDENTIAL_KEY = "Credential";
    private static final String SIGNED_HEADERS_KEY = "SignedHeaders";
    private static final String SIGNATURE_KEY = "Signature";
    private static final String DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";

    private final CredentialsObservable credentialsObservable;

    public HeadersClientInterceptor(CredentialsObservable credentialsObservable) {
        this.credentialsObservable = credentialsObservable;
    }

    private void customMetadata(Metadata headers) {
        final String tenantId = credentialsObservable.getTenantId();
        if (StringUtils.isNotBlank(tenantId)) {
            headers.put(Metadata.Key.of(TENANT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), tenantId);
        }

        headers.put(Metadata.Key.of(MQ_LANGUAGE, Metadata.ASCII_STRING_MARSHALLER), "JAVA");
        headers.put(Metadata.Key.of(REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), "JAVA");

        final String arn = credentialsObservable.getArn();
        if (StringUtils.isNotBlank(arn)) {
            headers.put(Metadata.Key.of(ARN_KEY, Metadata.ASCII_STRING_MARSHALLER), arn);
        }
        String dateTime = new SimpleDateFormat(DATE_TIME_FORMAT).format(new Date());
        headers.put(Metadata.Key.of(DATE_TIME_KEY, Metadata.ASCII_STRING_MARSHALLER), dateTime);

        final AccessCredential accessCredential = credentialsObservable.getAccessCredential();
        if (null == accessCredential) {
            return;
        }

        final String accessKey = accessCredential.getAccessKey();
        final String accessSecret = accessCredential.getAccessSecret();

        if (StringUtils.isBlank(accessKey)) {
            return;
        }

        if (StringUtils.isBlank(accessSecret)) {
            return;
        }

        String regionId = credentialsObservable.getRegionId();
        String serviceName = credentialsObservable.getServiceName();

        String sign = null;
        try {
            sign = TlsHelper.sign(accessSecret, dateTime);
        } catch (Throwable t) {
            log.error("Failed to sign accessSecret", t);
        }

        if (null != sign) {
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

            headers.put(Metadata.Key.of(AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER), authorization);
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                customMetadata(headers);
                super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onHeaders(Metadata headers) {
                        super.onHeaders(headers);
                    }

                    @Override
                    public void onMessage(RespT response) {
                        log.trace("gRPC response: {}\n{}", response.getClass().getName(), response);
                        super.onMessage(response);
                    }
                }, headers);
            }

            @Override
            public void sendMessage(ReqT request) {
                log.trace("gRPC request: {}\n{}", request.getClass().getName(), request);
                super.sendMessage(request);
            }
        };
    }
}

