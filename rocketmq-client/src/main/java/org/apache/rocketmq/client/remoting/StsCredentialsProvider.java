package org.apache.rocketmq.client.remoting;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.utility.HttpTinyClient;

@Slf4j
public class StsCredentialsProvider implements CredentialsProvider {
    private static final int HTTP_TIMEOUT_MILLIS = 3 * 1000;
    private static final SimpleDateFormat UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final String RAM_ROLE_HOST = "100.100.100.200";
    private static final String RAM_ROLE_URL_PREFIX = "/latest/meta-data/Ram/security-credentials/";
    private Credentials credentials;
    private final String ramRole;

    public StsCredentialsProvider(String ramRole) {
        this.ramRole = ramRole;
        this.credentials = null;
    }

    @Override
    public Credentials getCredentials() throws ClientException {
        if (null == credentials || credentials.expired()) {
            refresh();
        }
        return credentials;
    }

    @AllArgsConstructor
    @Getter
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
    }

    private void refresh() throws ClientException {
        try {
            String url = MixAll.HTTP_PREFIX + RAM_ROLE_HOST + RAM_ROLE_URL_PREFIX + ramRole;
            final HttpTinyClient.HttpResult httpResult = HttpTinyClient.httpGet(url, HTTP_TIMEOUT_MILLIS);
            if (httpResult.isOk()) {
                final String content = httpResult.getContent();
                Gson gson = new Gson();
                final StsCredentials stsCredentials = gson.fromJson(content, StsCredentials.class);
                final String expiration = stsCredentials.getExpiration();
                UTC_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
                final long expiredTimeMillis = UTC_DATE_FORMAT.parse(expiration).getTime();

                final String code = stsCredentials.getCode();
                if (StsCredentials.SUCCESS_CODE.equals(code)) {
                    credentials = new Credentials(stsCredentials.getAccessKeyId(), stsCredentials.getAccessKeySecret(),
                                                  stsCredentials.getSecurityToken(), expiredTimeMillis);
                    return;
                }
                log.error("Failed to fetch sts token, ramRole={}, code={}", ramRole, code);
            } else {
                log.error("Failed to fetch sts token, ramRole={}, httpCode={}", ramRole, httpResult.getCode());
            }
        } catch (Throwable e) {
            log.error("Failed to fetch sts token, ramRole={}", ramRole, e);
            throw new ClientException(ErrorCode.STS_TOKEN_GET_FAILURE, e);
        }
        throw new ClientException(ErrorCode.STS_TOKEN_GET_FAILURE);
    }
}
