package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Credentials {
    private final String accessKey;
    private final String accessSecret;
    private final String sessionToken;

    public Credentials(String accessKey, String accessSecret) {
        Preconditions.checkNotNull(accessKey, "AccessKey is null, please set it.");
        Preconditions.checkNotNull(accessSecret, "SecretKey is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.sessionToken = null;
    }

    public Credentials(String accessKey, String accessSecret, String sessionToken) {
        Preconditions.checkNotNull(accessKey, "AccessKey is null, please set it.");
        Preconditions.checkNotNull(accessSecret, "SecretKey is null, please set it.");
        Preconditions.checkNotNull(sessionToken, "SessionToken is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.sessionToken = sessionToken;
    }
}
