package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
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
        Preconditions.checkNotNull(accessKey, "AccessKey is null, please set it.");
        Preconditions.checkNotNull(accessSecret, "SecretKey is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.securityToken = null;
        this.expiredTimeMillis = Long.MAX_VALUE;
    }

    public Credentials(String accessKey, String accessSecret, String securityToken, long expiredTimeMillis) {
        Preconditions.checkNotNull(accessKey, "AccessKey is null, please set it.");
        Preconditions.checkNotNull(accessSecret, "SecretKey is null, please set it.");
        Preconditions.checkNotNull(securityToken, "SessionToken is null, please set it.");
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.securityToken = securityToken;
        this.expiredTimeMillis = expiredTimeMillis;
    }

    public boolean expired() {
        return System.currentTimeMillis() > expiredTimeMillis;
    }
}
