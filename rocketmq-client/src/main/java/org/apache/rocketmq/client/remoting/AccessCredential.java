package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class AccessCredential {
    private final String accessKey;
    private final String accessSecret;

    public AccessCredential(String accessKey, String accessSecret) {
        Preconditions.checkNotNull(accessKey, "AccessKey is null, please set it.");
        this.accessKey = accessKey;
        Preconditions.checkNotNull(accessSecret, "SecretKey is null, please set it.");
        this.accessSecret = accessSecret;
    }
}
