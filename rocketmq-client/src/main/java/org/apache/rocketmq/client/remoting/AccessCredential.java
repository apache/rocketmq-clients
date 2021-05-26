package org.apache.rocketmq.client.remoting;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class AccessCredential {
    private final String accessKey;
    private final String accessSecret;
}
