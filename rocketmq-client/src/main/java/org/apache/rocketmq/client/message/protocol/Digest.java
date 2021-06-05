package org.apache.rocketmq.client.message.protocol;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Digest {
    private final DigestType digestType;
    private final String digest;
}
