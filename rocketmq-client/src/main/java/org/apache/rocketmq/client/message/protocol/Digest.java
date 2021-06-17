package org.apache.rocketmq.client.message.protocol;

import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class Digest {
    private final DigestType digestType;
    private final String checkSum;
}
