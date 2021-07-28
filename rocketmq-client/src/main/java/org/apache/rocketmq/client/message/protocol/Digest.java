package org.apache.rocketmq.client.message.protocol;

import javax.annotation.concurrent.Immutable;
import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
@Immutable
public class Digest {
    private final DigestType digestType;
    private final String checkSum;
}
