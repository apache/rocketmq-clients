package org.apache.rocketmq.client.message;

import java.util.concurrent.ConcurrentMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;

@Data
@AllArgsConstructor
public class MessageImpl {
    private String topic;
    private final SystemAttribute systemAttribute;
    private final ConcurrentMap<String, String> userAttribute;
    private byte[] body;

    public void setBody(byte[] body) {
        if (null == body) {
            this.body = null;
            return;
        }
        this.body = body.clone();
    }

    public byte[] getBody() {
        return null == body ? null : body.clone();
    }
}
