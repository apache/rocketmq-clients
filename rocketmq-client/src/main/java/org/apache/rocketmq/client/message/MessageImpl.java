package org.apache.rocketmq.client.message;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;

@Data
public class MessageImpl {
    private String topic;
    private final SystemAttribute systemAttribute;
    private final Map<String, String> userAttribute;

    private byte[] body;

    public MessageImpl(String topic) {
        this.topic = topic;
        this.systemAttribute = new SystemAttribute();
        this.userAttribute = new HashMap<String, String>();
    }

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
