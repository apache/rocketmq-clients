package org.apache.rocketmq.client.message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class MessageBatch extends Message implements Iterable<Message> {
    private final List<Message> messages;

    public MessageBatch(MessageImpl impl) {
        super(impl);
        this.messages = new ArrayList<Message>();
    }

    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }
}
