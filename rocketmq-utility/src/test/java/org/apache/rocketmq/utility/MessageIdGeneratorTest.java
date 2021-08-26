package org.apache.rocketmq.utility;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class MessageIdGeneratorTest {

    @Test
    public void testNext() {
        final String messageId = MessageIdGenerator.getInstance().next();
        assertEquals(messageId.length(), 34);
    }
}
