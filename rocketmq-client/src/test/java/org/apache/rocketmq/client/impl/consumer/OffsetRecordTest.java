package org.apache.rocketmq.client.impl.consumer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OffsetRecordTest {

    @Test
    public void testCompareOffsetRecord() {
        OffsetRecord smaller = new OffsetRecord(1);
        OffsetRecord bigger = new OffsetRecord(2);
        Assert.assertTrue(smaller.compareTo(bigger) < 0);
    }

}