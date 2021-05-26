package org.apache.rocketmq.utility;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilAllTest {
    private static byte[] convertIPToBytes(String ip) {
        final String[] split = ip.split("\\.");
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            bytes[i] = (byte) Integer.parseInt(split[i]);
        }
        return bytes;
    }

    @Test
    public void testGetProcessId() {
    }

    @Test
    public void testBytes2string() {
    }

    @Test
    public void testGetIP() {
        final byte[] bytes = UtilAll.getIpv4AddressBytes();
        Assert.assertTrue(UtilAll.ipClassCheck(bytes));
    }

    @Test
    public void testIpClassCheck() {
        Assert.assertFalse(UtilAll.ipClassCheck(convertIPToBytes("127.0.0.1")));
        Assert.assertTrue(UtilAll.ipClassCheck(convertIPToBytes("1.0.0.1")));
    }

}
