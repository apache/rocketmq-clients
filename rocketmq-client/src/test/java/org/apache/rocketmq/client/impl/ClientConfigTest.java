package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.conf.TestBase;
import org.testng.annotations.Test;

public class ClientConfigTest extends TestBase {
    private final ClientConfig clientConfig;

    public ClientConfigTest() {
        this.clientConfig = new ClientConfig(dummyConsumerGroup0);
    }

    @Test
    public void testSetNamesrvAddr() {
//        {
//            clientConfig.setNamesrvAddr(dummyNameServerAddr);
//            final List<Endpoints> namesrvAddrs = clientConfig.getNamesrvAddr();
//            Assert.assertEquals(namesrvAddrs.size(), 1);
//            Assert.assertEquals(namesrvAddrs.get(0).getTarget(), AddressScheme.IPv4.getPrefix() + dummyNameServerAddr);
//        }
//        {
//            clientConfig.setNamesrvAddr("127.0.0.1:9876;127.0.0.2:9876");
//            final List<Endpoints> namesrvAddrs = clientConfig.getNamesrvAddr();
//            Assert.assertEquals(namesrvAddrs.size(), 2);
//        }
    }
}