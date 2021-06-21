package org.apache.rocketmq.client.impl;

import static org.testng.Assert.*;

import java.util.List;
import org.apache.rocketmq.client.conf.BaseConfig;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.AddressScheme;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientConfigTest extends BaseConfig {
    private final ClientConfig clientConfig;

    public ClientConfigTest() {
        this.clientConfig = new ClientConfig(dummyConsumerGroup);
    }

    @Test
    public void testSetNamesrvAddr() {
        {
            clientConfig.setNamesrvAddr(dummyNameServerAddr);
            final List<Endpoints> namesrvAddrs = clientConfig.getNamesrvAddr();
            Assert.assertEquals(namesrvAddrs.size(), 1);
            Assert.assertEquals(namesrvAddrs.get(0).getTarget(), AddressScheme.IPv4.getPrefix() + dummyNameServerAddr);
        }
        {
            clientConfig.setNamesrvAddr("127.0.0.1:9876;127.0.0.2:9876");
            final List<Endpoints> namesrvAddrs = clientConfig.getNamesrvAddr();
            Assert.assertEquals(namesrvAddrs.size(), 2);
        }
    }
}