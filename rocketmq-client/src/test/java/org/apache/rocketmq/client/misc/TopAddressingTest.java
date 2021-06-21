package org.apache.rocketmq.client.misc;

import java.io.IOException;
import java.util.List;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopAddressingTest {

    TopAddressing topAddressing = new TopAddressing();

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test
    public void testFetchNameServerAddresses() throws IOException {
        final List<Endpoints> endpoints = topAddressing.fetchNameServerAddresses();
        Assert.assertNotNull(endpoints);
        Assert.assertFalse(endpoints.isEmpty());
    }
}
