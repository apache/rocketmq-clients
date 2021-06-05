package org.apache.rocketmq.client.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.client.constant.SystemProperty;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.AddressScheme;

public class TopAddressing {
    private static final String DEFAULT_NAME_SERVER_DOMAIN = "jmenv.tbsite.net";
    private static final String DEFAULT_NAME_SERVER_SUB_GROUP = "nsaddr";

    private final HttpClient httpClient;
    private final String wsAddress;
    private String unitName;

    public TopAddressing() {
        this.httpClient = HttpClients.createDefault();
        this.wsAddress = getWsAddress();
    }

    private String getWsAddress() {
        final String wsDomain =
                System.getProperty(SystemProperty.NAME_SERVER_DOMAIN, DEFAULT_NAME_SERVER_DOMAIN);
        final String wsSubGroup =
                System.getProperty(SystemProperty.NAME_SERVER_SUB_GROUP, DEFAULT_NAME_SERVER_SUB_GROUP);
        String wsAddress = "http://" + wsDomain + ":8080/rocketmq/" + wsSubGroup;
        if (wsDomain.contains(":")) {
            wsAddress = "http://" + wsDomain + "/rocketmq/" + wsSubGroup;
        }
        return wsAddress;
    }

    // TODO: check result format here.
    public Endpoints fetchNameServerAddresses() throws IOException {
        final HttpGet httpGet = new HttpGet(wsAddress);
        final HttpResponse response = httpClient.execute(httpGet);
        final HttpEntity entity = response.getEntity();
        final String body = EntityUtils.toString(entity, MixAll.DEFAULT_CHARSET);
        // TODO: check split result here.
        final String[] nameServerAddresses = body.split(";");

        List<Address> addresses = new ArrayList<Address>();
        for (String nameServerAddress : nameServerAddresses) {
            final String[] split = nameServerAddress.split(":");
            String host = split[0];
            final int port = Integer.parseInt(split[1]);
            addresses.add(new Address(host, port));
        }
        return new Endpoints(AddressScheme.IPv4, addresses);
    }
}
