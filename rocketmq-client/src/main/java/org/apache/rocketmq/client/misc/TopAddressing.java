package org.apache.rocketmq.client.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.SystemProperty;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.utility.HttpTinyClient;

@Slf4j
public class TopAddressing {
    private static final int HTTP_TIMEOUT_MILLIS = 3 * 1000;
    private static final String DEFAULT_NAME_SERVER_DOMAIN = "jmenv.tbsite.net";
    private static final String DEFAULT_NAME_SERVER_SUB_GROUP = "nsaddr";

    private final String wsAddress;
    private String unitName;

    public TopAddressing() {
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
    public List<Endpoints> fetchNameServerAddresses() throws IOException {
        List<Endpoints> endpointsList = new ArrayList<Endpoints>();

        final HttpTinyClient.HttpResult httpResult = HttpTinyClient.httpGet(wsAddress, HTTP_TIMEOUT_MILLIS);
        if (httpResult.isOk()) {
            final String content = httpResult.getContent();
            // TODO: check split result here.
            final String[] nameServerAddresses = content.split(";");

            for (String nameServerAddress : nameServerAddresses) {
                List<Address> addresses = new ArrayList<Address>();
                final String[] split = nameServerAddress.split(":");
                String host = split[0];
                final int port = Integer.parseInt(split[1]);
                addresses.add(new Address(host, port));
                endpointsList.add(new Endpoints(AddressScheme.IPv4, addresses));
            }
            return endpointsList;
        }
        log.error("Failed to name server addresses from topAddress, httpCode={}", httpResult.getCode());
        return endpointsList;
    }
}
