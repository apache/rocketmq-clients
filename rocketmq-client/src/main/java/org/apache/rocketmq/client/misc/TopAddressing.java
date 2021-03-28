package org.apache.rocketmq.client.misc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.client.constant.SystemProperty;

public class TopAddressing {
  private static final String DEFAULT_NAME_SERVER_DOMAIN = "jmenv.tbsite.net";
  private static final String DEFAULT_NAME_SERVER_SUB_GROUP = "nsaddr";

  private final HttpClient httpClient;
  private final String wsAddress;
  private String unitName;

  public TopAddressing() {
    this.httpClient = HttpClients.createDefault();
    this.wsAddress = getWSAddress();
  }

  private String getWSAddress() {
    final String wsDomain =
        System.getProperty(SystemProperty.NAME_SERVER_DOMAIN.getName(), DEFAULT_NAME_SERVER_DOMAIN);
    final String wsSubGroup =
        System.getProperty(
            SystemProperty.NAME_SERVER_SUB_GROUP.getName(), DEFAULT_NAME_SERVER_SUB_GROUP);
    String wsAddress = "http://" + wsDomain + ":8080/rocketmq/" + wsSubGroup;
    if (wsDomain.indexOf(":") > 0) {
      wsAddress = "http://" + wsDomain + "/rocketmq/" + wsSubGroup;
    }
    return wsAddress;
  }

  public List<String> fetchNameServerAddresses() throws IOException {
    final HttpGet httpGet = new HttpGet(wsAddress);
    final HttpResponse response = httpClient.execute(httpGet);
    final HttpEntity entity = response.getEntity();
    final String body = EntityUtils.toString(entity, StandardCharsets.UTF_8);
    final String[] nameServerAddresses = body.split(";");
    return new ArrayList<>(Arrays.asList(nameServerAddresses));
  }
}
