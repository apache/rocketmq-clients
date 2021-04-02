package org.apache.rocketmq.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.constant.LanguageCode;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.ResourceUtil;
import org.apache.rocketmq.client.misc.TLSSystemConfig;
import org.apache.rocketmq.utility.RemotingUtil;

public class ClientConfig {

  @Getter @Setter private List<String> nameServerList;
  @Getter @Setter private String clientIP;
  @Getter @Setter private String groupName;
  private String unitName;
  @Getter @Setter private String instanceName;
  protected String namespace;

  @Getter @Setter private long routeUpdatePeriodMillis = 30 * 1000;
  @Getter @Setter private long heartbeatPeriodMillis = 30 * 1000;

  private boolean useTLS;
  private LanguageCode language;

  public ClientConfig() {
    this.nameServerList = new ArrayList<String>();
    this.clientIP = RemotingUtil.getLocalAddress();
    this.instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    this.useTLS = TLSSystemConfig.tlsEnable;
    this.language = LanguageCode.JAVA;
  }

  public void setNamesrvAddr(String nameServerStr) {
    final String[] nameServerList = nameServerStr.split(";");
    this.nameServerList = Arrays.asList(nameServerList);
  }

  public String buildClientId() {
    StringBuilder sb = new StringBuilder();
    sb.append(clientIP);
    sb.append("@");
    sb.append(instanceName);
    if (StringUtils.isNotBlank(unitName)) {
      sb.append("@");
      sb.append(unitName);
    }
    return sb.toString();
  }

  public String withNamespace(String resource) {
    return ResourceUtil.wrapWithNamespace(namespace, resource);
  }

  public Set<String> withNamespace(Set<String> resourceSet) {
    Set<String> resourceWithNamespace = new HashSet<String>();
    for (String resource : resourceSet) {
      resourceWithNamespace.add(withNamespace(resource));
    }
    return resourceWithNamespace;
  }

  public String unwrapWithNamespace(String resource) {
    return ResourceUtil.unwrapWithNamespace(resource, namespace);
  }

  public Set<String> unwrapWithNamespace(Set<String> resourceSet) {
    Set<String> newResourceSet = new HashSet<String>();
    for (String resource : resourceSet) {
      newResourceSet.add(unwrapWithNamespace(resource));
    }
    return newResourceSet;
  }

  public MessageQueue queueWrapWithNamespace(MessageQueue queue) {
    if (StringUtils.isEmpty(namespace)) {
      return queue;
    }

    return new MessageQueue(
        withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
  }

  public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
    if (StringUtils.isEmpty(namespace)) {
      return queues;
    }
    for (MessageQueue queue : queues) {
      queue.setTopic(withNamespace(queue.getTopic()));
    }
    return queues;
  }
}
