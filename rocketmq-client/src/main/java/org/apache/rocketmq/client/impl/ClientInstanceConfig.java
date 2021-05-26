package org.apache.rocketmq.client.impl;

import lombok.Data;
import org.apache.rocketmq.client.remoting.AccessCredential;

@Data
public class ClientInstanceConfig {
    private String arn;
    private AccessCredential accessCredential;
}
