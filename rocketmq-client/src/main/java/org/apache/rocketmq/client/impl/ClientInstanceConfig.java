package org.apache.rocketmq.client.impl;

import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.rocketmq.client.remoting.AccessCredential;

@Data
public class ClientInstanceConfig {
    private String arn = "";
    private AccessCredential accessCredential = null;

    public void setArn(String arn) {
        Preconditions.checkNotNull(arn, "Abstract resource name is null, please set it.");
        this.arn = arn;
    }
}
