package org.apache.rocketmq.client.remoting;

import org.apache.rocketmq.client.exception.ClientException;

public interface CredentialsProvider {
    Credentials getCredentials() throws ClientException;
}
