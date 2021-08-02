package org.apache.rocketmq.client.producer;

import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;

public interface Transaction {
    void commit() throws ClientException, ServerException, InterruptedException, TimeoutException;

    void rollback() throws ClientException, ServerException, InterruptedException, TimeoutException;

    SendResult getSendResult();
}
