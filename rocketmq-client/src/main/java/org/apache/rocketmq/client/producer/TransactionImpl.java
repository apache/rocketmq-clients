package org.apache.rocketmq.client.producer;

import java.util.concurrent.TimeoutException;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.remoting.Endpoints;

@AllArgsConstructor
public class TransactionImpl implements Transaction {
    private final String messageId;
    private final String transactionId;
    private final Endpoints endpoints;
    private final DefaultMQProducerImpl producerImpl;

    @Override
    public void commit() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.commit(endpoints, messageId, transactionId);
    }

    @Override
    public void rollback() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.rollback(endpoints, messageId, transactionId);
    }
}
