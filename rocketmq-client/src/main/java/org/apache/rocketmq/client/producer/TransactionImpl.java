package org.apache.rocketmq.client.producer;

import java.util.concurrent.TimeoutException;
import lombok.Getter;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;

public class TransactionImpl implements Transaction {
    @Getter
    private final SendResult sendResult;
    private final DefaultMQProducerImpl producerImpl;

    public TransactionImpl(SendResult sendResult, DefaultMQProducerImpl producerImpl) {
        this.sendResult = sendResult;
        this.producerImpl = producerImpl;
    }

    @Override
    public void commit() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.commit(sendResult.getEndpoints(), sendResult.getMsgId(), sendResult.getTransactionId());
    }

    @Override
    public void rollback() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.rollback(sendResult.getEndpoints(), sendResult.getMsgId(), sendResult.getTransactionId());
    }
}
