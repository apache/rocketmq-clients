package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;

@Getter
@Setter
public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;
    private int checkThreadPoolMinSize = 1;
    private int checkThreadPoolMaxSize = 1;
    private int checkRequestHoldMax = 2000;


    public TransactionMQProducer(String producerGroup) {
        super(producerGroup);
    }

    public TransactionMQProducer(String arn, String producerGroup) {
        super(arn, producerGroup);
    }

    /**
     * This method is to send transactional messages.
     *
     * @param msg      Transactional message to send.
     * @param executor local transaction executor.
     * @param object   Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter executor,
                                                          Object object) throws MQClientException {
        if (null == transactionCheckListener) {
            throw new MQClientException("Transaction check listener is null unexpectedly, please set it.");
        }
        return this.impl.sendTransaction(msg, executor, object);
    }
}
