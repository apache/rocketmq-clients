/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.producer;

import java.util.concurrent.TimeoutException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.message.Message;

@AllArgsConstructor
public class TransactionImpl implements Transaction {
    @Getter
    private final SendResult sendResult;
    private final Message message;
    private final DefaultMQProducerImpl producerImpl;

    @Override
    public void commit() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.commit(sendResult.getEndpoints(), message.getMessageExt(), sendResult.getTransactionId());
    }

    @Override
    public void rollback() throws ClientException, ServerException, InterruptedException, TimeoutException {
        producerImpl.rollback(sendResult.getEndpoints(), message.getMessageExt(), sendResult.getTransactionId());
    }
}
