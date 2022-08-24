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

package golang

import (
	"context"
	"fmt"
	"sync"
)

type TransactionResolution int32

const (
	UNKNOW TransactionResolution = iota // 开始生成枚举值, 默认为0
	COMMIT
	ROLLBACK
)

const (
	MAX_MESSAGE_NUM = 1
)

type TransactionChecker struct {
	Check func(msg *MessageView) TransactionResolution
}

type Transaction interface {
	Commit() error
	RollBack() error
}

var _ = Transaction(&transactionImpl{})

type transactionImpl struct {
	producerImpl          Producer
	messages              map[string]*PublishingMessage
	messagesLock          sync.RWMutex
	messageSendReceiptMap sync.Map
}

var NewTransactionImpl = func(producerImpl Producer) *transactionImpl {
	return &transactionImpl{
		producerImpl: producerImpl,
		messages:     make(map[string]*PublishingMessage),
	}
}

func (t *transactionImpl) Commit() error {
	t.messageSendReceiptMap.Range(func(_, value interface{}) bool {
		pubMessage := value.([]interface{})[0].(*PublishingMessage)
		sendReceipt := value.([]interface{})[1].(*SendReceipt)
		err := t.producerImpl.(*defaultProducer).endTransaction(context.TODO(), sendReceipt.Endpoints,
			pubMessage.msg.GetMessageCommon(), sendReceipt.MessageID, sendReceipt.TransactionId, COMMIT)
		if err != nil {
			sugarBaseLogger.Errorf("transaction message commit failed, err=%w", err)
		}
		return true
	})
	return nil
}

func (t *transactionImpl) RollBack() error {
	t.messageSendReceiptMap.Range(func(_, value interface{}) bool {
		pubMessage := value.([]interface{})[0].(*PublishingMessage)
		sendReceipt := value.([]interface{})[1].(*SendReceipt)
		err := t.producerImpl.(*defaultProducer).endTransaction(context.TODO(), sendReceipt.Endpoints,
			pubMessage.msg.GetMessageCommon(), sendReceipt.MessageID, sendReceipt.TransactionId, ROLLBACK)
		if err != nil {
			sugarBaseLogger.Errorf("transaction message rollback failed, err=%w", err)
		}
		return true
	})
	return nil
}

func (t *transactionImpl) tryAddMessage(message *Message) (*PublishingMessage, error) {
	t.messagesLock.RLock()
	if len(t.messages) > MAX_MESSAGE_NUM {
		return nil, fmt.Errorf("message in transaction has exceeded the threshold: %d", MAX_MESSAGE_NUM)
	}
	t.messagesLock.RUnlock()

	t.messagesLock.Lock()
	defer t.messagesLock.Unlock()
	if len(t.messages) > MAX_MESSAGE_NUM {
		return nil, fmt.Errorf("message in transaction has exceeded the threshold: %d", MAX_MESSAGE_NUM)
	}
	pubMessage, err := NewPublishingMessage(message, t.producerImpl.(*defaultProducer).pSetting, true)
	if err != nil {
		return nil, err
	}
	t.messages[pubMessage.messageId] = pubMessage
	return pubMessage, nil
}

func (t *transactionImpl) tryAddReceipt(pubMessage *PublishingMessage, sendReceipt *SendReceipt) error {
	t.messagesLock.RLock()
	defer t.messagesLock.RUnlock()

	if _, ok := t.messages[pubMessage.messageId]; !ok {
		return fmt.Errorf("message(s) is not contained in current transaction")
	}
	pair := []interface{}{pubMessage, sendReceipt}
	t.messageSendReceiptMap.Store(pubMessage.messageId, pair)
	return nil
}
