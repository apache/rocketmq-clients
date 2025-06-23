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
	"fmt"
	"time"
)

type ConsumeService interface {
	consume(ProcessQueue, []*MessageView)
	consumeWithDuration(*MessageView, time.Duration, func(ConsumerResult, error))
	Shutdown() error
}

type baseConsumeService struct {
	clientId            string
	messageListener     MessageListener
	consumptionExecutor *simpleThreadPool
	messageInterceptor  MessageInterceptor
}

func NewBaseConsumeService(clientId string, messageListener MessageListener, consumptionExecutor *simpleThreadPool, messageInterceptor MessageInterceptor) *baseConsumeService {
	return &baseConsumeService{
		clientId:            clientId,
		messageListener:     messageListener,
		consumptionExecutor: consumptionExecutor,
		messageInterceptor:  messageInterceptor,
	}
}

func (bcs *baseConsumeService) consumeImmediately(messageView *MessageView, callback func(ConsumerResult, error)) {
	bcs.consumeWithDuration(messageView, 0, callback)
}
func (bcs *baseConsumeService) consumeWithDuration(messageView *MessageView, duration time.Duration, callback func(ConsumerResult, error)) {
	task := bcs.newConsumeTask(bcs.clientId, bcs.messageListener, messageView, bcs.messageInterceptor, callback)
	if duration <= 0 {
		bcs.consumptionExecutor.Submit(task)
		return
	}
	time.AfterFunc(duration, func() { bcs.consumptionExecutor.Submit(task) })
}

func (bcs *baseConsumeService) Shutdown() error {
	bcs.consumptionExecutor.Shutdown()
	return nil
}

func (bcs *baseConsumeService) newConsumeTask(clientId string, messageListener MessageListener, messageView *MessageView, messageInterceptor MessageInterceptor, callback func(ConsumerResult, error)) func() {
	return func() {
		consumeResult := FAILURE
		defer func() {
			if e := recover(); e != nil {
				err, ok := e.(error)
				if !ok {
					err = fmt.Errorf("panic cause [%v]", e)
				}
				sugarBaseLogger.Errorf("Message Interceptor raised an exception while consuming messages, clientId=%s, mq=%s, messageId=%s, err=%w", clientId, messageView.messageQueue.String(), messageView.messageId, err)
				callback(FAILURE, err)
			} else {
				callback(consumeResult, nil)
			}
		}()
		messageInterceptor.doBefore(MessageHookPoints_CONSUME, []*MessageCommon{messageView.GetMessageCommon()})
		startTime := time.Now()
		func() {
			defer func() {
				if e := recover(); e != nil {
					consumeResult = FAILURE
					err, ok := e.(error)
					if !ok {
						err = fmt.Errorf("panic cause [%v]", e)
					}
					sugarBaseLogger.Errorf("Message listener raised an exception while consuming messages, clientId=%s, mq=%s, messageId=%s, err=%w", clientId, messageView.messageQueue.String(), messageView.messageId, err)
				}
			}()
			consumeResult = messageListener.consume(messageView)
		}()
		duration := time.Since(startTime)
		status := MessageHookPointsStatus_ERROR
		if consumeResult == SUCCESS {
			status = MessageHookPointsStatus_OK
		}
		messageInterceptor.doAfter(MessageHookPoints_CONSUME, []*MessageCommon{messageView.GetMessageCommon()}, duration, status)
	}
}

var _ = ConsumeService(&standardConsumeService{})

type standardConsumeService struct {
	baseConsumeService
}
type fifoConsumeService struct {
	baseConsumeService
}

func (scs *standardConsumeService) consume(pq ProcessQueue, messageViews []*MessageView) {
	for _, mv := range messageViews {
		if mv.isCorrupted() {
			sugarBaseLogger.Errorf("Message is corrupted for standard consumption, prepare to discard it, mq=%s, messageId=%s, clientId=%s", pq.getMessageQueue().String(), mv.GetMessageId(), scs.clientId)
			pq.discardMessage(mv)
			continue
		}
		scs.consumeImmediately(mv, func(result ConsumerResult, err error) {
			if err != nil {
				sugarBaseLogger.Errorf("[Bug] Exception raised in consumption callback, clientId=%s", scs.clientId)
				return
			}
			pq.eraseMessage(mv, result)
		})
	}
}

func NewStandardConsumeService(clientId string, messageListener MessageListener, consumptionExecutor *simpleThreadPool, messageInterceptor MessageInterceptor) *standardConsumeService {
	return &standardConsumeService{
		*NewBaseConsumeService(clientId, messageListener, consumptionExecutor, messageInterceptor),
	}
}

func (fcs *fifoConsumeService) consume(pq ProcessQueue, messageViews []*MessageView) {
	fcs.consumeIteratively(pq, &messageViews, 0)
}
func (fcs *fifoConsumeService) consumeIteratively(pq ProcessQueue, messageViewsPtr *[]*MessageView, ptr int) {
	if messageViewsPtr == nil {
		sugarBaseLogger.Errorf("[Bug] messageViews is nil when consumeIteratively")
		return
	}
	messageViews := *messageViewsPtr
	if ptr >= len(messageViews) {
		return
	}
	mv := messageViews[ptr]
	if mv.isCorrupted() {
		sugarBaseLogger.Errorf("Message is corrupted for FIFO consumption, prepare to discard it, mq=%s, messageId=%s, clientId=%s", pq.getMessageQueue().String(), mv.GetMessageId(), fcs.clientId)
		pq.discardFifoMessage(mv)
		fcs.consumeIteratively(pq, messageViewsPtr, ptr+1)
		return
	}
	fcs.consumeImmediately(mv, func(result ConsumerResult, err error) {
		if err != nil {
			sugarBaseLogger.Errorf("[Bug] Exception raised in consumption callback, clientId=%s", fcs.clientId)
			return
		}
		fmt.Printf("ConsumerResult: %v\n", result)
		pq.eraseFifoMessage(mv, result)
		fcs.consumeIteratively(pq, messageViewsPtr, ptr+1)
	})
}

func NewFiFoConsumeService(clientId string, messageListener MessageListener, consumptionExecutor *simpleThreadPool, messageInterceptor MessageInterceptor) *fifoConsumeService {
	return &fifoConsumeService{
		*NewBaseConsumeService(clientId, messageListener, consumptionExecutor, messageInterceptor),
	}
}
