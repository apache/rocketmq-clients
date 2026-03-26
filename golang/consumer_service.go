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

var (
	messageGroupExtractor = func(mv *MessageView) string {
		messageGroup := mv.GetMessageGroup()
		if messageGroup != nil {
			return *messageGroup
		}
		return ""
	}

	liteTopicExtractor = func(mv *MessageView) string {
		return mv.GetLiteTopic()
	}
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
		// Check if result is SUCCESS or SUSPEND (considered as successful)
		if consumeResult.Type == ConsumerResultTypeSuccess || consumeResult.Type == ConsumerResultTypeSuspend {
			status = MessageHookPointsStatus_OK
		}
		messageInterceptor.doAfter(MessageHookPoints_CONSUME, []*MessageCommon{messageView.GetMessageCommon()}, duration, status)
	}
}

var _ = ConsumeService(&standardConsumeService{})
var _ = ConsumeService(&liteFifoConsumeService{})

type standardConsumeService struct {
	baseConsumeService
}
type fifoConsumeService struct {
	baseConsumeService
	enableFifoConsumeAccelerator bool
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

// groupMessageBy groups messages by applying the provided groupKeyExtractor function
// It returns two maps: grouped messages and messages without a group key
func groupMessageBy(messageViews []*MessageView, groupKeyExtractor func(*MessageView) string) (map[string][]*MessageView, []*MessageView) {
	messageViewsGroupByGroupKey := make(map[string][]*MessageView)
	messageViewsWithoutGroupKey := make([]*MessageView, 0)

	for _, messageView := range messageViews {
		groupKey := groupKeyExtractor(messageView)
		if groupKey != "" {
			messageViewsGroupByGroupKey[groupKey] = append(messageViewsGroupByGroupKey[groupKey], messageView)
		} else {
			messageViewsWithoutGroupKey = append(messageViewsWithoutGroupKey, messageView)
		}
	}

	groupNum := len(messageViewsGroupByGroupKey)
	if len(messageViewsWithoutGroupKey) > 0 {
		groupNum++
	}
	sugarBaseLogger.Debugf("FifoConsumeService parallel consume, messageViewsNum=%d, groupNum=%d", len(messageViews), groupNum)

	return messageViewsGroupByGroupKey, messageViewsWithoutGroupKey
}

func (fcs *fifoConsumeService) consume(pq ProcessQueue, messageViews []*MessageView) {
	if !fcs.enableFifoConsumeAccelerator || len(messageViews) <= 1 {
		fcs.consumeIteratively(pq, &messageViews, 0)
		return
	}

	messageViewsGroupByGroupKey, messageViewsWithoutGroupKey := groupMessageBy(
		messageViews,
		messageGroupExtractor,
	)

	// Consume messages in parallel by group
	for _, group := range messageViewsGroupByGroupKey {
		fcs.consumeIteratively(pq, &group, 0)
	}
	if len(messageViewsWithoutGroupKey) > 0 {
		fcs.consumeIteratively(pq, &messageViewsWithoutGroupKey, 0)
	}
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
		pq.eraseFifoMessage(mv, result)
		fcs.consumeIteratively(pq, messageViewsPtr, ptr+1)
	})
}

func NewFiFoConsumeService(clientId string, messageListener MessageListener, consumptionExecutor *simpleThreadPool, messageInterceptor MessageInterceptor, enableFifoConsumeAccelerator bool) *fifoConsumeService {
	return &fifoConsumeService{
		baseConsumeService:           *NewBaseConsumeService(clientId, messageListener, consumptionExecutor, messageInterceptor),
		enableFifoConsumeAccelerator: enableFifoConsumeAccelerator,
	}
}

// liteFifoConsumeService is a fifoConsumeService that used for lite push consumer
type liteFifoConsumeService struct {
	baseConsumeService
	enableFifoConsumeAccelerator bool
}

func (lcs *liteFifoConsumeService) consume(pq ProcessQueue, messageViews []*MessageView) {
	if !lcs.enableFifoConsumeAccelerator || len(messageViews) <= 1 {
		lcs.consumeIteratively(pq, &messageViews, 0)
		return
	}

	messageViewsGroupByGroupKey, messageViewsWithoutGroupKey := groupMessageBy(
		messageViews,
		liteTopicExtractor,
	)

	for _, group := range messageViewsGroupByGroupKey {
		lcs.consumeIteratively(pq, &group, 0)
	}
	if len(messageViewsWithoutGroupKey) > 0 {
		lcs.consumeIteratively(pq, &messageViewsWithoutGroupKey, 0)
	}
}

func (lcs *liteFifoConsumeService) consumeIteratively(pq ProcessQueue, messageViewsPtr *[]*MessageView, ptr int) {
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
		sugarBaseLogger.Errorf("Message is corrupted for FIFO consumption, prepare to discard it, mq=%s, messageId=%s, clientId=%s", pq.getMessageQueue().String(), mv.GetMessageId(), lcs.clientId)
		pq.discardFifoMessage(mv)
		lcs.consumeIteratively(pq, messageViewsPtr, ptr+1)
		return
	}
	lcs.consumeImmediately(mv, func(result ConsumerResult, err error) {
		if err != nil {
			sugarBaseLogger.Errorf("[Bug] Exception raised in consumption callback, clientId=%s", lcs.clientId)
			return
		}
		pq.eraseFifoMessage(mv, result)
		if result.Type == ConsumerResultTypeSuspend {
			// Suspend all messages with the same liteTopic in this batch
			newMsgList := make([]*MessageView, 0)
			for i := ptr + 1; i < len(messageViews); i++ {
				msgView := messageViews[i]
				if msgView.GetLiteTopic() == mv.GetLiteTopic() {
					pq.eraseFifoMessage(msgView, result)
				} else {
					newMsgList = append(newMsgList, msgView)
				}
			}
			// Continue processing remaining messages with different liteTopic
			if len(newMsgList) > 0 {
				lcs.consumeIteratively(pq, &newMsgList, 0)
			}
		} else {
			lcs.consumeIteratively(pq, messageViewsPtr, ptr+1)
		}
	})
}

func NewLiteFifoConsumeService(clientId string, messageListener MessageListener, consumptionExecutor *simpleThreadPool, messageInterceptor MessageInterceptor, enableFifoConsumeAccelerator bool) *liteFifoConsumeService {
	return &liteFifoConsumeService{
		baseConsumeService:           *NewBaseConsumeService(clientId, messageListener, consumptionExecutor, messageInterceptor),
		enableFifoConsumeAccelerator: enableFifoConsumeAccelerator,
	}
}
