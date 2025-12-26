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
	"math"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/google/uuid"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProcessQueue interface {
	getMessageQueue() *v2.MessageQueue
	expired() bool
	drop()
	fetchMessageImmediately()
	discardMessage(*MessageView)
	eraseMessage(*MessageView, ConsumerResult)
	discardFifoMessage(*MessageView)
	eraseFifoMessage(*MessageView, ConsumerResult)
}

const (
	FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY time.Duration = time.Second
	ACK_MESSAGE_FAILURE_BACKOFF_DELAY                 time.Duration = time.Second
	CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY   time.Duration = time.Second
	RECEIVING_FLOW_CONTROL_BACKOFF_DELAY              time.Duration = time.Millisecond * 20
	RECEIVING_FAILURE_BACKOFF_DELAY                   time.Duration = time.Second
	RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL        time.Duration = time.Second
)

type defaultProcessQueue struct {
	consumer         *defaultPushConsumer
	dropped          atomic.Bool
	mqstr            utils.MessageQueueStr
	mq               *v2.MessageQueue
	filterExpression *FilterExpression

	cachedMessagesNums       atomic.Int32
	cachedMessagesBytes      atomic.Int64
	receptionTimes           atomic.Int64
	receivedMessagesQuantity atomic.Int64
	activityNanoTime         atomic.Int64
	cacheFullNanoTime        atomic.Int64
}

func (dpq *defaultProcessQueue) discardFifoMessage(mv *MessageView) {
	dpq.consumer.cli.log.Infof("Discard fifo message, mq=%s, messageId=%s, clientId=%s", dpq.mqstr, mv.GetMessageId(), dpq.consumer.cli.clientID)
	dpq.forwardToDeadLetterQueue(mv, func(error) { dpq.evictCacheMessage(mv) })
}

func (dpq *defaultProcessQueue) eraseFifoMessage(mv *MessageView, result ConsumerResult) {
	retryPolicy := dpq.consumer.pcSettings.GetRetryPolicy()
	maxAttempts := retryPolicy.MaxAttempts
	attempt := mv.GetMessageCommon().deliveryAttempt
	messageId := mv.GetMessageId()
	service := dpq.consumer.consumerService
	clientId := dpq.consumer.cli.clientID

	if result == FAILURE && attempt < maxAttempts {
		nextAttemptDelay := utils.GetNextAttemptDelay(retryPolicy, int(attempt))
		mv.deliveryAttempt += 1
		attempt = mv.deliveryAttempt
		dpq.consumer.cli.log.Debugf("Prepare to redeliver the fifo message because of the consumption failure, maxAttempt={},"+
			" attempt=%d, mq=%s, messageId=%s, nextAttemptDelay=%v, clientId=%s", maxAttempts, attempt, dpq.mqstr,
			messageId, nextAttemptDelay, clientId)
		service.consumeWithDuration(mv, nextAttemptDelay, func(result0 ConsumerResult, err0 error) {
			dpq.eraseFifoMessage(mv, result0)
		})
		return
	}

	if result != SUCCESS {
		dpq.consumer.cli.log.Infof("Failed to consume fifo message finally, run out of attempt times, maxAttempts=%d, "+
			"attempt=%d, mq=%s, messageId=%s, clientId=%s", maxAttempts, attempt, dpq.mqstr, messageId, clientId)
	}
	// Ack message or forward it to DLQ depends on consumption result.
	if result == SUCCESS {
		dpq.ackMessage(mv, func(error) { dpq.evictCacheMessage(mv) })
	} else {
		dpq.forwardToDeadLetterQueue(mv, func(error) { dpq.evictCacheMessage(mv) })
	}
}

func (dpq *defaultProcessQueue) forwardToDeadLetterQueue(mv *MessageView, callback func(error)) {
	dpq.forwardToDeadLetterQueue0(mv, 1, callback)
}

func (dpq *defaultProcessQueue) forwardToDeadLetterQueue0(mv *MessageView, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	consumerGroup := dpq.consumer.groupName
	messageId := mv.messageId
	endpoints := mv.endpoints

	ctx := context.Background()
	resp, err := dpq.consumer.forwardMessageToDeadLetterQueue0(ctx, mv)
	if err != nil {
		dpq.consumer.cli.log.Errorf("Exception raised while acknowledging message, clientId=%s, consumerGroup=%s, "+
			"would attempt to re-ack later, attempt=%d, messageId=%s, mq=%s, endpoints=%v, err=%w", clientId,
			consumerGroup, attempt, messageId, dpq.mqstr, endpoints, err)
		dpq.forwardToDeadLetterQueueLater(mv, 1+attempt, callback)
		return
	}
	requestId := utils.GetRequestID(ctx)
	status := resp.GetStatus()
	code := status.GetCode()
	// Log failure and retry later.
	if code != v2.Code_OK {
		dpq.consumer.cli.log.Errorf("Failed to forward message to dead letter queue, would attempt to re-forward later, "+
			" clientId=%s, consumerGroup=%s, messageId=%s, attempt=%d, mq=%s, endpoints=%v, requestId=%s, status message=[%s]", clientId, consumerGroup, messageId, attempt, dpq.mqstr,
			endpoints, requestId, status.GetMessage())
		dpq.forwardToDeadLetterQueueLater(mv, 1+attempt, callback)
	}
	// Set result if succeed in changing invisible time.
	callback(nil)
	// Log retries.
	if attempt > 1 {
		dpq.consumer.cli.log.Infof("Re-forward message to dead letter queue successfully, clientId=%s, consumerGroup=%s "+
			"messageId={}, attempt={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
			messageId, attempt, dpq.mqstr, endpoints, requestId)
		return
	}
	dpq.consumer.cli.log.Debugf("Forward message to dead letter queue successfully, clientId=%s, consumerGroup=%s, messageId=%s, "+
		"mq=%s, endpoints=%v, requestId=%s", clientId, consumerGroup, messageId, dpq.mqstr, endpoints,
		requestId)
}

func (dpq *defaultProcessQueue) forwardToDeadLetterQueueLater(mv *MessageView, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	messageId := mv.messageId
	time.AfterFunc(ACK_MESSAGE_FAILURE_BACKOFF_DELAY, func() {
		defer func() {
			if err := recover(); err != nil {
				dpq.consumer.cli.log.Errorf("[Bug] Failed to schedule message change invisible duration request, mq=%s, messageId=%s, "+
					"clientId=%s", dpq.mqstr, messageId, clientId)
				dpq.ackMessageLater(mv, 1+attempt, callback)
			}
		}()
		dpq.ackMessage0(mv, attempt, callback)
	})
}

func (dpq *defaultProcessQueue) eraseMessage(mv *MessageView, consumeResult ConsumerResult) {
	if consumeResult == SUCCESS {
		dpq.consumer.consumptionOkQuantity.Inc()
		dpq.ackMessage(mv, func(error) { dpq.evictCacheMessage(mv) })
	} else {
		dpq.consumer.consumptionErrorQuantity.Inc()
		dpq.nackMessage(mv, func(error) { dpq.evictCacheMessage(mv) })
	}

}

func (dpq *defaultProcessQueue) discardMessage(mv *MessageView) {
	dpq.consumer.cli.log.Infof("Discard message, mq=%s, messageId=%s, clientId=%s", dpq.mqstr, mv.GetMessageId(), dpq.consumer.cli.clientID)
	dpq.nackMessage(mv, func(error) { dpq.evictCacheMessage(mv) })
}

func (dpq *defaultProcessQueue) nackMessage(mv *MessageView, callback func(error)) {
	deliveryAttempt := mv.GetMessageCommon().deliveryAttempt
	duration := utils.GetNextAttemptDelay(dpq.consumer.pcSettings.GetRetryPolicy(), int(deliveryAttempt))
	dpq.changeInvisibleDuration(mv, duration, 1, callback)
}

func (dpq *defaultProcessQueue) changeInvisibleDuration(mv *MessageView, duration time.Duration, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	consumerGroup := dpq.consumer.groupName
	messageId := mv.messageId
	endpoints := mv.endpoints

	ctx := context.Background()
	resp, err := dpq.consumer.changeInvisibleDuration0(ctx, mv, duration)
	if err != nil {
		dpq.consumer.cli.log.Errorf("Exception raised while changing invisible duration, would retry later, clientId=%s, consumerGroup=%s, messageId=%s, mq=%s, endpoints=%v, err=%w",
			clientId, consumerGroup, messageId, dpq.mqstr, endpoints, err)
		dpq.changeInvisibleDurationLater(mv, duration, 1+attempt, callback)
		return
	}
	requestId := utils.GetRequestID(ctx)
	status := resp.GetStatus()
	code := status.GetCode()
	if code == v2.Code_INVALID_RECEIPT_HANDLE {
		dpq.consumer.cli.log.Errorf("Failed to change invisible duration due to the invalid receipt handle, forgive to retry, "+
			"clientId=%s, consumerGroup=%s, messageId=%s, attempt=%d, mq=%s, endpoints=%v, "+
			"requestId=%s, status message=[%s]", clientId, consumerGroup, messageId, attempt, dpq.mqstr,
			endpoints, requestId, status.GetMessage())
		callback(&ErrRpcStatus{Code: int32(v2.Code_BAD_REQUEST), Message: code.String()})
		return
	}
	// Log failure and retry later.
	if code != v2.Code_OK {
		dpq.consumer.cli.log.Errorf("Failed to change invisible duration, would retry later, "+
			" clientId=%s, consumerGroup=%s, messageId=%s, attempt=%d, mq=%s, endpoints=%v, requestId=%s, status message=[%s]", clientId, consumerGroup, messageId, attempt, dpq.mqstr,
			endpoints, requestId, status.GetMessage())
		dpq.changeInvisibleDurationLater(mv, duration, 1+attempt, callback)
	}
	// Set result if succeed in changing invisible time.
	callback(nil)
	// Log retries.
	if attempt > 1 {
		dpq.consumer.cli.log.Infof("Finally, change invisible duration successfully, clientId=%s, consumerGroup=%s "+
			"messageId={}, attempt={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
			messageId, attempt, dpq.mqstr, endpoints, requestId)
		return
	}
	dpq.consumer.cli.log.Debugf("Change invisible duration successfully, clientId=%s, consumerGroup=%s, messageId=%s, "+
		"mq=%s, endpoints=%v, requestId=%s", clientId, consumerGroup, messageId, dpq.mqstr, endpoints,
		requestId)
}

func (dpq *defaultProcessQueue) changeInvisibleDurationLater(mv *MessageView, duration time.Duration, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	messageId := mv.messageId
	time.AfterFunc(CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY, func() {
		defer func() {
			if err := recover(); err != nil {
				dpq.consumer.cli.log.Errorf("[Bug] Failed to schedule message change invisible duration request, mq=%s, messageId=%s, "+
					"clientId=%s", dpq.mqstr, messageId, clientId)
				dpq.changeInvisibleDurationLater(mv, duration, 1+attempt, callback)
			}
		}()
		dpq.changeInvisibleDuration(mv, duration, attempt, callback)
	})
}

func (dpq *defaultProcessQueue) ackMessage(mv *MessageView, callback func(error)) {
	dpq.ackMessage0(mv, 1, callback)
}

func (dpq *defaultProcessQueue) ackMessage0(mv *MessageView, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	consumerGroup := dpq.consumer.groupName
	messageId := mv.messageId
	endpoints := mv.endpoints

	ctx := context.Background()
	resp, err := dpq.consumer.ack0(ctx, mv)
	if err != nil {
		dpq.consumer.cli.log.Errorf("Exception raised while acknowledging message, clientId=%s, consumerGroup=%s, "+
			"would attempt to re-ack later, attempt=%d, messageId=%s, mq=%s, endpoints=%v, err=%w", clientId,
			consumerGroup, attempt, messageId, dpq.mqstr, endpoints, err)
		dpq.ackMessageLater(mv, 1+attempt, callback)
		return
	}
	requestId := utils.GetRequestID(ctx)
	status := resp.GetStatus()
	code := status.GetCode()
	if code == v2.Code_INVALID_RECEIPT_HANDLE {
		dpq.consumer.cli.log.Errorf("Failed to ack message due to the invalid receipt handle, forgive to retry, "+
			"clientId=%s, consumerGroup=%s, messageId=%s, attempt=%d, mq=%s, endpoints=%v, "+
			"requestId=%s, status message=[%s]", clientId, consumerGroup, messageId, attempt, dpq.mqstr,
			endpoints, requestId, status.GetMessage())
		callback(&ErrRpcStatus{Code: int32(v2.Code_BAD_REQUEST), Message: code.String()})
		return
	}
	// Log failure and retry later.
	if code != v2.Code_OK {
		dpq.consumer.cli.log.Errorf("Failed to ack message, would attempt to re-ack later, "+
			" clientId=%s, consumerGroup=%s, messageId=%s, attempt=%d, mq=%s, endpoints=%v, requestId=%s, status message=[%s]", clientId, consumerGroup, messageId, attempt, dpq.mqstr,
			endpoints, requestId, status.GetMessage())
		dpq.ackMessageLater(mv, 1+attempt, callback)
	}
	// Set result if succeed in changing invisible time.
	callback(nil)
	// Log retries.
	if attempt > 1 {
		dpq.consumer.cli.log.Infof("Finally, ack message successfully, clientId=%s, consumerGroup=%s "+
			"messageId={}, attempt={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
			messageId, attempt, dpq.mqstr, endpoints, requestId)
		return
	}
	dpq.consumer.cli.log.Debugf("Ack message successfully, clientId=%s, consumerGroup=%s, messageId=%s, "+
		"mq=%s, endpoints=%v, requestId=%s", clientId, consumerGroup, messageId, dpq.mqstr, endpoints,
		requestId)
}

func (dpq *defaultProcessQueue) ackMessageLater(mv *MessageView, attempt int, callback func(error)) {
	clientId := dpq.consumer.cli.clientID
	messageId := mv.messageId
	time.AfterFunc(ACK_MESSAGE_FAILURE_BACKOFF_DELAY, func() {
		defer func() {
			if err := recover(); err != nil {
				dpq.consumer.cli.log.Errorf("[Bug] Failed to schedule message change invisible duration request, mq=%s, messageId=%s, "+
					"clientId=%s", dpq.mqstr, messageId, clientId)
				dpq.ackMessageLater(mv, 1+attempt, callback)
			}
		}()
		dpq.ackMessage0(mv, attempt, callback)
	})
}

func (dpq *defaultProcessQueue) getMessageQueue() *v2.MessageQueue {
	return dpq.mq
}

func newDefaultProcessQueue(dpc *defaultPushConsumer, mqstr utils.MessageQueueStr, mq *v2.MessageQueue, fe *FilterExpression) *defaultProcessQueue {
	return &defaultProcessQueue{
		consumer:          dpc,
		dropped:           *atomic.NewBool(false),
		mqstr:             mqstr,
		mq:                mq,
		filterExpression:  fe,
		activityNanoTime:  *atomic.NewInt64(time.Now().UnixNano()),
		cacheFullNanoTime: *atomic.NewInt64(math.MinInt64),
	}
}

func (dpq *defaultProcessQueue) drop() {
	dpq.dropped.Store(true)
}

func (dpq *defaultProcessQueue) expired() bool {
	longPollingTimeout := dpq.consumer.pcSettings.longPollingTimeout
	requestTimeout := dpq.consumer.pcSettings.requestTimeout
	maxIdleDuration := ((longPollingTimeout + requestTimeout) * 3).Nanoseconds()
	idleDuration := time.Now().UnixNano() - dpq.activityNanoTime.Load()
	if idleDuration < maxIdleDuration {
		return false
	}
	afterCacheFullDuration := time.Now().UnixNano() - dpq.cacheFullNanoTime.Load()
	if afterCacheFullDuration < maxIdleDuration {
		return false
	}
	dpq.consumer.cli.log.Warnf("Process queue is idle, idleDuration=%d, maxIdleDuration=%d, afterCacheFullDuration=%d, mq=%s, clientId=%s", idleDuration, maxIdleDuration, afterCacheFullDuration, dpq.mqstr, dpq.consumer.cli.clientID)
	return true
}

var _ = ProcessQueue(&defaultProcessQueue{})

func (dpq *defaultProcessQueue) fetchMessageImmediately() {
	dpq.receiveMessageImmediately()
}

func (dpq *defaultProcessQueue) receiveMessage() {
	dpq.receiveMessageWithAttemptId(uuid.New().String())
}

func (dpq *defaultProcessQueue) receiveMessageWithAttemptId(attemptId string) {
	clientId := dpq.consumer.cli.clientID
	if dpq.dropped.Load() {
		dpq.consumer.cli.log.Infof("Process queue has been dropped, no longer receive message, mq=%s, clientId=%s", dpq.mqstr, clientId)
		return
	}
	if dpq.isCacheFull() {
		dpq.consumer.cli.log.Warnf("Process queue cache is full, would receive message later, mq=%s, clientId=%s", dpq.mqstr, clientId)
		dpq.receiveMessageLater(RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL, attemptId)
		return
	}
	dpq.receiveMessageImmediatelyWithAttemptId(attemptId)
}

func (dpq *defaultProcessQueue) isCacheFull() bool {
	clientId := dpq.consumer.cli.clientID

	cacheMessageCountThresholdPerQueue := dpq.consumer.cacheMessageCountThresholdPerQueue()
	actualMessagesQuantity := dpq.cachedMessagesNums.Load()
	if cacheMessageCountThresholdPerQueue <= actualMessagesQuantity {
		dpq.consumer.cli.log.Warnf("Process queue total cached messages quantity exceeds the threshold, threshold=%d, actual=%d, mq=%s, clientId=%s", cacheMessageCountThresholdPerQueue, actualMessagesQuantity, dpq.mqstr, clientId)
		dpq.cacheFullNanoTime.Store(time.Now().UnixNano())
		return true
	}

	cacheMessageBytesThresholdPerQueue := int64(dpq.consumer.cacheMessageBytesThresholdPerQueue())
	actualCachedMessagesBytes := dpq.cachedMessagesBytes.Load()
	if cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes {
		dpq.consumer.cli.log.Warnf("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, actual={} bytes, mq={}, clientId={}",
			cacheMessageBytesThresholdPerQueue, actualCachedMessagesBytes, dpq.mqstr, clientId)
		dpq.cacheFullNanoTime.Store(time.Now().UnixNano())
		return true
	}

	return false
}

func (dpq *defaultProcessQueue) receiveMessageImmediately() {
	dpq.receiveMessageImmediatelyWithAttemptId(uuid.New().String())
}

func (dpq *defaultProcessQueue) receiveMessageImmediatelyWithAttemptId(attemptId string) {
	clientId := dpq.consumer.cli.clientID
	if !dpq.consumer.isRunning() {
		dpq.consumer.cli.log.Infof("Stop to receive message because consumer is not running, mq=%s, clientId=%s", dpq.mqstr, clientId)
		return
	}

	defer func() {
		if err := recover(); err != nil {
			dpq.consumer.cli.log.Errorf("Exception raised during message reception, mq=%s, clientId=%s, err=%v", dpq.mqstr, clientId, err)
			dpq.onReceiveMessageException(err, attemptId)
		}
	}()

	endpoints := dpq.mq.Broker.Endpoints
	batchSize := dpq.getReceptionBatchSize()
	longPollingTimeout := dpq.consumer.pcSettings.longPollingTimeout
	request := dpq.consumer.wrapReceiveMessageRequest(int(batchSize), dpq.mq, dpq.filterExpression, longPollingTimeout)

	startTime := time.Now()
	dpq.activityNanoTime.Store(startTime.UnixNano())

	// Intercept before message reception.
	dpq.consumer.cli.doBefore(MessageHookPoints_RECEIVE, make([]*MessageCommon, 0))

	timeout := longPollingTimeout + dpq.consumer.cli.opts.timeout
	go func() {
		mvs, err := dpq.consumer.receiveMessage(context.TODO(), request, dpq.mq, timeout)
		duration := time.Since(startTime)
		if err == nil {
			messageCommons := make([]*MessageCommon, 0, len(mvs))
			for _, mv := range mvs {
				messageCommons = append(messageCommons, mv.GetMessageCommon())
			}
			dpq.consumer.cli.doAfter(MessageHookPoints_RECEIVE, messageCommons, duration, MessageHookPointsStatus_OK)
			dpq.onReceiveMessageResult(mvs)
		} else {
			nextAttemptId := ""
			if status.Code(err) == codes.DeadlineExceeded {
				nextAttemptId = request.GetAttemptId()
			}
			dpq.consumer.cli.doAfter(MessageHookPoints_RECEIVE, make([]*MessageCommon, 0), duration, MessageHookPointsStatus_ERROR)

			rpcError, ok := AsErrRpcStatus(err)
			if ok && rpcError.GetCode() == int32(v2.Code_MESSAGE_NOT_FOUND) {
				dpq.consumer.cli.log.Infof("latest message not found, mq=%s, endpoints=%v, attemptId=%s, "+
					"nextAttemptId=%s, clientId=%s, err=%v", dpq.mqstr, endpoints, request.GetAttemptId(), nextAttemptId,
					clientId, err)
			} else {
				dpq.consumer.cli.log.Errorf("Exception raised during message reception, mq=%s, endpoints=%v, attemptId=%s, "+
					"nextAttemptId=%s, clientId=%s, err=%v", dpq.mqstr, endpoints, request.GetAttemptId(), nextAttemptId,
					clientId, err)
			}

			dpq.onReceiveMessageException(err, nextAttemptId)
		}
	}()
	dpq.receptionTimes.Inc()
	dpq.consumer.receptionTimes.Inc()

}

func (dpq *defaultProcessQueue) getReceptionBatchSize() int32 {
	bufferSize := float64(dpq.consumer.cacheMessageCountThresholdPerQueue() - dpq.cachedMessagesNums.Load())
	bufferSize = math.Max(float64(bufferSize), 1)
	return int32(math.Min(bufferSize, float64(dpq.consumer.pcSettings.receiveBatchSize)))
}

func (dpq *defaultProcessQueue) onReceiveMessageException(t any, attemptId string) {
	duration := RECEIVING_FLOW_CONTROL_BACKOFF_DELAY
	error, ok := t.(error)
	if ok {
		rpcError, ok := AsErrRpcStatus(error)
		if ok && rpcError.GetCode() == int32(v2.Code_TOO_MANY_REQUESTS) {
			duration = RECEIVING_FAILURE_BACKOFF_DELAY
		}
	}
	dpq.receiveMessageLater(duration, attemptId)
}

func (dpq *defaultProcessQueue) receiveMessageLater(duration time.Duration, attemptId string) {
	time.AfterFunc(duration, func() {
		defer func() {
			if err := recover(); err != nil {
				dpq.consumer.cli.log.Errorf("[Bug] Failed to schedule message receiving request, mq=%s, clientId=%s, err=%v", dpq.mqstr, dpq.consumer.cli.clientID, err)
				dpq.onReceiveMessageException(err, attemptId)
			}
		}()
		dpq.consumer.cli.log.Infof("Try to receive message later, mq=%s, delay=%v, clientId=%s", dpq.mqstr, duration, dpq.consumer.cli.clientID)
		dpq.receiveMessageImmediatelyWithAttemptId(attemptId)
	})
}

func (dpq *defaultProcessQueue) onReceiveMessageResult(mvs []*MessageView) {
	mvslen := int64(len(mvs))
	if mvslen != 0 {
		dpq.cacheMessages(mvs)
		dpq.receivedMessagesQuantity.Add(mvslen)
		dpq.consumer.receivedMessagesQuantity.Add(mvslen)
		dpq.consumer.consumerService.consume(dpq, mvs)
	}
	dpq.receiveMessage()
}

func (dpq *defaultProcessQueue) cacheMessages(mvs []*MessageView) {
	for _, mv := range mvs {
		dpq.cachedMessagesNums.Inc()
		dpq.cachedMessagesBytes.Add(int64(len(mv.body)))
	}
}

func (dpq *defaultProcessQueue) evictCacheMessage(mv *MessageView) {
	dpq.cachedMessagesNums.Dec()
	dpq.cachedMessagesBytes.Sub(int64(len(mv.body)))
}
