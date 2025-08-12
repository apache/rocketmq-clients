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
	"io"
	"math"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/ticker"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PushConsumer interface {
	Consumer

	Start() error
	GracefulStop() error

	Subscribe(topic string, filterExpression *FilterExpression) error
	Unsubscribe(topic string) error
	Ack(ctx context.Context, messageView *MessageView) error
	ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error
	ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration)
}

var _ = PushConsumer(&defaultPushConsumer{})

type defaultPushConsumer struct {
	cli *defaultClient

	groupName                    string
	pcOpts                       pushConsumerOptions
	pcSettings                   *pushConsumerSettings
	awaitDuration                time.Duration
	subscriptionExpressions      *sync.Map
	subTopicRouteDataResultCache *sync.Map
	cacheAssignments             *sync.Map
	processQueueTable            *sync.Map
	consumerService              ConsumeService
	receptionTimes               atomic.Int64
	receivedMessagesQuantity     atomic.Int64

	consumptionOkQuantity    atomic.Int64
	consumptionErrorQuantity atomic.Int64

	stopping                        atomic.Bool
	inflightRequestCountInterceptor *defultInflightRequestCountInterceptor
}

func (pc *defaultPushConsumer) SetRequestTimeout(timeout time.Duration) {
	pc.cli.opts.timeout = timeout
	pc.pcSettings.requestTimeout = pc.cli.opts.timeout
}

func (pc *defaultPushConsumer) isOn() bool {
	return pc.cli.on.Load()
}

func (pc *defaultPushConsumer) changeInvisibleDuration0(context context.Context, messageView *MessageView, invisibleDuration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	endpoints := messageView.endpoints
	if endpoints == nil {
		return nil, fmt.Errorf("changeInvisibleDuration failed, err = the endpoints in message is nil")
	}
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	pc.cli.doBefore(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons)

	ctx := pc.cli.Sign(context)
	request := &v2.ChangeInvisibleDurationRequest{
		Topic: &v2.Resource{
			Name:              messageView.GetTopic(),
			ResourceNamespace: pc.cli.config.NameSpace,
		},
		Group: &v2.Resource{
			Name:              pc.groupName,
			ResourceNamespace: pc.cli.config.NameSpace,
		},
		ReceiptHandle:     messageView.GetReceiptHandle(),
		InvisibleDuration: durationpb.New(invisibleDuration),
		MessageId:         messageView.GetMessageId(),
	}
	watchTime := time.Now()
	resp, err := pc.cli.clientManager.ChangeInvisibleDuration(ctx, endpoints, request, pc.pcSettings.requestTimeout)
	duration := time.Since(watchTime)
	messageHookPointsStatus := MessageHookPointsStatus_OK
	if err != nil {
		pc.cli.log.Errorf("exception raised during message acknowledgement, messageId=%s, endpoints=%v, requestId=%s", messageView.GetMessageId(), endpoints, utils.GetRequestID(ctx))
	} else if resp.GetStatus().GetCode() != v2.Code_OK {
		pc.cli.log.Errorf("failed to change message invisible duration, messageId=%s, endpoints=%v, code=%v, status message=[%s], requestId=%s", messageView.GetMessageId(), endpoints, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage(), utils.GetRequestID(ctx))
		err = &ErrRpcStatus{
			Code:    int32(resp.Status.GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	if err != nil {
		messageHookPointsStatus = MessageHookPointsStatus_ERROR
	}
	pc.cli.doAfter(MessageHookPoints_CHANGE_INVISIBLE_DURATION, messageCommons, duration, messageHookPointsStatus)
	return resp, err
}

func (pc *defaultPushConsumer) changeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if messageView == nil {
		return fmt.Errorf("changeInvisibleDuration failed, err = the message is nil")
	}
	resp, err := pc.changeInvisibleDuration0(context.Background(), messageView, invisibleDuration)
	if err == nil && resp != nil {
		messageView.ReceiptHandle = resp.ReceiptHandle
	}
	return err
}

func (pc *defaultPushConsumer) ChangeInvisibleDuration(messageView *MessageView, invisibleDuration time.Duration) error {
	if !pc.isOn() {
		return fmt.Errorf("simple consumer is not running")
	}
	return pc.changeInvisibleDuration(messageView, invisibleDuration)
}

func (pc *defaultPushConsumer) ChangeInvisibleDurationAsync(messageView *MessageView, invisibleDuration time.Duration) {
	if !pc.isOn() {
		sugarBaseLogger.Errorf("simple consumer is not running")
		return
	}
	go func() {
		pc.changeInvisibleDuration(messageView, invisibleDuration)
	}()
}

func (pc *defaultPushConsumer) Subscribe(topic string, filterExpression *FilterExpression) error {
	_, err := pc.cli.getMessageQueues(context.Background(), topic)
	if err != nil {
		pc.cli.log.Errorf("subscribe error=%v with topic %s for pushConsumer", err, topic)
		return err
	}
	pc.subscriptionExpressions.Store(topic, filterExpression)
	return nil
}

func (pc *defaultPushConsumer) Unsubscribe(topic string) error {
	pc.subscriptionExpressions.Delete(topic)
	return nil
}

func (pc *defaultPushConsumer) wrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, longPollingTimeout time.Duration) *v2.ReceiveMessageRequest {
	return pc.wrapReceiveMessageRequestWithAttemptId(batchSize, messageQueue, filterExpression, longPollingTimeout, "")
}

func (pc *defaultPushConsumer) wrapReceiveMessageRequestWithAttemptId(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, longPollingTimeout time.Duration, attemptId string) *v2.ReceiveMessageRequest {
	if len(attemptId) == 0 {
		attemptId = uuid.New().String()
	}
	var filterType v2.FilterType
	switch filterExpression.expressionType {
	case SQL92:
		filterType = v2.FilterType_SQL
	case TAG:
		filterType = v2.FilterType_TAG
	default:
		filterType = v2.FilterType_FILTER_TYPE_UNSPECIFIED
	}

	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name:              pc.groupName,
			ResourceNamespace: pc.cli.config.NameSpace,
		},
		MessageQueue: messageQueue,
		FilterExpression: &v2.FilterExpression{
			Expression: filterExpression.expression,
			Type:       filterType,
		},
		LongPollingTimeout: durationpb.New(longPollingTimeout),
		BatchSize:          int32(batchSize),
		AutoRenew:          true,
		AttemptId:          &attemptId,
	}
}

func (pc *defaultPushConsumer) wrapAckMessageRequest(messageView *MessageView) *v2.AckMessageRequest {
	return &v2.AckMessageRequest{
		Group: pc.pcSettings.groupName,
		Topic: &v2.Resource{
			Name:              messageView.GetTopic(),
			ResourceNamespace: pc.cli.config.NameSpace,
		},
		Entries: []*v2.AckMessageEntry{
			{
				MessageId:     messageView.GetMessageId(),
				ReceiptHandle: messageView.GetReceiptHandle(),
			},
		},
	}
}

func (pc *defaultPushConsumer) GetGroupName() string {
	return pc.groupName
}

func (pc *defaultPushConsumer) receiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest, messageQueue *v2.MessageQueue, timeout time.Duration) ([]*MessageView, error) {
	var err error
	ctx = pc.cli.Sign(ctx)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	endpoints := messageQueue.GetBroker().GetEndpoints()
	receiveMessageClient, err := pc.cli.clientManager.ReceiveMessage(ctx, endpoints, request)
	if err != nil {
		return nil, err
	}
	done := make(chan bool, 1)

	resps := make([]*v2.ReceiveMessageResponse, 0)
	go func() {
		for {
			var resp *v2.ReceiveMessageResponse
			resp, err = receiveMessageClient.Recv()
			if err == io.EOF {
				done <- true
				defer close(done)
				break
			}
			if err != nil {
				pc.cli.log.Errorf("pushConsumer recv msg err=%v, requestId=%s", err, utils.GetRequestID(ctx))
				break
			}
			sugarBaseLogger.Debugf("receiveMessage response: %v", resp)
			resps = append(resps, resp)
		}
		cancel()
	}()
	select {
	case <-ctx.Done():
		// timeout
		return nil, fmt.Errorf("[error] CODE=DEADLINE_EXCEEDED")
	case <-done:
		if err != nil && err != io.EOF {
			return nil, err
		}
		messageViewList := make([]*MessageView, 0)
		status := &v2.Status{
			Code:    v2.Code_INTERNAL_SERVER_ERROR,
			Message: "status was not set by server",
		}
		var deliveryTimestamp *timestamppb.Timestamp
		messageList := make([]*v2.Message, 0)
		for _, resp := range resps {
			switch r := resp.GetContent().(type) {
			case *v2.ReceiveMessageResponse_Status:
				status = r.Status
			case *v2.ReceiveMessageResponse_Message:
				messageList = append(messageList, r.Message)
			case *v2.ReceiveMessageResponse_DeliveryTimestamp:
				deliveryTimestamp = r.DeliveryTimestamp
			default:
				pc.cli.log.Warnf("[bug] not recognized content for receive message response, mq=%v, resp=%v", messageQueue, resp)
			}
		}
		for _, message := range messageList {
			messageView := fromProtobuf_MessageView2(message, messageQueue, deliveryTimestamp)
			messageViewList = append(messageViewList, messageView)
		}
		if status.GetCode() == v2.Code_OK {
			return messageViewList, nil
		} else {
			return nil, &ErrRpcStatus{
				Code:    int32(status.GetCode()),
				Message: status.GetMessage(),
			}
		}
	}
}

func (pc *defaultPushConsumer) isClient() {

}

func (pc *defaultPushConsumer) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	return fmt.Errorf("ignore orphaned transaction recovery command from remote, which is not expected, client id=%s, command=%v", pc.cli.clientID, command)
}

func (pc *defaultPushConsumer) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	return nil
}

func (pc *defaultPushConsumer) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group:      pc.pcSettings.groupName,
		ClientType: v2.ClientType_SIMPLE_CONSUMER,
	}
}

var NewPushConsumer = func(config *Config, opts ...PushConsumerOption) (PushConsumer, error) {
	copyOpt := defaultPushConsumerOptions
	pcOpts := &copyOpt
	for _, opt := range opts {
		opt.apply(pcOpts)
	}
	if len(config.ConsumerGroup) == 0 {
		return nil, fmt.Errorf("consumerGroup could not be nil")
	}
	if pcOpts.messageListener == nil {
		return nil, fmt.Errorf("messageListener could not be nil")
	}
	if utils.CountSyncMapSize(pcOpts.subscriptionExpressions) == 0 {
		return nil, fmt.Errorf("subscriptionExpressions have not been set yet")
	}
	cli, err := pcOpts.clientFunc(config)
	if err != nil {
		return nil, err
	}
	if pcOpts.subscriptionExpressions == nil {
		pcOpts.subscriptionExpressions = &sync.Map{}
	}
	pc := &defaultPushConsumer{
		pcOpts:    *pcOpts,
		cli:       cli.(*defaultClient),
		groupName: config.ConsumerGroup,

		awaitDuration:           pcOpts.awaitDuration,
		subscriptionExpressions: pcOpts.subscriptionExpressions,

		subTopicRouteDataResultCache:    &sync.Map{},
		cacheAssignments:                &sync.Map{},
		processQueueTable:               &sync.Map{},
		stopping:                        *atomic.NewBool(false),
		inflightRequestCountInterceptor: NewDefultInflightRequestCountInterceptor(),
	}
	pc.cli.initTopics = make([]string, 0)
	pcOpts.subscriptionExpressions.Range(func(key, value interface{}) bool {
		pc.cli.initTopics = append(pc.cli.initTopics, key.(string))
		return true
	})
	endpoints, err := utils.ParseTarget(config.Endpoint)
	if err != nil {
		return nil, err
	}
	pc.pcSettings = &pushConsumerSettings{
		clientId:       pc.cli.GetClientID(),
		endpoints:      endpoints,
		clientType:     v2.ClientType_PUSH_CONSUMER,
		requestTimeout: pc.cli.opts.timeout,

		retryPolicy: &v2.RetryPolicy{
			MaxAttempts: 16,
		},
		isFifo:           false,
		receiveBatchSize: 32,

		groupName: &v2.Resource{
			Name:              pc.groupName,
			ResourceNamespace: config.NameSpace,
		},
		longPollingTimeout:      time.Second * 30,
		subscriptionExpressions: pcOpts.subscriptionExpressions,
	}
	pc.cli.settings = pc.pcSettings
	pc.cli.clientImpl = pc
	pc.cli.registerMessageInterceptor(pc.inflightRequestCountInterceptor)
	return pc, nil
}

func (pc *defaultPushConsumer) Start() error {
	err := pc.cli.startUp()

	threadPool := NewSimpleThreadPool("MessageConsumption", int(pc.pcOpts.maxCacheMessageCount), int(pc.pcOpts.consumptionThreadCount))
	if pc.pcSettings.isFifo {
		pc.consumerService = NewFiFoConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, threadPool, pc.cli)
		pc.cli.log.Infof("Create FIFO consume service, consumerGroup=%s, clientId=%s", pc.cli.config.ConsumerGroup, pc.cli.clientID)
	} else {
		pc.consumerService = NewStandardConsumeService(pc.cli.clientID, pc.pcOpts.messageListener, threadPool, pc.cli)
		pc.cli.log.Infof("Create standard consume service, consumerGroup=%s, clientId=%s", pc.cli.config.ConsumerGroup, pc.cli.clientID)
	}

	if err == nil {
		go func() {
			time.Sleep(time.Second)
			pc.scanAssignments()
			ticker.Tick(pc.scanAssignments, 5*time.Second, pc.cli.done)
		}()
		return nil
	}
	err2 := pc.GracefulStop()
	if err2 != nil {
		return fmt.Errorf("startUp err=%w, shutdown err=%v", err, err2)
	}
	return err
}

func (pc *defaultPushConsumer) scanAssignments() {
	// When stopping in progress, return directly
	if pc.stopping.Load() {
		return
	}
	pc.subscriptionExpressions.Range(func(key, value interface{}) bool {
		topic := key.(string)
		filterExpression := value.(*FilterExpression)
		newest, err := pc.cli.queryAssignments(context.TODO(), topic, pc.groupName, pc.cli.opts.timeout)
		if err != nil {
			pc.cli.log.Errorf("Exception raised while scanning the assignments, topic=%s, clientId=%s, err=%v",
				topic, pc.cli.clientID, err)
		}
		val, _ := pc.cacheAssignments.Load(topic)
		var existed *[]*v2.Assignment
		if val != nil {
			existed = val.(*[]*v2.Assignment)
		}
		if utils.IsAssignmentsEmpty(newest) {
			if utils.IsAssignmentsEmpty(existed) {
				pc.cli.log.Infof("Acquired empty assignments from remote, would scan later, topic=%s, clientId=%s", topic, pc.cli.clientID)
				return true
			}
			pc.cli.log.Infof("Attention!!! acquired empty assignments from remote, but existed assignments is not empty, topic=%s, clientId=%d",
				topic, pc.cli.clientID)
		}
		if !utils.CompareAssignments(newest, existed) {
			pc.cli.log.Infof("Assignments of topic=%s has changed, %v => %v, clientId=%s", topic, existed,
				newest, pc.cli.clientID)
			pc.syncProcessQueue(topic, newest, filterExpression)
			pc.cacheAssignments.Store(topic, newest)
			return true
		}
		pc.cli.log.Debugf("Assignments of topic=%s remains the same, assignments=%v, clientId=%s", topic,
			existed, pc.cli.clientID)
		pc.syncProcessQueue(topic, newest, filterExpression)
		return true
	})
}

func (pc *defaultPushConsumer) syncProcessQueue(topic string, assignments *[]*v2.Assignment, filterExpression *FilterExpression) {
	latest := make(map[utils.MessageQueueStr]*v2.MessageQueue)
	if assignments != nil {
		for _, a := range *assignments {
			latest[utils.ParseMessageQueue2Str(a.MessageQueue)] = a.MessageQueue
		}
	}
	activeMqs := make(map[utils.MessageQueueStr]*v2.MessageQueue)
	pc.processQueueTable.Range(func(key, value interface{}) bool {
		messageQueueStr := key.(utils.MessageQueueStr)
		pair := value.([]interface{})
		messageQueue := pair[0].(*v2.MessageQueue)
		processQueue := pair[1].(ProcessQueue)
		if topic != messageQueue.Topic.Name {
			return true
		}
		if _, ok := latest[messageQueueStr]; !ok {
			pc.cli.log.Infof("Drop message queue according to the latest assignmentList, mq=%s, clientId=%s", messageQueueStr, pc.cli.clientID)
			pc.dropProcessQueue(messageQueueStr)
			return true
		}
		if processQueue.expired() {
			pc.cli.log.Warnf("Drop message queue because it is expired, mq=%s, clientId=%s", messageQueueStr, pc.cli.clientID)
			pc.dropProcessQueue(messageQueueStr)
			return true
		}
		activeMqs[messageQueueStr] = messageQueue
		return true
	})
	for mqs, mq := range latest {
		if _, ok := activeMqs[mqs]; ok {
			continue
		}
		optionalProcessQueue := pc.createProcessQueue(mqs, mq, filterExpression)
		if optionalProcessQueue != nil {
			pc.cli.log.Infof("Start to fetch message from remote, mq=%s, clientId={}", mqs, pc.cli.clientID)
			optionalProcessQueue.fetchMessageImmediately()
		}
	}
}
func (pc *defaultPushConsumer) createProcessQueue(mqstr utils.MessageQueueStr, mq *v2.MessageQueue, fe *FilterExpression) ProcessQueue {
	pq := newDefaultProcessQueue(pc, mqstr, mq, fe)
	_, existed := pc.processQueueTable.LoadOrStore(mqstr, []interface{}{mq, pq})
	if existed {
		return nil
	}
	return pq
}
func (pc *defaultPushConsumer) dropProcessQueue(mqstr utils.MessageQueueStr) {
	v, _ := pc.processQueueTable.LoadAndDelete(mqstr)
	if v != nil {
		if v2, ok := v.([]interface{}); ok {
			v2[1].(ProcessQueue).drop()
		}
	}
}

/**
 * PushConsumerImpl shutdown order
 * 1. when begin shutdown, do not send any new receive request
 * 2. cancel scanAssignmentsFuture, do not create new processQueue
 * 3. waiting all inflight receive request finished or timeout
 * 4. shutdown consumptionExecutor and waiting all message consumption finished
 * 5. sleep 1s to ack message async
 * 6. shutdown clientImpl
 */
func (pc *defaultPushConsumer) GracefulStop() error {
	// step 1 and 2
	pc.stopping.Store(true)

	// step 3
	pc.cli.log.Infof("Waiting for the inflight receive requests to be finished, clientId=%s", pc.cli.clientID)
	pc.waitingReceiveRequestFinished()
	pc.cli.log.Infof("Begin to Shutdown consumption executor, clientId=%s", pc.cli.clientID)

	// step 4
	pc.consumerService.Shutdown()

	// step 5
	time.Sleep(time.Second)

	// step 6
	return pc.cli.GracefulStop()
}

func (pc *defaultPushConsumer) waitingReceiveRequestFinished() error {
	maxWaitingTime := pc.pcSettings.GetRequestTimeout() + pc.pcSettings.longPollingTimeout
	endTime := time.Now().Add(maxWaitingTime)
	defer func() {
		if err := recover(); err != nil {
			pc.cli.log.Errorf("Unexpected exception while waiting for the inflight receive requests to be finished, "+
				"clientId=%s, err=%v", pc.cli.clientID, err)
		}
	}()

	for {
		inflightReceiveRequestCount := pc.inflightRequestCountInterceptor.getInflightReceiveRequestCount()
		if inflightReceiveRequestCount <= 0 {
			pc.cli.log.Infof("All inflight receive requests have been finished, clientId=%s", pc.cli.clientID)
			break
		} else if time.Now().After(endTime) {
			pc.cli.log.Warnf("Timeout waiting for all inflight receive requests to be finished, clientId=%s, "+
				"inflightReceiveRequestCount=%d", pc.cli.clientID, inflightReceiveRequestCount)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (pc *defaultPushConsumer) getSubscriptionTopicRouteResult(ctx context.Context, topic string) (SubscriptionLoadBalancer, error) {
	item, ok := pc.subTopicRouteDataResultCache.Load(topic)
	if ok {
		if ret, ok := item.(SubscriptionLoadBalancer); ok {
			return ret, nil
		}
	}
	route, err := pc.cli.getMessageQueues(ctx, topic)
	if err != nil {
		return nil, err
	}
	slb, err := NewSubscriptionLoadBalancer(route)
	if err != nil {
		return nil, err
	}
	pc.subTopicRouteDataResultCache.Store(topic, slb)
	return slb, nil
}

// Ack implements PushConsumer
func (pc *defaultPushConsumer) Ack(ctx context.Context, messageView *MessageView) error {
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	pc.cli.doBefore(MessageHookPoints_ACK, messageCommons)

	watchTime := time.Now()
	resp, err := pc.ack0(ctx, messageView)
	duration := time.Since(watchTime)

	messageHookPointsStatus := MessageHookPointsStatus_ERROR
	if err != nil {
		pc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
		return err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		messageHookPointsStatus = MessageHookPointsStatus_OK
	}
	pc.cli.doAfter(MessageHookPoints_ACK, messageCommons, duration, messageHookPointsStatus)
	return nil
}

func (pc *defaultPushConsumer) ack0(ctx context.Context, messageView *MessageView) (*v2.AckMessageResponse, error) {
	if !pc.isOn() {
		return nil, fmt.Errorf("push consumer is not running")
	}
	endpoints := messageView.endpoints
	request := pc.wrapAckMessageRequest(messageView)
	ctx = pc.cli.Sign(ctx)
	return pc.cli.clientManager.AckMessage(ctx, endpoints, request, pc.cli.opts.timeout)
}

func (pc *defaultPushConsumer) wrapForwardMessageToDeadLetterQueueRequest(messageView *MessageView) *v2.ForwardMessageToDeadLetterQueueRequest {
	return &v2.ForwardMessageToDeadLetterQueueRequest{
		Group: pc.pcSettings.groupName,
		Topic: &v2.Resource{
			Name:              messageView.GetTopic(),
			ResourceNamespace: pc.cli.config.NameSpace,
		},
		ReceiptHandle:       messageView.GetReceiptHandle(),
		MessageId:           messageView.GetMessageId(),
		DeliveryAttempt:     messageView.GetDeliveryAttempt(),
		MaxDeliveryAttempts: pc.pcSettings.GetRetryPolicy().MaxAttempts,
	}
}

func (pc *defaultPushConsumer) ForwardMessageToDeadLetterQueue(ctx context.Context, messageView *MessageView) error {
	messageCommons := []*MessageCommon{messageView.GetMessageCommon()}
	pc.cli.doBefore(MessageHookPoints_FORWARD_TO_DLQ, messageCommons)

	watchTime := time.Now()
	resp, err := pc.forwardMessageToDeadLetterQueue0(ctx, messageView)
	duration := time.Since(watchTime)

	messageHookPointsStatus := MessageHookPointsStatus_ERROR
	if err != nil {
		pc.cli.doAfter(MessageHookPoints_FORWARD_TO_DLQ, messageCommons, duration, messageHookPointsStatus)
		return err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		messageHookPointsStatus = MessageHookPointsStatus_OK
	}
	pc.cli.doAfter(MessageHookPoints_FORWARD_TO_DLQ, messageCommons, duration, messageHookPointsStatus)
	return nil
}

func (pc *defaultPushConsumer) forwardMessageToDeadLetterQueue0(ctx context.Context, messageView *MessageView) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	endpoints := messageView.endpoints
	request := pc.wrapForwardMessageToDeadLetterQueueRequest(messageView)
	ctx = pc.cli.Sign(ctx)
	return pc.cli.clientManager.ForwardMessageToDeadLetterQueue(ctx, endpoints, request, pc.cli.opts.timeout)
}

func (pc *defaultPushConsumer) isRunning() bool {
	// graceful stop in pushConsumer
	if pc.stopping.Load() {
		return false
	}
	return pc.cli.isRunning()
}

func (pc *defaultPushConsumer) getQueueSize() int32 {
	return utils.CountSyncMapSize(pc.processQueueTable)
}

func (pc *defaultPushConsumer) cacheMessageCountThresholdPerQueue() int32 {
	size := pc.getQueueSize()
	// All process queues are removed, no need to cache messages.
	if size <= 0 {
		return 0
	}
	return int32(math.Max(1, float64(pc.pcOpts.maxCacheMessageCount)/float64(size)))
}

func (pc *defaultPushConsumer) cacheMessageBytesThresholdPerQueue() int64 {
	size := pc.getQueueSize()
	// All process queues are removed, no need to cache messages.
	if size <= 0 {
		return 0
	}
	return int64(math.Max(1, float64(pc.pcOpts.maxCacheMessageSizeInBytes)/float64(size)))
}

type defultInflightRequestCountInterceptor struct {
	inflightReceiveRequestCount atomic.Int64
}

var _ = MessageInterceptor(&defultInflightRequestCountInterceptor{})

var NewDefultInflightRequestCountInterceptor = func() *defultInflightRequestCountInterceptor {
	return &defultInflightRequestCountInterceptor{
		inflightReceiveRequestCount: *atomic.NewInt64(0),
	}
}

func (dirci *defultInflightRequestCountInterceptor) doBefore(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon) error {
	if messageHookPoints == MessageHookPoints_RECEIVE {
		dirci.inflightReceiveRequestCount.Inc()
	}
	return nil
}

func (dirci *defultInflightRequestCountInterceptor) doAfter(messageHookPoints MessageHookPoints, messageCommons []*MessageCommon, duration time.Duration, status MessageHookPointsStatus) error {
	if messageHookPoints == MessageHookPoints_RECEIVE {
		dirci.inflightReceiveRequestCount.Dec()
	}
	return nil
}

func (dirci *defultInflightRequestCountInterceptor) getInflightReceiveRequestCount() int64 {
	return dirci.inflightReceiveRequestCount.Load()
}

func (pc *defaultPushConsumer) IsEndpointUpdated() bool {
	return pc.cli.ReceiveReconnect
}
