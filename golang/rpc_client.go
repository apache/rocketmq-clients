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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	// "google.golang.org/protobuf/types/known/durationpb"
)

var (
	ErrNoAvailableBrokers = errors.New("rocketmq: no available brokers")
)

type RpcClient interface {
	GracefulStop() error
	SendMessage(ctx context.Context, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error)
}

var _ = RpcClient(&rpcClient{})

type rpcClient struct {
	opts  rpcClientOptions
	queue atomic.Value
	mux   sync.Mutex
	conn  ClientConn
	msc   v2.MessagingServiceClient
	done  chan struct{}
}

func NewRpcClient(endpoint string, opts ...RpcClientOption) (RpcClient, error) {
	rc := &rpcClient{
		opts: defaultRpcClientOptions,
	}
	for _, opt := range opts {
		opt.apply(&rc.opts)
	}
	conn, err := rc.opts.clientConnFunc(endpoint, rc.opts.connOptions...)
	if err != nil {
		return nil, err
	}
	rc.done = make(chan struct{})
	rc.conn = conn
	rc.msc = v2.NewMessagingServiceClient(conn.Conn())
	return rc, nil
}

func (rc *rpcClient) Close() {}

func (rc *rpcClient) GracefulStop() error {
	close(rc.done)
	return rc.conn.Close()
}

func (rc *rpcClient) SendMessage(ctx context.Context, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	return rc.msc.SendMessage(ctx, request)
}

// // TODO refer to java sdk.
// func (rc *rpcClient) send0(ctx context.Context, msg *Message, txEnabled bool) ([]*SendReceipt, error) {
// 	// var publishingMessage PublishingMessage
// 	publishingMessage, err := NewPublishingMessage(msg, txEnabled)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// messageType := publishingMessage.messageType
// 	// var messageGroup string
// 	// // Message group must be same if message type is FIFO, or no need to proceed.
// 	// if messageType == v2.MessageType_FIFO{
// 	// 	messageGroup = publishingMessage.msg.GetMessageGroup()
// 	// }

// 	msgV2, err := publishingMessage.toProtobuf()
// 	if err != nil {
// 		return nil, err
// 	}
// 	sendRequest := &v2.SendMessageRequest{
// 		Messages: []*v2.Message{
// 			msgV2,
// 		},
// 	}
// 	resp, err := rc.msc.SendMessage(ctx, sendRequest)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if resp.GetStatus().GetCode() != v2.Code_OK {
// 		return nil, errors.New(resp.String())
// 	}
// 	var res []*SendReceipt
// 	for i := 0; i < len(resp.GetEntries()); i++ {
// 		res = append(res, &SendReceipt{
// 			MessageID: resp.GetEntries()[i].GetMessageId(),
// 		})
// 	}
// 	return res, nil
// 	// return nil, nil
// }

// func (rc *rpcClient) Send(ctx context.Context, msg *Message) ([]*SendReceipt, error) {
// 	return rc.send0(ctx, msg, false)
// }

// func (rc *rpcClient) HeartBeat(req *v2.HeartbeatRequest) {
// 	f := func() {
// 		ctx, _ := context.WithTimeout(context.TODO(), rc.opts.timeout)
// 		_, _ = rc.msc.Heartbeat(ctx, req)
// 	}
// 	f()
// 	ticker.OnceAndTick(f, rc.opts.heartbeatDuration, rc.done)
// }

// func (rc *rpcClient) QueryAssignment(ctx context.Context, topic string) ([]*v2.Assignment, error) {
// 	rc.HeartBeat(rc.getHeartBeatRequest(topic))
// 	req, err := rc.getQueryAssignmentRequest(topic)
// 	if err != nil {
// 		return nil, err
// 	}
// 	assignment, err := rc.msc.QueryAssignment(ctx, req)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if assignment.GetStatus().GetCode() != v2.Code_OK {
// 		return nil, fmt.Errorf("QueryAssignment err = %s", assignment.String())
// 	}

// 	if len(assignment.Assignments) == 0 {
// 		return nil, errors.New("rocketmq: no available assignments")
// 	}
// 	return assignment.Assignments, nil
// }

// func (rc *rpcClient) ReceiveMessage(ctx context.Context, queue *v2.MessageQueue, topic string) (v2.MessagingService_ReceiveMessageClient, error) {
// 	stream, err := rc.msc.ReceiveMessage(ctx, rc.getReceiveMessageRequest(queue, topic))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return stream, nil
// }

// func (rc *rpcClient) AckMessage(ctx context.Context, msg *MessageExt) error {
// 	response, err := rc.msc.AckMessage(ctx, rc.getAckMessageRequest(msg))
// 	if err != nil {
// 		return err
// 	}
// 	if response.GetStatus().GetCode() != v2.Code_OK {
// 		return fmt.Errorf("AckMessage err = %s", response.String())
// 	}
// 	return nil
// }

// func (rc *rpcClient) getQueryAssignmentRequest(topic string) (ret *v2.QueryAssignmentRequest, err error) {
// 	brokers, ok := rc.queue.Load().(map[string]*v2.Broker)
// 	if !ok || len(brokers) == 0 {
// 		err = ErrNoAvailableBrokers
// 		return
// 	}

// 	for _, broker := range brokers {
// 		ret = &v2.QueryAssignmentRequest{
// 			Topic: &v2.Resource{
// 				ResourceNamespace: rc.conn.Config().NameSpace,
// 				Name:              topic,
// 			},
// 			Group: &v2.Resource{
// 				ResourceNamespace: rc.conn.Config().NameSpace,
// 				Name:              rc.conn.Config().Group,
// 			},
// 			Endpoints: &v2.Endpoints{
// 				Scheme:    broker.GetEndpoints().GetScheme(),
// 				Addresses: broker.GetEndpoints().GetAddresses(),
// 			},
// 		}
// 		break
// 	}
// 	return
// }

// func (rc *rpcClient) getHeartBeatRequest(topic string) *v2.HeartbeatRequest {
// 	return &v2.HeartbeatRequest{
// 		Group: &v2.Resource{
// 			ResourceNamespace: rc.conn.Config().NameSpace,
// 			Name:              rc.conn.Config().Group,
// 		},
// 		ClientType: v2.ClientType_SIMPLE_CONSUMER,
// 	}
// }

// func (rc *rpcClient) getReceiveMessageRequest(queue *v2.MessageQueue, topic string) *v2.ReceiveMessageRequest {
// 	return &v2.ReceiveMessageRequest{
// 		Group: &v2.Resource{
// 			ResourceNamespace: rc.conn.Config().NameSpace,
// 			Name:              rc.conn.Config().Group,
// 		},
// 		MessageQueue: &v2.MessageQueue{
// 			Topic: &v2.Resource{
// 				ResourceNamespace: rc.conn.Config().NameSpace,
// 				Name:              topic,
// 			},
// 			Id: queue.GetId(),
// 			Broker: &v2.Broker{
// 				Name: queue.GetBroker().GetName(),
// 				Id:   queue.GetBroker().GetId(),
// 				Endpoints: &v2.Endpoints{
// 					Scheme:    queue.GetBroker().GetEndpoints().GetScheme(),
// 					Addresses: queue.GetBroker().GetEndpoints().GetAddresses(),
// 				},
// 			},
// 			AcceptMessageTypes: []v2.MessageType{
// 				v2.MessageType_NORMAL,
// 			},
// 		},
// 		FilterExpression: &v2.FilterExpression{
// 			Type:       v2.FilterType_TAG,
// 			Expression: "*",
// 		},
// 		BatchSize:         32,
// 		InvisibleDuration: durationpb.New(time.Millisecond * 15 * 60 * 1000),
// 		AutoRenew:         false,
// 	}
// }

// func (rc *rpcClient) getAckMessageRequest(msg *MessageExt) *v2.AckMessageRequest {
// 	return &v2.AckMessageRequest{
// 		Group: &v2.Resource{
// 			ResourceNamespace: rc.conn.Config().NameSpace,
// 			Name:              rc.conn.Config().Group,
// 		},
// 		Topic: &v2.Resource{
// 			ResourceNamespace: rc.conn.Config().NameSpace,
// 			Name:              msg.Topic,
// 		},
// 		Entries: []*v2.AckMessageEntry{
// 			{
// 				ReceiptHandle: msg.ReceiptHandle,
// 			},
// 		},
// 	}
// }
