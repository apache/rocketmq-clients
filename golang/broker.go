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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-clients/golang/metadata"
	innerOS "github.com/apache/rocketmq-clients/golang/pkg/os"
	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNoAvailableBrokers = errors.New("rocketmq: no available brokers")
)

type Broker interface {
	SetMessageQueue(queues []*v2.MessageQueue)
	Send(ctx context.Context, msg *Message) ([]*SendReceipt, error)
	QueryAssignment(ctx context.Context, topic string) ([]*v2.Assignment, error)
	ReceiveMessage(ctx context.Context, partition *v2.MessageQueue, topic string) (v2.MessagingService_ReceiveMessageClient, error)
	AckMessage(ctx context.Context, msg *MessageExt) error
	GracefulStop() error
}

type BrokerFunc func(config *Config, route []*v2.MessageQueue, opts ...BrokerOption) (Broker, error)

var _ = Broker(&broker{})

type broker struct {
	opts  brokerOptions
	cc    resolver.ClientConn
	queue atomic.Value
	mux   sync.Mutex
	conn  ClientConn
	msc   v2.MessagingServiceClient
	done  chan struct{}
}

func NewBroker(config *Config, route []*v2.MessageQueue, opts ...BrokerOption) (Broker, error) {
	b := &broker{
		opts: defaultBrokerOptions,
	}
	for _, opt := range opts {
		opt.apply(&b.opts)
	}
	b.SetMessageQueue(route)
	config.Endpoint = b.URL()
	connOpts := append(b.opts.connOptions, WithDialOptions(
		grpc.WithResolvers(b),
	))
	conn, err := b.opts.clientConnFunc(config, connOpts...)
	if err != nil {
		return nil, err
	}
	b.done = make(chan struct{})
	b.conn = conn
	b.msc = v2.NewMessagingServiceClient(conn.Conn())
	return b, nil
}

func (b *broker) SetMessageQueue(queues []*v2.MessageQueue) {
	endpoints := make(map[string]*v2.Broker)
	for i := 0; i < len(queues); i++ {
		if !b.opts.producer {
			if queues[i].Permission == v2.Permission_NONE {
				continue
			}
			if queues[i].Broker.Id != 0 {
				continue
			}
		}
		for _, ep := range queues[i].GetBroker().GetEndpoints().GetAddresses() {
			endpoints[fmt.Sprintf("%s:%d", ep.Host, ep.Port)] = queues[i].GetBroker()
		}
	}
	b.queue.Store(endpoints)
}

func (b *broker) Build(target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error) {
	b.cc = cc
	b.ResolveNow(resolver.ResolveNowOptions{})
	return b, nil
}
func (b *broker) Close() {}

func (b *broker) doResolve(opts resolver.ResolveNowOptions) {
	brokers, ok := b.queue.Load().(map[string]*v2.Broker)
	if !ok || len(brokers) == 0 {
		return
	}
	var addrs []resolver.Address
	for addr, broker := range brokers {
		addrs = append(addrs, resolver.Address{
			Addr:       addr,
			ServerName: broker.GetName(),
		})
	}
	b.mux.Lock()
	defer b.mux.Unlock()
	b.cc.UpdateState(resolver.State{
		Addresses: addrs,
	})
}

func (b *broker) ResolveNow(opts resolver.ResolveNowOptions) {
	b.doResolve(opts)
}

func (b *broker) Scheme() string {
	return "brokers"
}

func (b *broker) URL() string {
	return fmt.Sprintf("%s:///%s-%s", b.Scheme(), metadata.Rocketmq, shortuuid.New())
}

func (b *broker) GracefulStop() error {
	close(b.done)
	return b.conn.Close()
}

func (b *broker) Send(ctx context.Context, msg *Message) ([]*SendReceipt, error) {
	resp, err := b.msc.SendMessage(ctx, b.getSendMessageRequest(msg))
	if err != nil {
		return nil, err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		return nil, errors.New(resp.String())
	}
	var res []*SendReceipt
	for i := 0; i < len(resp.GetEntries()); i++ {
		res = append(res, &SendReceipt{
			MessageID: resp.GetEntries()[i].GetMessageId(),
		})
	}
	return res, nil
}

func (b *broker) HeartBeat(req *v2.HeartbeatRequest) {
	f := func() {
		ctx, _ := context.WithTimeout(context.TODO(), b.opts.timeout)
		_, _ = b.msc.Heartbeat(ctx, req)
	}
	f()
	ticker.OnceAndTick(f, b.opts.heartbeatDuration, b.done)
}

func (b *broker) QueryAssignment(ctx context.Context, topic string) ([]*v2.Assignment, error) {
	b.HeartBeat(b.getHeartBeatRequest(topic))
	req, err := b.getQueryAssignmentRequest(topic)
	if err != nil {
		return nil, err
	}
	assignment, err := b.msc.QueryAssignment(ctx, req)
	if err != nil {
		return nil, err
	}
	if assignment.GetStatus().GetCode() != v2.Code_OK {
		return nil, fmt.Errorf("QueryAssignment err = %s", assignment.String())
	}

	if len(assignment.Assignments) == 0 {
		return nil, errors.New("rocketmq: no available assignments")
	}
	return assignment.Assignments, nil
}

func (b *broker) ReceiveMessage(ctx context.Context, queue *v2.MessageQueue, topic string) (v2.MessagingService_ReceiveMessageClient, error) {
	stream, err := b.msc.ReceiveMessage(ctx, b.getReceiveMessageRequest(queue, topic))
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (b *broker) AckMessage(ctx context.Context, msg *MessageExt) error {
	response, err := b.msc.AckMessage(ctx, b.getAckMessageRequest(msg))
	if err != nil {
		return err
	}
	if response.GetStatus().GetCode() != v2.Code_OK {
		return fmt.Errorf("AckMessage err = %s", response.String())
	}
	return nil
}

func (b *broker) getSendMessageRequest(msg *Message) *v2.SendMessageRequest {
	return &v2.SendMessageRequest{
		Messages: []*v2.Message{
			{
				Topic: &v2.Resource{
					ResourceNamespace: b.conn.Config().NameSpace,
					Name:              msg.Topic,
				},
				SystemProperties: &v2.SystemProperties{
					Tag:           &msg.Tag,
					MessageGroup:  &b.conn.Config().Group,
					MessageType:   v2.MessageType_NORMAL,
					BodyEncoding:  v2.Encoding_IDENTITY,
					BornHost:      innerOS.Hostname(),
					MessageId:     uuid.New().String(),
					BornTimestamp: timestamppb.Now(),
				},
				Body: msg.Body,
			},
		},
	}
}

func (b *broker) getQueryAssignmentRequest(topic string) (ret *v2.QueryAssignmentRequest, err error) {
	brokers, ok := b.queue.Load().(map[string]*v2.Broker)
	if !ok || len(brokers) == 0 {
		err = ErrNoAvailableBrokers
		return
	}

	for _, broker := range brokers {
		ret = &v2.QueryAssignmentRequest{
			Topic: &v2.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              topic,
			},
			Group: &v2.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              b.conn.Config().Group,
			},
			Endpoints: &v2.Endpoints{
				Scheme:    broker.GetEndpoints().GetScheme(),
				Addresses: broker.GetEndpoints().GetAddresses(),
			},
		}
		break
	}
	return
}

func (b *broker) getHeartBeatRequest(topic string) *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group: &v2.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              b.conn.Config().Group,
		},
		ClientType: v2.ClientType_SIMPLE_CONSUMER,
	}
}

func (b *broker) getReceiveMessageRequest(queue *v2.MessageQueue, topic string) *v2.ReceiveMessageRequest {
	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              b.conn.Config().Group,
		},
		MessageQueue: &v2.MessageQueue{
			Topic: &v2.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              topic,
			},
			Id: queue.GetId(),
			Broker: &v2.Broker{
				Name: queue.GetBroker().GetName(),
				Id:   queue.GetBroker().GetId(),
				Endpoints: &v2.Endpoints{
					Scheme:    queue.GetBroker().GetEndpoints().GetScheme(),
					Addresses: queue.GetBroker().GetEndpoints().GetAddresses(),
				},
			},
			AcceptMessageTypes: []v2.MessageType{
				v2.MessageType_NORMAL,
			},
		},
		FilterExpression: &v2.FilterExpression{
			Type:       v2.FilterType_TAG,
			Expression: "*",
		},
		BatchSize:         32,
		InvisibleDuration: durationpb.New(time.Millisecond * 15 * 60 * 1000),
		AutoRenew:         false,
	}
}

func (b *broker) getAckMessageRequest(msg *MessageExt) *v2.AckMessageRequest {
	return &v2.AckMessageRequest{
		Group: &v2.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              b.conn.Config().Group,
		},
		Topic: &v2.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              msg.Topic,
		},
		Entries: []*v2.AckMessageEntry{
			{
				ReceiptHandle: msg.ReceiptHandle,
			},
		},
	}
}

func (b *broker) ID() string {
	return b.opts.id
}

func init() {
	resolver.Register(&broker{})
}
