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
	v1 "github.com/apache/rocketmq-clients/golang/protocol/v1"

	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNoAvailableBrokers = errors.New("rocketmq: no available brokers")
)

type Broker interface {
	SetPartition(partitions []*v1.Partition)
	Send(ctx context.Context, msg *Message) (*SendReceipt, error)
	QueryAssignment(ctx context.Context, topic string) ([]*v1.Assignment, error)
	ReceiveMessage(ctx context.Context, partition *v1.Partition, topic string) ([]*MessageExt, error)
	AckMessage(ctx context.Context, msg *MessageExt) error
	GracefulStop() error
}

type BrokerFunc func(config *Config, route []*v1.Partition, opts ...BrokerOption) (Broker, error)

var _ = Broker(&broker{})

type broker struct {
	opts       brokerOptions
	cc         resolver.ClientConn
	partitions atomic.Value
	mux        sync.Mutex
	conn       ClientConn
	msc        v1.MessagingServiceClient
	done       chan struct{}
}

func NewBroker(config *Config, route []*v1.Partition, opts ...BrokerOption) (Broker, error) {
	b := &broker{
		opts: defaultBrokerOptions,
	}
	for _, opt := range opts {
		opt.apply(&b.opts)
	}
	b.SetPartition(route)
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
	b.msc = v1.NewMessagingServiceClient(conn.Conn())
	return b, nil
}

func (b *broker) SetPartition(partitions []*v1.Partition) {
	endpoints := make(map[string]*v1.Broker)
	for i := 0; i < len(partitions); i++ {
		if !b.opts.producer {
			if partitions[i].Permission == v1.Permission_NONE {
				continue
			}
			if partitions[i].Broker.Id != 0 {
				continue
			}
		}
		for _, ep := range partitions[i].GetBroker().GetEndpoints().GetAddresses() {
			endpoints[fmt.Sprintf("%s:%d", ep.Host, ep.Port)] = partitions[i].GetBroker()
		}
	}
	b.partitions.Store(endpoints)
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
	brokers, ok := b.partitions.Load().(map[string]*v1.Broker)
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

func (b *broker) Send(ctx context.Context, msg *Message) (*SendReceipt, error) {
	resp, err := b.msc.SendMessage(ctx, b.getSendMessageRequest(msg))
	if err != nil {
		return nil, err
	}
	return &SendReceipt{
		MessageID: resp.GetMessageId(),
	}, nil
}

func (b *broker) HeartBeat(req *v1.HeartbeatRequest) {
	f := func() {
		ctx, _ := context.WithTimeout(context.TODO(), b.opts.timeout)
		_, _ = b.msc.Heartbeat(ctx, req)
	}
	f()
	ticker.OnceAndTick(f, b.opts.heartbeatDuration, b.done)
}

func (b *broker) QueryAssignment(ctx context.Context, topic string) ([]*v1.Assignment, error) {
	b.HeartBeat(b.getHeartBeatRequest(topic))
	req, err := b.getQueryAssignmentRequest(topic)
	if err != nil {
		return nil, err
	}
	assignment, err := b.msc.QueryAssignment(ctx, req)
	if err != nil {
		return nil, err
	}
	if assignment.GetCommon().GetStatus().GetCode() != int32(codes.OK) {
		return nil, fmt.Errorf("QueryAssignment err = %s", assignment.String())
	}

	if len(assignment.Assignments) == 0 {
		return nil, errors.New("rocketmq: no available assignments")
	}
	return assignment.Assignments, nil
}

func (b *broker) ReceiveMessage(ctx context.Context, partition *v1.Partition, topic string) ([]*MessageExt, error) {
	response, err := b.msc.ReceiveMessage(ctx, b.getReceiveMessageRequest(partition, topic))
	if err != nil {
		return nil, err
	}
	if response.GetCommon().GetStatus().GetCode() != int32(codes.OK) {
		return nil, fmt.Errorf("ReceiveMessage err = %s", response.String())
	}
	var ret []*MessageExt
	for _, msg := range response.Messages {
		ret = append(ret, &MessageExt{
			MessageID:     msg.GetSystemAttribute().GetMessageId(),
			ReceiptHandle: msg.GetSystemAttribute().GetReceiptHandle(),
			Message: Message{
				Topic:      msg.GetTopic().GetName(),
				Body:       msg.GetBody(),
				Tag:        msg.GetSystemAttribute().GetTag(),
				Keys:       msg.GetSystemAttribute().GetKeys(),
				Properties: msg.GetUserAttribute(),
			},
		})
	}
	return ret, nil
}

func (b *broker) AckMessage(ctx context.Context, msg *MessageExt) error {
	response, err := b.msc.AckMessage(ctx, b.getAckMessageRequest(msg))
	if err != nil {
		return err
	}
	if response.GetCommon().GetStatus().GetCode() != int32(codes.OK) {
		return fmt.Errorf("AckMessage err = %s", response.String())
	}
	return nil
}

func (b *broker) getSendMessageRequest(msg *Message) *v1.SendMessageRequest {
	return &v1.SendMessageRequest{
		Message: &v1.Message{
			Topic: &v1.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              msg.Topic,
			},
			SystemAttribute: &v1.SystemAttribute{
				Tag:           msg.Tag,
				MessageGroup:  b.conn.Config().Group,
				MessageType:   v1.MessageType_NORMAL,
				BodyEncoding:  v1.Encoding_IDENTITY,
				BornHost:      innerOS.Hostname(),
				MessageId:     uuid.New().String(),
				BornTimestamp: timestamppb.Now(),
			},
			Body: msg.Body,
		},
	}
}

func (b *broker) getQueryAssignmentRequest(topic string) (ret *v1.QueryAssignmentRequest, err error) {
	brokers, ok := b.partitions.Load().(map[string]*v1.Broker)
	if !ok || len(brokers) == 0 {
		err = ErrNoAvailableBrokers
		return
	}

	for _, broker := range brokers {
		ret = &v1.QueryAssignmentRequest{
			Topic: &v1.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              topic,
			},
			Group: &v1.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              b.conn.Config().Group,
			},
			ClientId: b.ID(),
			Endpoints: &v1.Endpoints{
				Scheme:    broker.GetEndpoints().GetScheme(),
				Addresses: broker.GetEndpoints().GetAddresses(),
			},
		}
		break
	}
	return
}

func (b *broker) getHeartBeatRequest(topic string) *v1.HeartbeatRequest {
	return &v1.HeartbeatRequest{
		ClientId: b.ID(),
		FifoFlag: false,
		ClientData: &v1.HeartbeatRequest_ConsumerData{
			ConsumerData: &v1.ConsumerData{
				Group: &v1.Resource{
					ResourceNamespace: b.conn.Config().NameSpace,
					Name:              b.conn.Config().Group,
				},
				Subscriptions: []*v1.SubscriptionEntry{
					{
						Topic: &v1.Resource{
							ResourceNamespace: b.conn.Config().NameSpace,
							Name:              topic,
						},
						Expression: &v1.FilterExpression{
							Type:       v1.FilterType_TAG,
							Expression: "*",
						},
					},
				},
				ConsumeType:   v1.ConsumeMessageType_PASSIVE,
				ConsumeModel:  v1.ConsumeModel_CLUSTERING,
				ConsumePolicy: v1.ConsumePolicy_RESUME,
				DeadLetterPolicy: &v1.DeadLetterPolicy{
					MaxDeliveryAttempts: 17,
				},
			},
		},
	}
}

func (b *broker) getReceiveMessageRequest(partition *v1.Partition, topic string) *v1.ReceiveMessageRequest {
	return &v1.ReceiveMessageRequest{
		Group: &v1.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              b.conn.Config().Group,
		},
		ClientId: b.ID(),
		Partition: &v1.Partition{
			Topic: &v1.Resource{
				ResourceNamespace: b.conn.Config().NameSpace,
				Name:              topic,
			},
			Id: partition.Id,
			Broker: &v1.Broker{
				Name: partition.Broker.Name,
			},
		},
		FilterExpression: &v1.FilterExpression{
			Type:       v1.FilterType_TAG,
			Expression: "*",
		},
		ConsumePolicy:     v1.ConsumePolicy_RESUME,
		BatchSize:         32,
		InvisibleDuration: durationpb.New(time.Millisecond * 15 * 60 * 1000),
		AwaitTime:         durationpb.New(time.Millisecond * 0),
		FifoFlag:          false,
	}
}

func (b *broker) getAckMessageRequest(msg *MessageExt) *v1.AckMessageRequest {
	return &v1.AckMessageRequest{
		Group: &v1.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              b.conn.Config().Group,
		},
		Topic: &v1.Resource{
			ResourceNamespace: b.conn.Config().NameSpace,
			Name:              msg.Topic,
		},
		ClientId:  b.ID(),
		MessageId: msg.MessageID,
		Handle: &v1.AckMessageRequest_ReceiptHandle{
			ReceiptHandle: msg.ReceiptHandle,
		},
	}
}

func (b *broker) ID() string {
	return b.opts.id
}

func init() {
	resolver.Register(&broker{})
}
