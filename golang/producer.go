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
)

type Producer interface {
	Send(context.Context, *Message) ([]*SendReceipt, error)
	GracefulStop() error
}

type producer struct {
	po producerOptions
	ns NameServer
}

var _ = Producer(&producer{})

func NewProducer(config *Config, opts ...ProducerOption) (Producer, error) {
	po := &defaultProducerOptions
	for _, opt := range opts {
		opt.apply(po)
	}
	ns, err := po.nameServerFunc(config, WithBrokerOptions(WithProducer()))
	if err != nil {
		return nil, err
	}
	return &producer{
		po: *po,
		ns: ns,
	}, nil
}

func (p *producer) Send(ctx context.Context, msg *Message) ([]*SendReceipt, error) {
	b, err := p.ns.GetBroker(ctx, msg.Topic)
	if err != nil {
		return nil, err
	}
	return b.Send(ctx, msg)
}

func (p *producer) GracefulStop() error {
	return p.ns.GracefulStop()
}
