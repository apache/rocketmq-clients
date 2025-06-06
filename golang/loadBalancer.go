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
	"sync"

	"go.uber.org/atomic"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/dchest/siphash"
	// "github.com/dchest/siphash"
)

type PublishingLoadBalancer interface {
	TakeMessageQueueByMessageGroup(messageGroup *string) ([]*v2.MessageQueue, error)
	TakeMessageQueues(excluded *sync.Map, count int) ([]*v2.MessageQueue, error)
	CopyAndUpdate([]*v2.MessageQueue) PublishingLoadBalancer
}

type publishingLoadBalancer struct {
	messageQueues []*v2.MessageQueue

	index atomic.Int32
}

var _ = PublishingLoadBalancer(&publishingLoadBalancer{})

var NewPublishingLoadBalancer = func(messageQueues []*v2.MessageQueue) (PublishingLoadBalancer, error) {
	plb := &publishingLoadBalancer{
		messageQueues: messageQueues,
	}
	return plb, nil
}

func (plb *publishingLoadBalancer) TakeMessageQueueByMessageGroup(messageGroup *string) ([]*v2.MessageQueue, error) {
	if len(plb.messageQueues) == 0 {
		return nil, fmt.Errorf("messageQueues is empty")
	}
	if messageGroup == nil {
		return nil, fmt.Errorf("messageGroup is nil")
	}
	h := int64(siphash.Hash(506097522914230528, 1084818905618843912, []byte(*messageGroup)))
	i := utils.Mod64(h, len(plb.messageQueues))
	return []*v2.MessageQueue{
		plb.messageQueues[i],
	}, nil
}

func (plb *publishingLoadBalancer) TakeMessageQueues(excluded *sync.Map, count int) ([]*v2.MessageQueue, error) {
	if len(plb.messageQueues) == 0 {
		return nil, fmt.Errorf("messageQueues is empty")
	}
	next := plb.index.Inc()
	var candidates []*v2.MessageQueue
	candidateBrokerNames := make(map[string]bool, 32)

	for i := 0; i < len(plb.messageQueues); i++ {
		idx := utils.Mod(next+1, len(plb.messageQueues))
		selectMessageQueue := plb.messageQueues[idx]
		broker := selectMessageQueue.Broker
		brokerName := broker.GetName()

		if _, ok := candidateBrokerNames[brokerName]; ok {
			continue
		}

		pass := false
		for _, address := range broker.GetEndpoints().GetAddresses() {
			if _, ok := excluded.Load(utils.ParseAddress(address)); ok {
				pass = true
				break
			}
		}
		if pass {
			continue
		}

		candidates = append(candidates, selectMessageQueue)
		candidateBrokerNames[brokerName] = true

		if len(candidates) >= count {
			return candidates, nil
		}
	}
	if len(candidates) == 0 {
		for i := 0; i < len(plb.messageQueues); i++ {
			idx := utils.Mod(next+1, len(plb.messageQueues))
			selectMessageQueue := plb.messageQueues[idx]
			broker := selectMessageQueue.Broker
			brokerName := broker.GetName()

			if _, ok := candidateBrokerNames[brokerName]; ok {
				continue
			}
			candidates = append(candidates, selectMessageQueue)
			candidateBrokerNames[brokerName] = true

			if len(candidates) >= count {
				return candidates, nil
			}
		}
	}
	return candidates, nil
}

func (plb *publishingLoadBalancer) CopyAndUpdate(messageQueues []*v2.MessageQueue) PublishingLoadBalancer {
	return &publishingLoadBalancer{
		messageQueues: messageQueues,
		index:         plb.index,
	}
}

type SubscriptionLoadBalancer interface {
	TakeMessageQueue() (*v2.MessageQueue, error)
	CopyAndUpdate([]*v2.MessageQueue) SubscriptionLoadBalancer
}

type subscriptionLoadBalancer struct {
	messageQueues []*v2.MessageQueue

	index atomic.Int32
}

var _ = SubscriptionLoadBalancer(&subscriptionLoadBalancer{})

var NewSubscriptionLoadBalancer = func(messageQueues []*v2.MessageQueue) (SubscriptionLoadBalancer, error) {
	slb := &subscriptionLoadBalancer{
		messageQueues: messageQueues,
	}
	return slb, nil
}

func (slb *subscriptionLoadBalancer) TakeMessageQueue() (*v2.MessageQueue, error) {
	if len(slb.messageQueues) == 0 {
		return nil, fmt.Errorf("messageQueues is empty")
	}
	next := slb.index.Inc()
	idx := utils.Mod(next+1, len(slb.messageQueues))
	selectMessageQueue := slb.messageQueues[idx]
	return selectMessageQueue, nil
}

func (slb *subscriptionLoadBalancer) CopyAndUpdate(messageQueues []*v2.MessageQueue) SubscriptionLoadBalancer {
	return &subscriptionLoadBalancer{
		messageQueues: messageQueues,
		index:         slb.index,
	}
}
