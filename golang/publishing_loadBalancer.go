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
	"sync"
	"sync/atomic"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/apache/rocketmq-clients/golang/utils"
)

type PublishingLoadBalancer interface {
	TakeMessageQueueByMessageGroup(messageGroup string) ([]*v2.MessageQueue, error)
	TakeMessageQueues(excluded sync.Map, count int) ([]*v2.MessageQueue, error)
}

type publishingLoadBalancer struct {
	messageQueues []*v2.MessageQueue

	index int32
}

var _ = PublishingLoadBalancer(&publishingLoadBalancer{})

func NewPublishingLoadBalancer(messageQueues []*v2.MessageQueue) (PublishingLoadBalancer, error) {
	plb := &publishingLoadBalancer{
		messageQueues: messageQueues,
	}
	return plb, nil
}
func (plb *publishingLoadBalancer) TakeMessageQueueByMessageGroup(messageGroup string) ([]*v2.MessageQueue, error) {
	return nil, nil
}

func (plb *publishingLoadBalancer) TakeMessageQueues(excluded sync.Map, count int) ([]*v2.MessageQueue, error) {
	next := atomic.AddInt32(&plb.index, 1)
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
			if _, ok := excluded.Load(address.String()); ok {
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

			if _, ok := candidateBrokerNames[brokerName]; !ok {
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
