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
	"testing"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

func TestPublishingLoadBalancerFiltersNonWritableMasterQueues(t *testing.T) {
	queues := []*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_READ_WRITE),
		newMessageQueue(0, v2.Permission_READ),
		newMessageQueue(1, v2.Permission_READ_WRITE),
	}

	loadBalancer, err := NewPublishingLoadBalancer(queues)
	if err != nil {
		t.Fatalf("NewPublishingLoadBalancer() error = %v", err)
	}

	candidates, err := loadBalancer.TakeMessageQueues(&sync.Map{}, len(queues))
	if err != nil {
		t.Fatalf("TakeMessageQueues() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("TakeMessageQueues() returned %d queues, want 1", len(candidates))
	}
	assertWritableMasterQueue(t, candidates[0])

	messageGroup := "group"
	fifoCandidates, err := loadBalancer.TakeMessageQueueByMessageGroup(&messageGroup)
	if err != nil {
		t.Fatalf("TakeMessageQueueByMessageGroup() error = %v", err)
	}
	if len(fifoCandidates) != 1 {
		t.Fatalf("TakeMessageQueueByMessageGroup() returned %d queues, want 1", len(fifoCandidates))
	}
	assertWritableMasterQueue(t, fifoCandidates[0])
}

func TestPublishingLoadBalancerRejectsRouteWithoutWritableMaster(t *testing.T) {
	queues := []*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_READ),
		newMessageQueue(1, v2.Permission_READ_WRITE),
	}

	if _, err := NewPublishingLoadBalancer(queues); err == nil {
		t.Fatal("NewPublishingLoadBalancer() error = nil, want non-nil")
	}
}

func TestPublishingLoadBalancerFiltersUpdatedRoute(t *testing.T) {
	loadBalancer, err := NewPublishingLoadBalancer([]*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_WRITE),
	})
	if err != nil {
		t.Fatalf("NewPublishingLoadBalancer() error = %v", err)
	}

	updated := loadBalancer.CopyAndUpdate([]*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_READ_WRITE),
		newMessageQueue(1, v2.Permission_READ_WRITE),
	})
	candidates, err := updated.TakeMessageQueues(&sync.Map{}, 2)
	if err != nil {
		t.Fatalf("TakeMessageQueues() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("TakeMessageQueues() returned %d queues, want 1", len(candidates))
	}
	assertWritableMasterQueue(t, candidates[0])

	withoutMaster := updated.CopyAndUpdate([]*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_READ),
		newMessageQueue(1, v2.Permission_READ_WRITE),
	})
	if _, err := withoutMaster.TakeMessageQueues(&sync.Map{}, 1); err == nil {
		t.Fatal("TakeMessageQueues() error = nil after removing writable master, want non-nil")
	}

	recovered := withoutMaster.CopyAndUpdate([]*v2.MessageQueue{
		newMessageQueue(0, v2.Permission_WRITE),
	})
	recoveredCandidates, err := recovered.TakeMessageQueues(&sync.Map{}, 1)
	if err != nil {
		t.Fatalf("TakeMessageQueues() after recovery error = %v", err)
	}
	if len(recoveredCandidates) != 1 {
		t.Fatalf("TakeMessageQueues() after recovery returned %d queues, want 1", len(recoveredCandidates))
	}
	assertWritableMasterQueue(t, recoveredCandidates[0])
}

func newMessageQueue(brokerID int32, permission v2.Permission) *v2.MessageQueue {
	return &v2.MessageQueue{
		Permission: permission,
		Broker: &v2.Broker{
			Name: "broker",
			Id:   brokerID,
		},
	}
}

func assertWritableMasterQueue(t *testing.T, queue *v2.MessageQueue) {
	t.Helper()
	if queue.GetBroker().GetId() != 0 {
		t.Errorf("broker id = %d, want 0", queue.GetBroker().GetId())
	}
	permission := queue.GetPermission()
	if permission != v2.Permission_WRITE && permission != v2.Permission_READ_WRITE {
		t.Errorf("permission = %s, want WRITE or READ_WRITE", permission)
	}
}
