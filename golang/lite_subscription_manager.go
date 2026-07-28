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
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/ticker"
	"github.com/apache/rocketmq-clients/golang/v5/pkg/utils"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// clientCore is the consumer-side narrow view of a client implementation:
// the methods are defined on the isClient contract, but the manager only
// depends on the subset it actually needs.
type clientCore interface {
	isRunning() bool
	getClient() *defaultClient
	getRequestTimeout() time.Duration
}

// liteSubscriptionManager manages lite topic subscriptions, providing client-side
// validation, quota enforcement, and multi-endpoint synchronization.
type liteSubscriptionManager struct {
	consumerImpl          clientCore
	bindTopic             string
	groupResource         *v2.Resource
	namespace             string
	liteTopicSet          sync.Map
	liteSubscriptionQuota int32
	maxLiteTopicSize      int32
}

func newLiteSubscriptionManager(consumer clientCore, bindTopic string, groupResource *v2.Resource, namespace string) *liteSubscriptionManager {
	return &liteSubscriptionManager{
		consumerImpl:          consumer,
		bindTopic:             bindTopic,
		groupResource:         groupResource,
		namespace:             namespace,
		liteSubscriptionQuota: 2000,
		maxLiteTopicSize:      64,
	}
}

func (m *liteSubscriptionManager) startUp() {
	m.syncAllLiteSubscription() // syncAll after startup for subscribeLite("*")
	ticker.Tick(m.syncAllLiteSubscription, time.Second*30, m.consumerImpl.getClient().done)
}

// sync applies server-pushed subscription settings (quota, max topic size).
func (m *liteSubscriptionManager) sync(subscription *v2.Subscription) {
	if subscription == nil {
		sugarBaseLogger.Warnf("onSettingsCommand err = subscription is nil")
		return
	}
	if subscription.LiteSubscriptionQuota != nil {
		m.liteSubscriptionQuota = *subscription.LiteSubscriptionQuota
	}
	if subscription.MaxLiteTopicSize != nil {
		m.maxLiteTopicSize = *subscription.MaxLiteTopicSize
	}
}

func (m *liteSubscriptionManager) subscribeLite(liteTopic string, offsetOption ...OffsetOption) error {
	if len(offsetOption) > 1 {
		return errors.New("only one offset option is supported")
	}
	var option *OffsetOption
	if len(offsetOption) == 1 {
		option = &offsetOption[0]
	}
	if err := m.checkRunning(); err != nil {
		return err
	}
	if _, loaded := m.liteTopicSet.Load(liteTopic); loaded {
		return nil
	}
	if err := m.validateLiteTopic(liteTopic); err != nil {
		return err
	}
	if err := m.checkLiteSubscriptionQuota(1); err != nil {
		return err
	}
	if err := m.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_PARTIAL_ADD, []string{liteTopic}, option); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer SubscribeLite liteTopic:%s err:%v", liteTopic, err)
		return err
	}
	m.liteTopicSet.Store(liteTopic, struct{}{})
	sugarBaseLogger.Infof("SubscribeLite %s, topic=%s, group=%s, clientId=%s",
		liteTopic, m.bindTopic, m.groupResource.GetName(), m.consumerImpl.getClient().clientID)
	return nil
}

func (m *liteSubscriptionManager) unsubscribeLite(liteTopic string) error {
	if err := m.checkRunning(); err != nil {
		return err
	}
	if _, loaded := m.liteTopicSet.Load(liteTopic); !loaded {
		return nil
	}
	if err := m.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_PARTIAL_REMOVE, []string{liteTopic}, nil); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer UnSubscribeLite liteTopic:%s err:%v", liteTopic, err)
		return err
	}
	m.liteTopicSet.Delete(liteTopic)
	sugarBaseLogger.Infof("UnsubscribeLite %s, topic=%s, group=%s, clientId=%s",
		liteTopic, m.bindTopic, m.groupResource.GetName(), m.consumerImpl.getClient().clientID)
	return nil
}

func (m *liteSubscriptionManager) onNotifyUnsubscribeLiteCommand(command *v2.NotifyUnsubscribeLiteCommand) {
	liteTopic := command.LiteTopic
	sugarBaseLogger.Infof("LitePushConsumer notifyUnsubscribeLite liteTopic:%s", liteTopic)
	if liteTopic != "" {
		m.liteTopicSet.Delete(liteTopic)
	}
}

// getLiteTopicSet returns an immutable snapshot of current lite topics.
func (m *liteSubscriptionManager) getLiteTopicSet() []string {
	topics := make([]string, 0)
	m.liteTopicSet.Range(func(key, value interface{}) bool {
		if liteTopic, ok := key.(string); ok {
			topics = append(topics, liteTopic)
		}
		return true
	})
	return topics
}

func (m *liteSubscriptionManager) syncAllLiteSubscription() {
	if err := m.checkLiteSubscriptionQuota(0); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer syncAllLiteSubscription quota check failed: %v", err)
		return
	}
	liteTopics := m.getLiteTopicSet()
	if err := m.syncLiteSubscription(context.TODO(), v2.LiteSubscriptionAction_COMPLETE_ADD, liteTopics, nil); err != nil {
		sugarBaseLogger.Errorf("LitePushConsumer syncAllLiteSubscription:%v, err:%v", liteTopics, err)
	}
}

func (m *liteSubscriptionManager) validateLiteTopic(liteTopic string) error {
	if strings.TrimSpace(liteTopic) == "" {
		return errors.New("liteTopic is blank")
	}
	if int32(len(liteTopic)) > m.maxLiteTopicSize {
		return fmt.Errorf("liteTopic length exceeded max length %d, liteTopic: %s", m.maxLiteTopicSize, liteTopic)
	}
	return nil
}

func (m *liteSubscriptionManager) checkLiteSubscriptionQuota(delta int32) error {
	currentSize := int32(len(m.getLiteTopicSet()))
	if currentSize+delta > m.liteSubscriptionQuota {
		return &ErrRpcStatus{
			Code:    int32(v2.Code_LITE_SUBSCRIPTION_QUOTA_EXCEEDED),
			Message: fmt.Sprintf("lite subscription quota exceeded %d", m.liteSubscriptionQuota),
		}
	}
	return nil
}

func (m *liteSubscriptionManager) checkRunning() error {
	if !m.consumerImpl.isRunning() {
		err := fmt.Errorf("client not running, clientId=%s", m.consumerImpl.getClient().clientID)
		sugarBaseLogger.Error(err.Error())
		return err
	}
	return nil
}

// getRouteEndpoints extracts unique broker endpoints from the cached topic route data.
func (m *liteSubscriptionManager) getRouteEndpoints() []*v2.Endpoints {
	cli := m.consumerImpl.getClient()
	item, ok := cli.router.Load(m.bindTopic)
	if !ok {
		return nil
	}
	queues, ok := item.([]*v2.MessageQueue)
	if !ok {
		return nil
	}
	seen := make(map[string]bool)
	var endpoints []*v2.Endpoints
	for _, mq := range queues {
		ep := mq.GetBroker().GetEndpoints()
		key := utils.EndpointsToString(ep)
		if !seen[key] {
			seen[key] = true
			endpoints = append(endpoints, ep)
		}
	}
	return endpoints
}

func (m *liteSubscriptionManager) syncLiteSubscription(ctx context.Context, action v2.LiteSubscriptionAction, diff []string, offsetOption *OffsetOption) error {
	request := &v2.SyncLiteSubscriptionRequest{
		Action: action,
		Topic: &v2.Resource{
			Name:              m.bindTopic,
			ResourceNamespace: m.namespace,
		},
		Group:        m.groupResource,
		LiteTopicSet: diff,
	}
	if offsetOption != nil {
		request.OffsetOption = offsetOption.toProtobuf()
	}

	clientId := m.consumerImpl.getClient().clientID
	if action == v2.LiteSubscriptionAction_COMPLETE_ADD {
		sugarBaseLogger.Infof("syncLiteSubscription action:%s, topic:%s, group:%s, clientId:%s, liteTopicCount:%d",
			action, m.bindTopic, m.groupResource.GetName(), clientId, len(diff))
	} else {
		sugarBaseLogger.Infof("syncLiteSubscription action:%s, topic:%s, group:%s, clientId:%s, liteTopics:%v",
			action, m.bindTopic, m.groupResource.GetName(), clientId, diff)
	}

	// Collect unique broker endpoints; fall back to accessPoint if no route data
	cli := m.consumerImpl.getClient()
	routeEndpoints := m.getRouteEndpoints()
	if len(routeEndpoints) == 0 {
		routeEndpoints = []*v2.Endpoints{cli.accessPoint}
	}

	timeout := m.consumerImpl.getRequestTimeout()

	var firstErr error
	for _, ep := range routeEndpoints {
		signedCtx := cli.Sign(ctx)
		resp, err := cli.clientManager.SyncLiteSubscription(signedCtx, ep, request, timeout)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if resp.GetStatus().GetCode() != v2.Code_OK {
			if firstErr == nil {
				firstErr = &ErrRpcStatus{
					Code:    int32(resp.Status.GetCode()),
					Message: resp.GetStatus().GetMessage(),
				}
			}
		}
	}
	return firstErr
}
