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
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

var _ = ClientSettings(&liteSimpleConsumerSettings{})

type liteSimpleConsumerSettings struct {
	*simpleConsumerSettings
	bindTopic               string
	liteSubscriptionManager *liteSubscriptionManager
}

func newLiteSimpleConsumerSettings(settings *simpleConsumerSettings, bindTopic string) *liteSimpleConsumerSettings {
	return &liteSimpleConsumerSettings{
		simpleConsumerSettings: settings,
		bindTopic:              bindTopic,
	}
}

// GetAccessPoint implements ClientSettings
func (lsc *liteSimpleConsumerSettings) GetAccessPoint() *v2.Endpoints {
	return lsc.simpleConsumerSettings.endpoints
}

// GetClientID implements ClientSettings
func (lsc *liteSimpleConsumerSettings) GetClientID() string {
	return lsc.simpleConsumerSettings.clientId
}

// GetClientType implements ClientSettings
func (lsc *liteSimpleConsumerSettings) GetClientType() v2.ClientType {
	return lsc.simpleConsumerSettings.clientType
}

// GetRequestTimeout implements ClientSettings
func (lsc *liteSimpleConsumerSettings) GetRequestTimeout() time.Duration {
	return lsc.simpleConsumerSettings.requestTimeout
}

// GetRetryPolicy implements ClientSettings
func (lsc *liteSimpleConsumerSettings) GetRetryPolicy() *v2.RetryPolicy {
	return lsc.simpleConsumerSettings.retryPolicy
}

// applySettingsCommand implements ClientSettings
func (lsc *liteSimpleConsumerSettings) applySettingsCommand(settings *v2.Settings) error {
	if lsc.simpleConsumerSettings.applySettingsCommand(settings) != nil {
		sugarBaseLogger.Warnf("liteSimpleConsumerSettings applySettingsCommand failed")
		return fmt.Errorf("liteSimpleConsumerSettings applySettingsCommand failed")
	}
	// Delegate subscription settings (quota, max topic size) to the manager
	if lsc.liteSubscriptionManager != nil {
		lsc.liteSubscriptionManager.sync(settings.GetSubscription())
	}
	return nil
}

// toProtobuf implements ClientSettings
func (lsc *liteSimpleConsumerSettings) toProtobuf() *v2.Settings {
	return lsc.simpleConsumerSettings.toProtobuf()
}
