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
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/credentials"
	gomock "github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
)

func TestCLINewClient(t *testing.T) {
	stubs := gostub.Stub(&defaultClientManagerOptions, clientManagerOptions{
		RPC_CLIENT_MAX_IDLE_DURATION: time.Second,

		RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Hour,
		RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Hour,

		HEART_BEAT_INITIAL_DELAY: time.Hour,
		HEART_BEAT_PERIOD:        time.Hour,

		LOG_STATS_INITIAL_DELAY: time.Hour,
		LOG_STATS_PERIOD:        time.Hour,

		SYNC_SETTINGS_DELAY:  time.Hour,
		SYNC_SETTINGS_PERIOD: time.Hour,
	})

	stubs2 := gostub.Stub(&NewRpcClient, func(target string, opts ...RpcClientOption) (RpcClient, error) {
		if target == fakeAddress {
			return MOCK_RPC_CLIENT, nil
		}
		return nil, fmt.Errorf("invalid target=%s", target)
	})

	defer func() {
		stubs.Reset()
		stubs2.Reset()
	}()

	MOCK_RPC_CLIENT.EXPECT().Telemetry(gomock.Any()).Return(&MOCK_MessagingService_TelemetryClient{
		trace: make([]string, 0),
	}, nil)

	endpoints := fmt.Sprintf("%s:%d", fakeHost, fakePort)
	cli, err := NewClient(&Config{
		Endpoint:    endpoints,
		Credentials: &credentials.SessionCredentials{},
	})
	if err != nil {
		t.Error(err)
	}
	sugarBaseLogger.Info(cli)
	err = cli.(*defaultClient).startUp()
	if err != nil {
		t.Error(err)
	}
}
