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
	"testing"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// This test is designed to verify there is no data race in dcmp.Reset
func TestDefaultClientMeterProviderResetNoDataRace(t *testing.T) {
	cli := BuildCLient(t)
	metric := &v2.Metric{On: false, Endpoints: cli.accessPoint}

	for i := 0; i < 5; i++ {
		go func() {
			cli.clientMeterProvider.Reset(metric)
		}()
	}
}
