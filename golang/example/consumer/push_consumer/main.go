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

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

const (
	ConsumerGroup = "xxxxxx"
	Topic         = "xxxxxx"
	Endpoint      = "xxxxxx"
	AccessKey     = "xxxxxx"
	SecretKey     = "xxxxxx"
	NameSpace     = "xxxxxx"
)

var (
	// maximum waiting time for receive func
	awaitDuration = time.Second * 5
)

func main() {
	// log to console
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
	// In most case, you don't need to create many consumers, singleton pattern is more recommended.
	pushConsumer, err := rmq_client.NewPushConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
		NameSpace: NameSpace,
	},
		rmq_client.WithPushAwaitDuration(awaitDuration),
		rmq_client.WithPushSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			Topic: rmq_client.SUB_ALL,
		}),
		rmq_client.WithPushMessageListener(&rmq_client.FuncMessageListener{
			Consume: func(mv *rmq_client.MessageView) rmq_client.ConsumerResult {
				fmt.Println(mv)
				// ack message
				return rmq_client.SUCCESS
			},
		}),
		rmq_client.WithPushConsumptionThreadCount(20),
		rmq_client.WithPushMaxCacheMessageCount(1024),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start pushConsumer
	err = pushConsumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop pushConsumer
	defer pushConsumer.GracefulStop()
	// run for a while
	time.Sleep(time.Minute * 100)
}
