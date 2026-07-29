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
	"context"
	"fmt"
	"log"
	"os"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

const (
	ConsumerGroup = "xxxxxx"
	BindTopic     = "xxxxxx"
	LiteTopic     = "xxxxxx"
	Endpoint      = "xxxxxx"
	AccessKey     = "xxxxxx"
	SecretKey     = "xxxxxx"
	NameSpace     = ""
)

var (
	// maximum waiting time for receive func
	awaitDuration = time.Second * 5
	// maximum number of messages received at one time
	maxMessageNum int32 = 16
	// invisibleDuration should > 20s
	invisibleDuration = time.Second * 20
)

func main() {
	// log to console
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
	// In most case, you don't need to create many consumers, singleton pattern is more recommended.
	liteSimpleConsumer, err := rmq_client.NewLiteSimpleConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
		NameSpace: NameSpace,
	},
		rmq_client.NewLiteSimpleConsumerConfig(BindTopic),
		rmq_client.WithSimpleAwaitDuration(awaitDuration),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start liteSimpleConsumer
	err = liteSimpleConsumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop liteSimpleConsumer
	defer liteSimpleConsumer.GracefulStop()

	// subscribe a lite topic
	err = liteSimpleConsumer.SubscribeLite(LiteTopic)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			fmt.Println("start receive message")
			mvs, err := liteSimpleConsumer.Receive(context.TODO(), maxMessageNum, invisibleDuration)
			if err != nil {
				fmt.Println(err)
			}
			// ack message
			for _, mv := range mvs {
				liteSimpleConsumer.Ack(context.TODO(), mv)
				fmt.Println("received message:", mv.GetMessageId(), mv.GetTopic(), mv.GetLiteTopic(), mv.GetProperties())
			}
			fmt.Println("wait a moment")
			fmt.Println()
			time.Sleep(time.Second * 3)
		}
	}()
	// run for a while
	time.Sleep(time.Minute)
}
