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
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"log"
	"os"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

const (
	ConsumerGroup = "victory_lite_group001"
	Topic         = "victory_lite_topic001"
	Endpoint      = "11.167.131.190:8081"
	AccessKey     = "xxxxxx"
	SecretKey     = "xxxxxx"
	NameSpace     = ""
)

var (
	// maximum waiting time for receive func
	awaitDuration = time.Second * 5
)

func main() {
	// log to console
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
	rmq_client.EnableSsl = false
	// In most case, you don't need to create many consumers, singleton pattern is more recommended.
	pushConsumer, err := rmq_client.NewLitePushConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
		NameSpace: NameSpace,
	},
		rmq_client.NewLitePushConsumerConfig(Topic, time.Second*30),
		rmq_client.WithPushAwaitDuration(awaitDuration),
		rmq_client.WithPushMessageListener(&rmq_client.FuncMessageListener{
			Consume: func(mv *rmq_client.MessageView) rmq_client.ConsumerResult {
				fmt.Println("received message:", mv.GetMessageId(), mv.GetTopic(), mv.GetLiteTopic(), mv.GetProperties())
				// ack message
				return rmq_client.SUCCESS
			},
		}),
		rmq_client.WithPushConsumptionThreadCount(20),
		rmq_client.WithPushMaxCacheMessageCount(1024),
	)
	if err != nil {
		log.Fatal(err)
		return
	}
	// start pushConsumer
	err = pushConsumer.Start()
	if err != nil {
		log.Fatal(err)
		return
	}
	//time.Sleep(time.Second * 60)
	err = pushConsumer.SubscribeLite("00900")
	if err != nil {
		log.Fatal(err)
		return
	}
	// graceful stop pushConsumer
	defer pushConsumer.GracefulStop()
	// run for a while
	ch := make(chan struct{})
	<-ch
}
