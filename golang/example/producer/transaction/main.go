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
	"strconv"
	"time"

	"github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

const (
	// Topic         = "xxxxxx"
	// ConsumerGroup = "xxxxxx"
	// Endpoint      = "xxxxxx"
	// Region        = "xxxxxx"
	// AccessKey     = "xxxxxx"
	// SecretKey     = "xxxxxx"

	Topic         = "v2_grpc_transaction"
	ConsumerGroup = "v2_grpc"
	Endpoint      = "rmq-cn-tl32tb4s50g.cn-hangzhou.rmq.aliyuncs.com:8080"
	Region        = "cn-zhangjiakou"
	AccessKey     = "hM71WHX8Sx0OfGXR"
	SecretKey     = "2vyd43Kii50h6lzH"
)

func main() {
	// log to console
	os.Setenv("mq.consoleAppender.enabled", "true")
	golang.ResetLogger()
	// new producer instance
	producer, err := golang.NewProducer(&golang.Config{
		Endpoint: Endpoint,
		Group:    ConsumerGroup,
		Region:   Region,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		golang.WithTransactionChecker(&golang.TransactionChecker{
			Check: func(msg *golang.MessageView) golang.TransactionResolution {
				log.Printf("check transaction message: %v", msg)
				return golang.COMMIT
			},
		}),
		golang.WithTopics(Topic),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// gracefule stop producer
	defer producer.GracefulStop()
	for i := 0; i < 10; i++ {
		// new a message
		msg := &golang.Message{
			Topic: Topic,
			Body:  []byte("this is a message : " + strconv.Itoa(i)),
		}
		// set keys and tag
		msg.SetKeys("a", "b")
		msg.SetTag("ab")
		// send message in sync
		transaction := producer.BeginTransaction()
		resp, err := producer.SendWithTransaction(context.TODO(), msg, transaction)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			fmt.Printf("%#v\n", resp[i])
		}
		// commit transaction message
		err = transaction.Commit()
		if err != nil {
			log.Fatal(err)
		}
		// wait a moment
		time.Sleep(time.Second * 1)
	}
}
