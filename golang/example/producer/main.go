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
	"strconv"
	"time"

	"github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

const (
	Topic         = "v2_grpc"
	ConsumerGroup = "v2_grpc"
	NameSpace     = ""
	Endpoint      = "121.196.167.124:8081"
	AccessKey     = ""
	SecretKey     = ""
)

func main() {
	producer, err := golang.NewProducer(&golang.Config{
		Endpoint: Endpoint,
		Group:    ConsumerGroup,
		Region:   "cn-zhangjiakou",
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.GracefulStop()
	for i := 0; i < 10; i++ {
		msg := &golang.Message{
			Topic: Topic,
			Body:  []byte(strconv.Itoa(i)),
			Tag:   "*",
		}
		// msg.SetDelayTimeLevel(time.Now().Add(time.Second * 10))
		resp, err := producer.Send(context.TODO(), msg)
		if err != nil {
			log.Println(err)
			// return
		}
		for i := 0; i < len(resp); i++ {
			fmt.Printf("%#v\n", resp[i])
		}

		// producer.SendAsync(context.Background(), msg, func(ctx context.Context, resp []*golang.SendReceipt, err error) {
		// 	if err != nil {
		// 		log.Println(err)
		// 		return
		// 	}
		// 	for i := 0; i < len(resp); i++ {
		// 		fmt.Printf("%#v\n", resp[i])
		// 	}
		// })
		time.Sleep(time.Second * 4)
	}
	select {}
}
