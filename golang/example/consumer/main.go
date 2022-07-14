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

// import (
// 	"context"
// 	"fmt"
// 	"log"

// 	"github.com/apache/rocketmq-clients/golang"
// 	"github.com/apache/rocketmq-clients/golang/credentials"
// )

// const (
// 	Topic         = "v2_grpc"
// 	ConsumerGroup = "v2_grpc"
// 	NameSpace     = ""
// 	Endpoint      = "121.196.167.124:8081"
// 	AccessKey     = ""
// 	SecretKey     = ""
// )

// func main() {
// 	consumer, err := golang.NewConsumer(&golang.Config{
// 		Endpoint:  Endpoint,
// 		NameSpace: NameSpace,
// 		Group:     ConsumerGroup,
// 		Region:    "cn-zhangjiakou",
// 		Credentials: &credentials.SessionCredentials{
// 			AccessKey:    AccessKey,
// 			AccessSecret: SecretKey,
// 		},
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer consumer.GracefulStop()
// 	if err := consumer.Consume(context.TODO(), Topic, func(ctx context.Context, entry *golang.MessageExt) error {
// 		fmt.Printf("%#v\n", entry)
// 		return nil
// 	}); err != nil {
// 		log.Fatal(err)
// 	}
// 	consumer.Start()
// }
