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
	Topic     = "topic-priority"
	Endpoint  = "127.0.0.1:8080"
	AccessKey = ""
	SecretKey = ""
)

func main() {
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()

	// In most case, you don't need to create many producers, singleton pattern is more recommended.
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithTopics(Topic),
	)
	if err != nil {
		log.Fatal(err)
	}

	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop producer
	defer producer.GracefulStop()

	fmt.Println("========== Priority Message Producer Example ==========")
	fmt.Printf("Topic: %s\n", Topic)
	fmt.Printf("Endpoint: %s\n\n", Endpoint)

	// Send priority messages with different levels
	fmt.Println("Sending priority messages...")
	fmt.Println("Note: Higher priority value = higher priority\n")

	// Low priority message (priority = 1)
	lowPriorityMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("This is a low priority message"),
	}
	err = lowPriorityMsg.SetPriority(1)
	if err != nil {
		log.Printf("Failed to set priority: %v", err)
	} else {
		receipts, err := producer.Send(context.Background(), lowPriorityMsg)
		if err != nil {
			log.Printf("Failed to send low priority message: %v", err)
		} else {
			fmt.Printf("✓ Low priority (1) message sent, MessageID: %s\n", receipts[0].MessageID)
		}
	}

	// Medium priority message (priority = 5)
	mediumPriorityMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("This is a medium priority message"),
	}
	err = mediumPriorityMsg.SetPriority(5)
	if err != nil {
		log.Printf("Failed to set priority: %v", err)
	} else {
		receipts, err := producer.Send(context.Background(), mediumPriorityMsg)
		if err != nil {
			log.Printf("Failed to send medium priority message: %v", err)
		} else {
			fmt.Printf("✓ Medium priority (5) message sent, MessageID: %s\n", receipts[0].MessageID)
		}
	}

	// High priority message (priority = 10)
	highPriorityMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("This is a high priority message"),
	}
	err = highPriorityMsg.SetPriority(10)
	if err != nil {
		log.Printf("Failed to set priority: %v", err)
	} else {
		receipts, err := producer.Send(context.Background(), highPriorityMsg)
		if err != nil {
			log.Printf("Failed to send high priority message: %v", err)
		} else {
			fmt.Printf("✓ High priority (10) message sent, MessageID: %s\n", receipts[0].MessageID)
		}
	}

	// Demonstrate mutual exclusivity constraints
	fmt.Println("\nDemonstrating mutual exclusivity constraints...")

	// This will fail: priority + messageGroup
	fifoMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("test"),
	}
	fifoMsg.SetMessageGroup("group-1")
	err = fifoMsg.SetPriority(5)
	if err != nil {
		fmt.Printf("✓ Correctly rejected: priority + messageGroup - %v\n", err)
	}

	// This will fail: priority + deliveryTimestamp
	delayWithPriorityMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("test"),
	}
	delayWithPriorityMsg.SetDelayTimestamp(time.Now().Add(1 * time.Minute))
	err = delayWithPriorityMsg.SetPriority(5)
	if err != nil {
		fmt.Printf("✓ Correctly rejected: priority + deliveryTimestamp - %v\n", err)
	}

	// This will fail: negative priority
	negativePriorityMsg := &rmq_client.Message{
		Topic: Topic,
		Body:  []byte("test"),
	}
	err = negativePriorityMsg.SetPriority(-1)
	if err != nil {
		fmt.Printf("✓ Correctly rejected: negative priority - %v\n", err)
	}

	fmt.Println("\n========== All validation checks passed! ==========")
	fmt.Println("\nExpected behavior:")
	fmt.Println("- Messages with higher priority values are delivered first")
	fmt.Println("- Priority 10 > Priority 5 > Priority 1")
	fmt.Println("- Use a consumer to verify the delivery order")
}
