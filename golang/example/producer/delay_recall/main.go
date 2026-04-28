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
	Topic     = "topic-delay-new"
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

	fmt.Println("========== Delay Message Recall Example ==========")
	fmt.Printf("Topic: %s\n", Topic)
	fmt.Printf("Endpoint: %s\n\n", Endpoint)

	// Send delay messages with recall capability
	fmt.Println("Sending delay messages that can be recalled...")
	fmt.Println("Messages will be delivered after 10 minutes if not recalled\n")

	var recallHandles []string

	for i := 1; i <= 3; i++ {
		delayMsg := &rmq_client.Message{
			Topic: Topic,
			Body:  []byte(fmt.Sprintf("Delay message %d - scheduled for delivery at %s", i, time.Now().Add(10*time.Minute).Format("15:04:05"))),
		}

		// Set delivery timestamp to 10 minutes later
		deliveryTime := time.Now().Add(10 * time.Minute)
		delayMsg.SetDelayTimestamp(deliveryTime)

		receipts, err := producer.Send(context.Background(), delayMsg)
		if err != nil {
			log.Printf("Failed to send delay message %d: %v", i, err)
		} else {
			fmt.Printf("✓ Delay message %d sent:\n", i)
			fmt.Printf("    MessageID: %s\n", receipts[0].MessageID)
			fmt.Printf("    RecallHandle: %s\n", receipts[0].RecallHandle)
			fmt.Printf("    Delivery Time: %s\n", deliveryTime.Format("15:04:05"))

			if receipts[0].RecallHandle != "" {
				recallHandles = append(recallHandles, receipts[0].RecallHandle)
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println("\n========== All delay messages sent ==========")

	// Recall the first delay message
	if len(recallHandles) > 0 {
		fmt.Println("\nWaiting 3 seconds before recalling the first message...")
		time.Sleep(3 * time.Second)

		fmt.Printf("\nAttempting to recall message 1...\n")
		recalledMessageID, err := producer.Recall(context.Background(), Topic, recallHandles[0])
		if err != nil {
			fmt.Printf("✗ Failed to recall message: %v\n", err)
			fmt.Println("\nPossible reasons:")
			fmt.Println("  - The message has already been delivered")
			fmt.Println("  - The recall_handle is invalid")
			fmt.Println("  - Network issues")
		} else {
			fmt.Printf("✓ Message recalled successfully!\n")
			fmt.Printf("    Recalled MessageID: %s\n", recalledMessageID)
		}
	}

	fmt.Println("\n========== Test Complete ==========")
	fmt.Println("\nNext steps:")
	fmt.Println("1. Start consumer 'GID-normal-consumer_topic-normal' to receive remaining messages")
	fmt.Println("2. Messages should be delivered ~10 minutes after sending")
	fmt.Println("3. Message 1 was recalled and should NOT be delivered")
	fmt.Println("4. Messages 2 and 3 should be delivered on schedule")
}
