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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// Fill in the following parameters before running this example:
//   - Endpoint:    gRPC endpoint of your RocketMQ instance, e.g. "127.0.0.1:8080".
//   - AccessKey:   access key for authentication (leave empty if ACL is disabled).
//   - SecretKey:   secret key for authentication (leave empty if ACL is disabled).
//   - NormalTopic: an existing NORMAL topic on the instance.
//   - FIFOTopic:   an existing FIFO topic on the instance.
//   - SimpleGroup: consumer group for the SimpleConsumer of the normal topic.
//   - PushGroup:   consumer group for the PushConsumer of the normal topic.
//   - FIFOGroup:   consumer group for the FIFO SimpleConsumer.
//
// Never commit real credentials to the repository.
const (
	Endpoint  = ""
	AccessKey = ""
	SecretKey = ""

	NormalTopic = ""
	FIFOTopic   = ""

	SimpleGroup = ""
	PushGroup   = ""
	FIFOGroup   = ""
)

var (
	normalSent   atomic.Int64
	normalRecv   atomic.Int64
	fifoSent     atomic.Int64
	fifoRecv     atomic.Int64
	fifoOrderErr atomic.Int64
	pushRecv     atomic.Int64
)

func main() {
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()

	fmt.Println("====== RocketMQ Go Client Continuous Run Test ======")
	fmt.Printf("Endpoint: %s\n", Endpoint)
	fmt.Println("Press Ctrl+C to exit")
	fmt.Println()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start them one by one to avoid concurrent connection timeouts.
	go runNormalProducer(ctx)
	time.Sleep(time.Second)
	go runSimpleConsumer(ctx)
	time.Sleep(time.Second)
	go runPushConsumer(ctx)
	time.Sleep(time.Second)
	go runFIFOProducer(ctx)
	time.Sleep(time.Second)
	go runFIFOConsumer(ctx)

	// Print statistics periodically.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("\n[Stats] Normal: sent=%d recv=%d | FIFO: sent=%d recv=%d out-of-order=%d | PushConsumer recv=%d\n\n",
					normalSent.Load(), normalRecv.Load(),
					fifoSent.Load(), fifoRecv.Load(), fifoOrderErr.Load(),
					pushRecv.Load(),
				)
			}
		}
	}()

	// Wait for the exit signal.
	sig := <-sigCh
	fmt.Printf("\nReceived signal %v, exiting...\n", sig)
	cancel()
	time.Sleep(3 * time.Second)
}

func runNormalProducer(ctx context.Context) {
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	}, rmq_client.WithTopics(NormalTopic))
	if err != nil {
		fmt.Printf("[Producer] failed to create: %v\n", err)
		return
	}
	if err := producer.Start(); err != nil {
		fmt.Printf("[Producer] failed to start: %v\n", err)
		return
	}
	defer producer.GracefulStop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg := &rmq_client.Message{
			Topic: NormalTopic,
			Body:  []byte(fmt.Sprintf("normal-%d", seq)),
		}
		_, err := producer.Send(ctx, msg)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Printf("[Producer] failed to send: %v\n", err)
			time.Sleep(time.Second)
			continue
		}
		normalSent.Add(1)
		seq++
		time.Sleep(500 * time.Millisecond)
	}
}

func runSimpleConsumer(ctx context.Context) {
	consumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: SimpleGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithSimpleAwaitDuration(5*time.Second),
		rmq_client.WithSimpleSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			NormalTopic: rmq_client.SUB_ALL,
		}),
	)
	if err != nil {
		fmt.Printf("[SimpleConsumer] failed to create: %v\n", err)
		return
	}
	if err := consumer.Start(); err != nil {
		fmt.Printf("[SimpleConsumer] failed to start: %v\n", err)
		return
	}
	defer consumer.GracefulStop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		recvCtx, recvCancel := context.WithTimeout(ctx, 15*time.Second)
		mvs, err := consumer.Receive(recvCtx, 16, 20*time.Second)
		recvCancel()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for _, mv := range mvs {
			fmt.Printf("[SimpleConsumer] id=%s body=%s\n", mv.GetMessageId(), string(mv.GetBody()))
			consumer.Ack(context.Background(), mv)
			normalRecv.Add(1)
		}
	}
}

func runPushConsumer(ctx context.Context) {
	consumer, err := rmq_client.NewPushConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: PushGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithPushAwaitDuration(5*time.Second),
		rmq_client.WithPushSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			NormalTopic: rmq_client.SUB_ALL,
		}),
		rmq_client.WithPushMessageListener(&rmq_client.FuncMessageListener{
			Consume: func(mv *rmq_client.MessageView) rmq_client.ConsumerResult {
				fmt.Printf("[PushConsumer] id=%s body=%s\n", mv.GetMessageId(), string(mv.GetBody()))
				pushRecv.Add(1)
				return rmq_client.SUCCESS
			},
		}),
	)
	if err != nil {
		fmt.Printf("[PushConsumer] failed to create: %v\n", err)
		return
	}
	if err := consumer.Start(); err != nil {
		fmt.Printf("[PushConsumer] failed to start: %v\n", err)
		return
	}
	defer consumer.GracefulStop()

	<-ctx.Done()
}

func runFIFOProducer(ctx context.Context) {
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	}, rmq_client.WithTopics(FIFOTopic))
	if err != nil {
		fmt.Printf("[FIFO Producer] failed to create: %v\n", err)
		return
	}
	if err := producer.Start(); err != nil {
		fmt.Printf("[FIFO Producer] failed to start: %v\n", err)
		return
	}
	defer producer.GracefulStop()

	groups := []string{"fifo-A", "fifo-B"}
	seqs := make(map[string]int)
	for _, g := range groups {
		seqs[g] = 0
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		for _, group := range groups {
			seq := seqs[group]
			msg := &rmq_client.Message{
				Topic: FIFOTopic,
				Body:  []byte(fmt.Sprintf("%s-seq-%d", group, seq)),
			}
			msg.SetMessageGroup(group)
			_, err := producer.Send(ctx, msg)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("[FIFO Producer] failed to send: %v\n", err)
				time.Sleep(time.Second)
				continue
			}
			fifoSent.Add(1)
			seqs[group] = seq + 1
		}
		time.Sleep(time.Second)
	}
}

func runFIFOConsumer(ctx context.Context) {
	consumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: FIFOGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithSimpleAwaitDuration(5*time.Second),
		rmq_client.WithSimpleSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			FIFOTopic: rmq_client.SUB_ALL,
		}),
	)
	if err != nil {
		fmt.Printf("[FIFO SimpleConsumer] failed to create: %v\n", err)
		return
	}
	if err := consumer.Start(); err != nil {
		fmt.Printf("[FIFO SimpleConsumer] failed to start: %v\n", err)
		return
	}
	defer consumer.GracefulStop()

	var mu sync.Mutex
	lastSeq := make(map[string]int) // group -> last received seq
	firstMsg := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		recvCtx, recvCancel := context.WithTimeout(ctx, 15*time.Second)
		mvs, err := consumer.Receive(recvCtx, 16, 20*time.Second)
		recvCancel()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for _, mv := range mvs {
			body := string(mv.GetBody())
			parts := strings.SplitN(body, "-seq-", 2)
			if len(parts) == 2 {
				seq, _ := strconv.Atoi(parts[1])
				group := parts[0]

				mu.Lock()
				if !firstMsg[group] {
					firstMsg[group] = true
					lastSeq[group] = seq
				} else {
					if seq <= lastSeq[group] {
						fifoOrderErr.Add(1)
						fmt.Printf("[FIFO] out of order! group=%s prev=%d cur=%d\n", group, lastSeq[group], seq)
					}
					lastSeq[group] = seq
				}
				mu.Unlock()

				fmt.Printf("[FIFOConsumer] group=%s seq=%d body=%s\n", group, seq, body)
			}
			consumer.Ack(context.Background(), mv)
			fifoRecv.Add(1)
		}
	}
}
