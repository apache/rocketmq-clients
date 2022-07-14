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

// import (
// 	"context"
// 	"io"
// 	"reflect"
// 	"sync"
// 	"time"

// 	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
// 	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
// )

// type Consumer interface {
// 	Start() error
// 	Consume(ctx context.Context, topic string, handler Handler) error
// 	GracefulStop() error
// }

// var _ = Consumer(&consumer{})

// type consumer struct {
// 	opts       consumerOptions
// 	ns         NameServer
// 	assignment sync.Map
// 	handlers   map[string]Handler
// 	done       chan struct{}
// }

// type Handler func(ctx context.Context, entry *MessageExt) error

// func NewConsumer(config *Config, opts ...ConsumerOption) (Consumer, error) {
// 	ns, err := NewNameServer(config)
// 	if err != nil {
// 		return nil, err
// 	}
// 	c := &consumer{
// 		opts:     defaultConsumerOptions,
// 		handlers: make(map[string]Handler),
// 		done:     make(chan struct{}),
// 	}
// 	for _, opt := range opts {
// 		opt.apply(&c.opts)
// 	}

// 	c.ns = ns
// 	return c, nil
// }

// func (c *consumer) Start() error {
// 	c.assignment.Range(func(k, v interface{}) bool {
// 		topic, ok := k.(string)
// 		if ok {
// 			handler := c.handlers[topic]
// 			f := func() {
// 				ctx, _ := context.WithTimeout(context.TODO(), time.Second*20)
// 				b, err := c.ns.GetBroker(ctx, topic)
// 				if err != nil {
// 					return
// 				}
// 				assign, ok := v.([]*v2.Assignment)
// 				if !ok || len(assign) == 0 {
// 					return
// 				}
// 				var wg sync.WaitGroup
// 				for i := 0; i < len(assign); i++ {
// 					wg.Add(1)
// 					i := i
// 					go func() {
// 						defer wg.Done()
// 						stream, err := b.ReceiveMessage(ctx, assign[i].MessageQueue, topic)
// 						if err != nil {
// 							return
// 						}
// 						for {
// 							resp, err := stream.Recv()
// 							if err == io.EOF {
// 								break
// 							}
// 							if err != nil {
// 								time.Sleep(time.Second * 5)
// 								continue
// 							}
// 							msg, ok := resp.GetContent().(*v2.ReceiveMessageResponse_Message)
// 							if !ok {
// 								continue
// 							}
// 							messageExt := &MessageExt{
// 								MessageID:     msg.Message.GetSystemProperties().GetMessageId(),
// 								ReceiptHandle: msg.Message.GetSystemProperties().GetReceiptHandle(),
// 								Message: Message{
// 									Topic:      msg.Message.GetTopic().GetName(),
// 									Body:       msg.Message.GetBody(),
// 									Keys:       msg.Message.GetSystemProperties().GetKeys(),
// 									Tag:        msg.Message.GetSystemProperties().GetTag(),
// 									Properties: msg.Message.GetUserProperties(),
// 								},
// 							}
// 							if err = handler(ctx, messageExt); err == nil {
// 								_ = b.AckMessage(ctx, messageExt)
// 							}
// 						}
// 					}()
// 				}
// 				wg.Wait()
// 			}
// 			ticker.Tick(f, time.Second*20, c.done)
// 		}
// 		return true
// 	})
// 	<-c.done
// 	return nil
// }

// func (c *consumer) Consume(ctx context.Context, topic string, handler Handler) error {
// 	if err := c.queryAssignment(ctx, topic); err != nil {
// 		return err
// 	}

// 	c.handlers[topic] = handler
// 	return nil
// }

// func (c *consumer) queryAssignment(ctx context.Context, topic string) error {
// 	b, err := c.ns.GetBroker(ctx, topic)
// 	if err != nil {
// 		return err
// 	}
// 	assignment, err := b.QueryAssignment(ctx, topic)
// 	if err != nil {
// 		return err
// 	}
// 	c.assignment.Store(topic, assignment)
// 	f := func() {
// 		assign, err := b.QueryAssignment(ctx, topic)
// 		if err != nil {
// 			return
// 		}
// 		cache, ok := c.assignment.Load(topic)
// 		if !ok {
// 			return
// 		}
// 		oldAssign, ok := cache.([]*v2.Assignment)
// 		if !ok {
// 			return
// 		}
// 		if reflect.DeepEqual(assign, oldAssign) {
// 			return
// 		}
// 		c.assignment.Store(topic, assign)
// 	}
// 	ticker.Tick(f, time.Second*10, c.done)
// 	return nil
// }

// func (c *consumer) GracefulStop() error {
// 	defer close(c.done)
// 	c.done <- struct{}{}
// 	return c.ns.GracefulStop()
// }
