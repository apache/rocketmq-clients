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

import (
	"context"
	"sync"
)

type receiveRateLimiter struct {
	mu             sync.Mutex
	cond           *sync.Cond
	maxConcurrency int
	currentCount   int
}

func newReceiveRateLimiter(maxConcurrency int) *receiveRateLimiter {
	if maxConcurrency <= 0 {
		maxConcurrency = 10 // default 10 concurrent requests
	}
	rl := &receiveRateLimiter{
		maxConcurrency: maxConcurrency,
	}
	rl.cond = sync.NewCond(&rl.mu)
	return rl
}

func (rl *receiveRateLimiter) acquire(ctx context.Context) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for rl.currentCount >= rl.maxConcurrency {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		waitDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				rl.cond.Broadcast()
			case <-waitDone:
			}
		}()

		rl.cond.Wait()
		close(waitDone)

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	rl.currentCount++
	return nil
}

func (rl *receiveRateLimiter) release() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.currentCount > 0 {
		rl.currentCount--
		rl.cond.Signal()
	}
}
