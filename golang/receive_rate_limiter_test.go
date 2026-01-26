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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReceiveRateLimiter_BasicLimit(t *testing.T) {
	maxConcurrency := 3
	limiter := newReceiveRateLimiter(maxConcurrency)

	// Test basic limit: should be able to acquire maxConcurrency permits
	ctx := context.Background()
	for i := 0; i < maxConcurrency; i++ {
		err := limiter.acquire(ctx)
		assert.NoError(t, err, "should be able to acquire permit")
	}

	// Try to acquire the (maxConcurrency+1)th permit, should be blocked
	acquired := int32(0)
	go func() {
		err := limiter.acquire(ctx)
		if err == nil {
			atomic.StoreInt32(&acquired, 1)
			limiter.release()
		}
	}()

	// Wait a short time to confirm it was not immediately acquired
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&acquired), "should not acquire permit immediately")

	// Release one permit, should wake up the waiting goroutine
	limiter.release()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&acquired), "should acquire permit after release")
}

func TestReceiveRateLimiter_ConcurrentLimit(t *testing.T) {
	maxConcurrency := 5
	limiter := newReceiveRateLimiter(maxConcurrency)
	ctx := context.Background()

	// Start many goroutines to request permits concurrently
	totalGoroutines := 20
	acquiredCount := int32(0)
	activeCount := int32(0)
	maxActive := int32(0)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < totalGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.acquire(ctx)
			if err != nil {
				return
			}

			// Record current active count
			current := atomic.AddInt32(&activeCount, 1)
			mu.Lock()
			if current > maxActive {
				maxActive = current
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(50 * time.Millisecond)

			atomic.AddInt32(&acquiredCount, 1)
			atomic.AddInt32(&activeCount, -1)
			limiter.release()
		}()
	}

	wg.Wait()

	// Verify: all goroutines should successfully acquire permits
	assert.Equal(t, int32(totalGoroutines), acquiredCount, "all goroutines should successfully acquire permits")

	// Verify: concurrent active count should not exceed maxConcurrency
	assert.LessOrEqual(t, maxActive, int32(maxConcurrency), "concurrent active count should not exceed limit")
}

func TestReceiveRateLimiter_ContextCancel(t *testing.T) {
	maxConcurrency := 2
	limiter := newReceiveRateLimiter(maxConcurrency)

	// Acquire all permits first
	ctx := context.Background()
	for i := 0; i < maxConcurrency; i++ {
		err := limiter.acquire(ctx)
		assert.NoError(t, err)
	}

	// Create a context that will be cancelled
	cancelCtx, cancel := context.WithCancel(context.Background())

	// Try to acquire permit, should be blocked
	acquired := int32(0)
	errChan := make(chan error, 1)
	go func() {
		err := limiter.acquire(cancelCtx)
		errChan <- err
		if err == nil {
			atomic.StoreInt32(&acquired, 1)
			limiter.release()
		}
	}()

	// Wait a short time to confirm it was blocked
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&acquired), "should be blocked")

	// Cancel context
	cancel()

	// Wait for goroutine to return
	select {
	case err := <-errChan:
		assert.Error(t, err, "should return context cancellation error")
		assert.Equal(t, context.Canceled, err, "error should be context.Canceled")
	case <-time.After(1 * time.Second):
		t.Fatal("should return immediately after context cancellation")
	}

	// Release previous permits
	limiter.release()
	limiter.release()
}

func TestReceiveRateLimiter_ReleaseWithoutAcquire(t *testing.T) {
	limiter := newReceiveRateLimiter(5)
	ctx := context.Background()

	// Release without acquiring, should not panic
	assert.NotPanics(t, func() {
		limiter.release()
	})

	// Should be able to acquire permit normally after release
	err := limiter.acquire(ctx)
	assert.NoError(t, err, "should be able to acquire permit normally")
	limiter.release()
}

func TestReceiveRateLimiter_MultipleRelease(t *testing.T) {
	maxConcurrency := 3
	limiter := newReceiveRateLimiter(maxConcurrency)
	ctx := context.Background()

	// Acquire one permit
	err := limiter.acquire(ctx)
	assert.NoError(t, err)

	// Release multiple times, should not panic
	limiter.release()
	assert.NotPanics(t, func() {
		limiter.release()
		limiter.release()
	})

	// Should be able to acquire maxConcurrency permits (because count was reset)
	for i := 0; i < maxConcurrency; i++ {
		err := limiter.acquire(ctx)
		assert.NoError(t, err, "should be able to acquire permit")
	}
}

func TestReceiveRateLimiter_ZeroMaxConcurrency(t *testing.T) {
	// Test that when maxConcurrency is 0 or negative, default value 10 should be used
	limiter := newReceiveRateLimiter(0)
	ctx := context.Background()

	// Should be able to acquire 10 permits
	for i := 0; i < 10; i++ {
		err := limiter.acquire(ctx)
		assert.NoError(t, err, "should be able to acquire permit")
	}

	// The 11th should be blocked
	acquired := int32(0)
	go func() {
		err := limiter.acquire(ctx)
		if err == nil {
			atomic.StoreInt32(&acquired, 1)
			limiter.release()
		}
	}()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&acquired), "should be blocked")

	// Cleanup
	for i := 0; i < 10; i++ {
		limiter.release()
	}
}

func TestReceiveRateLimiter_StressTest(t *testing.T) {
	maxConcurrency := 10
	limiter := newReceiveRateLimiter(maxConcurrency)
	ctx := context.Background()

	// Stress test: large number of concurrent requests
	totalRequests := 1000
	successCount := int32(0)
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.acquire(ctx)
			if err != nil {
				return
			}
			atomic.AddInt32(&successCount, 1)
			time.Sleep(1 * time.Millisecond) // Simulate work
			limiter.release()
		}()
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Verify all requests succeeded
	assert.Equal(t, int32(totalRequests), successCount, "all requests should succeed")

	// Verify rate limiting works: without rate limiting, 1000 requests should complete quickly
	// With rate limiting, it should take longer
	// This is just a simple verification, actual time depends on scheduling
	t.Logf("Completed %d requests in %v", totalRequests, duration)
	assert.Greater(t, duration, 50*time.Millisecond, "with rate limiting, it should take some time")
}

func TestReceiveRateLimiter_SequentialAcquireRelease(t *testing.T) {
	limiter := newReceiveRateLimiter(3)
	ctx := context.Background()

	// Sequential acquire and release
	for i := 0; i < 10; i++ {
		err := limiter.acquire(ctx)
		assert.NoError(t, err, "should be able to acquire permit")
		limiter.release()
	}

	// Verify can acquire again
	err := limiter.acquire(ctx)
	assert.NoError(t, err, "should be able to acquire permit again")
	limiter.release()
}
