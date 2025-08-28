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
	"sync"

	"go.uber.org/atomic"
)

type simpleThreadPool struct {
	name      string
	tasks     chan func()
	shutdown  chan any
	waitGroup sync.WaitGroup
	once      sync.Once
	running   atomic.Bool
}

func NewSimpleThreadPool(poolName string, taskSize int, threadNum int) *simpleThreadPool {
	r := &simpleThreadPool{
		name:     poolName,
		tasks:    make(chan func(), taskSize),
		shutdown: make(chan any),
		running:  *atomic.NewBool(true),
	}
	for i := 0; i < threadNum; i++ {
		r.waitGroup.Add(1)
		go func() {
			defer r.waitGroup.Done()
			tp := r
			for {
				select {
				case <-tp.shutdown:
					sugarBaseLogger.Infof("routine pool is shutdown, name=%s", tp.name)
					// complete all remaining tasks
					for t := range tp.tasks {
						t()
					}
					return
				case t := <-tp.tasks:
					if t != nil {
						t()
					}
				}
			}
		}()
	}
	return r
}

func (tp *simpleThreadPool) Submit(task func()) {
	defer func() {
		if r := recover(); r != nil {
			// the running flag may have concurrency security, here is a fallback
			sugarBaseLogger.Warnf("recover: simple thread pool [%s], task=%v, err=%v", tp.name, task, r)
		}
	}()
	if !tp.running.Load() {
		sugarBaseLogger.Warnf("simple thread pool [%s] is not running, task=%v", tp.name, task)
		return
	}
	tp.tasks <- task
}
func (tp *simpleThreadPool) Shutdown() {
	tp.running.Store(false)
	tp.once.Do(func() {
		close(tp.shutdown)
		// do not accept other task
		close(tp.tasks)
		tp.waitGroup.Wait()
	})
}
