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

type simpleThreadPool struct {
	name     string
	tasks    chan func()
	shutdown chan any
}

func NewSimpleThreadPool(poolName string, taskSize int, threadNum int) *simpleThreadPool {
	r := &simpleThreadPool{
		name:     poolName,
		tasks:    make(chan func(), taskSize),
		shutdown: make(chan any),
	}
	for i := 0; i < threadNum; i++ {
		go func() {
			tp := r
			for {
				select {
				case <-tp.shutdown:
					sugarBaseLogger.Infof("routine pool is shutdown, name=%s", tp.name)
					return
				case t := <-tp.tasks:
					t()
				}
			}
		}()
	}
	return r
}

func (tp *simpleThreadPool) Submit(task func()) {
	tp.tasks <- task
}
func (tp *simpleThreadPool) Shutdown() {
	tp.shutdown <- 0
	close(tp.shutdown)
}
