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

type producerOptions struct {
	clientFunc NewClientFunc
}

var defaultProducerOptions = producerOptions{
	clientFunc: NewClient,
}

// A ProducerOption sets options such as tls.Config, etc.
type ProducerOption interface {
	apply(*producerOptions)
}

// funcProducerOption wraps a function that modifies options into an implementation of
// the ConnOption interface.
type funcProducerOption struct {
	f func(options *producerOptions)
}

func (fpo *funcProducerOption) apply(po *producerOptions) {
	fpo.f(po)
}

func newFuncProducerOption(f func(options *producerOptions)) *funcProducerOption {
	return &funcProducerOption{
		f: f,
	}
}

// WithClientFunc returns a ProducerOption that sets ClientFunc for producer.
// Default is nameserver.New.
func WithClientFunc(f NewClientFunc) ProducerOption {
	return newFuncProducerOption(func(o *producerOptions) {
		o.clientFunc = f
	})
}
