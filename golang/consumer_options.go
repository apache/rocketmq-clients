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

type consumerOptions struct {
	tag string
}

var defaultConsumerOptions = consumerOptions{
	tag: "*",
}

// A ConsumerOption sets options such as tag, etc.
type ConsumerOption interface {
	apply(*consumerOptions)
}

// funcOption wraps a function that modifies options into an implementation of
// the Option interface.
type funcConsumerOption struct {
	f func(*consumerOptions)
}

func (fo *funcConsumerOption) apply(do *consumerOptions) {
	fo.f(do)
}

func newFuncConsumerOptionn(f func(*consumerOptions)) *funcConsumerOption {
	return &funcConsumerOption{
		f: f,
	}
}

// WithTag returns a consumerOption that sets tag for consumer.
// Note: Default it uses *.
func WithTag(tag string) ConsumerOption {
	return newFuncConsumerOptionn(func(o *consumerOptions) {
		o.tag = tag
	})
}
