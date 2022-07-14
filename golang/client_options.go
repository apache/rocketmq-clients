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
	"time"
)

type clientOptions struct {
	tickerDuration   time.Duration
	timeout          time.Duration
	clientConnFunc   ClientConnFunc
	connOptions      []ConnOption
	rpcClientOptions []RpcClientOption
}

var defaultNSOptions = clientOptions{
	tickerDuration: time.Second * 30,
	timeout:        time.Millisecond * 3000,
	clientConnFunc: NewClientConn,
}

// A ClientOption sets options such as timeout, etc.
type ClientOption interface {
	apply(*clientOptions)
}

// funcNSOption wraps a function that modifies options into an implementation of
// the ClientOption interface.
type funcNSOption struct {
	f func(options *clientOptions)
}

func (fpo *funcNSOption) apply(po *clientOptions) {
	fpo.f(po)
}

func newFuncNSOption(f func(options *clientOptions)) *funcNSOption {
	return &funcNSOption{
		f: f,
	}
}

// WithQueryRouteTimeout returns a Option that sets timeout duration for nameserver.
// Default is 3s.
func WithQueryRouteTimeout(d time.Duration) ClientOption {
	return newFuncNSOption(func(o *clientOptions) {
		o.timeout = d
	})
}

// WithTickerDuration returns a Option that sets ticker duration for nameserver.
// Default is 30s.
func WithTickerDuration(d time.Duration) ClientOption {
	return newFuncNSOption(func(o *clientOptions) {
		o.tickerDuration = d
	})
}

// WithClientConnFunc returns a Option that sets ClientConnFunc for nameserver.
// Default is NewClientConn.
func WithClientConnFunc(f ClientConnFunc) ClientOption {
	return newFuncNSOption(func(o *clientOptions) {
		o.clientConnFunc = f
	})
}

// WithConnOptions returns a Option that sets ConnOption for grpc ClientConn.
func WithConnOptions(opts ...ConnOption) ClientOption {
	return newFuncNSOption(func(o *clientOptions) {
		o.connOptions = append(o.connOptions, opts...)
	})
}

// WithRpcClientOptions returns a Option that sets RpcClientOption for grpc ClientConn.
func WithRpcClientOptions(opts ...RpcClientOption) ClientOption {
	return newFuncNSOption(func(o *clientOptions) {
		o.rpcClientOptions = append(o.rpcClientOptions, opts...)
	})
}
