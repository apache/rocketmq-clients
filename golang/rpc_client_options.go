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

type rpcClientOptions struct {
	heartbeatDuration   time.Duration
	healthCheckDuration time.Duration
	timeout             time.Duration
	clientConnFunc      ClientConnFunc
	connOptions         []ConnOption
}

var defaultRpcClientOptions = rpcClientOptions{
	heartbeatDuration:   time.Second * 10,
	healthCheckDuration: time.Second * 15,
	timeout:             time.Second * 5,
	clientConnFunc:      NewClientConn,
}

// A RpcClientOption sets options such as tls.Config, etc.
type RpcClientOption interface {
	apply(*rpcClientOptions)
}

// funcRpcClientOption wraps a function that modifies options into an implementation of
// the ConnOption interface.
type funcRpcClientOption struct {
	f func(options *rpcClientOptions)
}

func (fpo *funcRpcClientOption) apply(po *rpcClientOptions) {
	fpo.f(po)
}

func newFuncOption(f func(options *rpcClientOptions)) *funcRpcClientOption {
	return &funcRpcClientOption{
		f: f,
	}
}

// WithRpcClientClientConnFunc returns a RpcClientOption that sets ClientConnFunc for RpcClient.
// Default is NewClientConn.
func WithRpcClientClientConnFunc(f ClientConnFunc) RpcClientOption {
	return newFuncOption(func(o *rpcClientOptions) {
		o.clientConnFunc = f
	})
}

// WithRpcClientConnOption returns a RpcClientOption that sets ConnOption for RpcClient.
func WithRpcClientConnOption(opts ...ConnOption) RpcClientOption {
	return newFuncOption(func(o *rpcClientOptions) {
		o.connOptions = append(o.connOptions, opts...)
	})
}

// WithHeartbeatDuration returns a RpcClientOption that sets heartbeatDuration for RpcClient.
// Default is 10s.
func WithHeartbeatDuration(d time.Duration) RpcClientOption {
	return newFuncOption(func(o *rpcClientOptions) {
		o.heartbeatDuration = d
	})
}

// WithHealthCheckDuration returns a RpcClientOption that sets healthCheckDuration for RpcClient.
// Default is 15s.
func WithHealthCheckDuration(d time.Duration) RpcClientOption {
	return newFuncOption(func(o *rpcClientOptions) {
		o.healthCheckDuration = d
	})
}

// WithRpcClientTimeout returns a RpcClientOption that sets time for RpcClient when heartbeat and health check.
// Default is 5s.
func WithRpcClientTimeout(d time.Duration) RpcClientOption {
	return newFuncOption(func(o *rpcClientOptions) {
		o.timeout = d
	})
}
