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
	"crypto/tls"
	"crypto/x509"
	"math"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5/pkg/zaplog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type connOptions struct {
	// MaxCallSendMsgSize is the client-side request send limit in bytes.
	// If 0, it defaults to 2.0 MiB (2 * 1024 * 1024).
	// Make sure that "MaxCallSendMsgSize" < server-side default send/recv limit.
	MaxCallSendMsgSize int

	// MaxCallRecvMsgSize is the client-side response receive limit.
	// If 0, it defaults to "math.MaxInt32", because range response can
	// easily exceed request send limits.
	// Make sure that "MaxCallRecvMsgSize" >= server-side default send/recv limit.
	MaxCallRecvMsgSize int

	// TLS holds the client secure credentials, if any.
	TLS *tls.Config

	// DialOptions is a list of dial options for the grpc client (e.g., for interceptors).
	// For example, pass "grpc.WithBlock()" to block until the underlying connection is up.
	// Without this, Dial returns immediately and connecting the server happens in background.
	DialOptions []grpc.DialOption

	// Context is the default client context; it can be used to cancel grpc dial out and
	// other operations that do not have an explicit context.
	Context context.Context

	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout time.Duration

	// Logger is logger
	Logger *zap.Logger

	// EnableSsl indicates whether to enable SSL/TLS for the connection.
	EnableSsl bool
}

var defaultConnOptions = connOptions{
	DialTimeout:        time.Second * 5,
	MaxCallSendMsgSize: 2 * 1024 * 1024,
	MaxCallRecvMsgSize: math.MaxInt32,
	TLS: &tls.Config{
		RootCAs:            x509.NewCertPool(),
		InsecureSkipVerify: true,
	},
	Logger:    zaplog.New(),
	EnableSsl: true,
}

// A ConnOption sets options such as tls.Config, etc.
type ConnOption interface {
	apply(*connOptions)
}

// funcConnOption wraps a function that modifies options into an implementation of
// the ConnOption interface.
type funcConnOption struct {
	f func(options *connOptions)
}

func (fco *funcConnOption) apply(co *connOptions) {
	fco.f(co)
}

func newFuncConnOption(f func(options *connOptions)) *funcConnOption {
	return &funcConnOption{
		f: f,
	}
}

// WithTLSConfig returns a ConnOption that sets tls.Config for grpc.DialContext.
// Default it is x509 insecure tls.Config.
func WithTLSConfig(tc *tls.Config) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.TLS = tc
	})
}

// WithDialTimeout returns a ConnOption that sets DialTimeout for grpc.DialContext.
// Default it is 5 second.
func WithDialTimeout(dur time.Duration) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.DialTimeout = dur
	})
}

// WithMaxCallSendMsgSize returns a ConnOption that sets  the client-side response
// receive limit. If 0, it defaults to "math.MaxInt32", because range response can
// easily exceed request send limits. Make sure that "MaxCallRecvMsgSize" >= server-side
// default send/recv limit.
func WithMaxCallSendMsgSize(size int) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		if size > 0 {
			o.MaxCallSendMsgSize = size
		}
	})
}

// WithMaxCallRecvMsgSize returns a ConnOption that sets client-side request send limit in
// bytes for grpc.DialContext.
func WithMaxCallRecvMsgSize(size int) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		if size > 0 {
			o.MaxCallRecvMsgSize = size
		}
	})
}

// WithDialOptions returns a ConnOption that sets grpc.DialOption for grpc.DialContext.
func WithDialOptions(dialOptions ...grpc.DialOption) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.DialOptions = append(o.DialOptions, dialOptions...)
	})
}

// WithContext is the default client context; it can be used to cancel grpc dial out and
// other operations that do not have an explicit context.
func WithContext(ctx context.Context) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.Context = ctx
	})
}

func WithZapLogger(logger *zap.Logger) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.Logger = logger
	})
}

func WithEnableSsl(enable bool) ConnOption {
	return newFuncConnOption(func(o *connOptions) {
		o.EnableSsl = enable
	})
}
