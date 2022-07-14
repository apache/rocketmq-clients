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
	"errors"
	"fmt"

	"github.com/apache/rocketmq-clients/golang/pkg/grpc/middleware/zaplog"
	validator "github.com/go-playground/validator/v10"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	ErrNoAvailableEndpoints = errors.New("rocketmq: no available endpoints")
)

type ClientConnFunc func(string, ...ConnOption) (ClientConn, error)

type ClientConn interface {
	Conn() *grpc.ClientConn
	Close() error
}

var _ = ClientConn(&clientConn{})

type clientConn struct {
	opts     connOptions
	creds    credentials.TransportCredentials
	ctx      context.Context
	cancel   context.CancelFunc
	callOpts []grpc.CallOption
	conn     *grpc.ClientConn
	validate *validator.Validate
}

func NewClientConn(endpoint string, opts ...ConnOption) (ClientConn, error) {
	client := &clientConn{
		opts:     defaultConnOptions,
		validate: validator.New(),
	}
	if len(endpoint) == 0 {
		return nil, ErrNoAvailableEndpoints
	}
	for _, opt := range opts {
		opt.apply(&client.opts)
	}

	baseCtx := context.TODO()
	if client.opts.Context != nil {
		baseCtx = client.opts.Context
	}

	ctx, cancel := context.WithCancel(baseCtx)

	client.ctx = ctx
	client.cancel = cancel
	client.creds = credentials.NewTLS(client.opts.TLS)

	if client.opts.MaxCallSendMsgSize > 0 || client.opts.MaxCallRecvMsgSize > 0 {
		if client.opts.MaxCallRecvMsgSize > 0 && client.opts.MaxCallSendMsgSize > client.opts.MaxCallRecvMsgSize {
			return nil, fmt.Errorf("gRPC message recv limit (%d bytes) must be greater than send limit (%d bytes)", client.opts.MaxCallRecvMsgSize, client.opts.MaxCallSendMsgSize)
		}
		if client.opts.MaxCallSendMsgSize > 0 {
			client.callOpts = append(client.callOpts, grpc.MaxCallSendMsgSize(client.opts.MaxCallSendMsgSize))
		}
		if client.opts.MaxCallRecvMsgSize > 0 {
			client.callOpts = append(client.callOpts, grpc.MaxCallRecvMsgSize(client.opts.MaxCallRecvMsgSize))
		}
	}

	conn, err := client.dial(endpoint)
	if err != nil {
		client.cancel()
		return nil, err
	}
	client.conn = conn

	return client, nil
}

func (c *clientConn) Conn() *grpc.ClientConn {
	return c.conn
}

func (c *clientConn) Close() error {
	defer c.cancel()
	return c.conn.Close()
}

func (c *clientConn) dialSetupOpts(dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
	if c.opts.DialKeepAliveTime > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.opts.DialKeepAliveTime,
			Timeout:             c.opts.DialKeepAliveTimeout,
			PermitWithoutStream: c.opts.PermitWithoutStream,
		}))
	}
	opts = append(opts, dopts...)
	if c.creds != nil {
		opts = append(opts, grpc.WithTransportCredentials(c.creds))
	}
	opts = append(opts, grpc.WithBlock(), grpc.WithChainUnaryInterceptor(
		zaplog.UnaryClientInterceptor(c.opts.Logger),
	))
	return
}

func (c *clientConn) dial(target string, dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts, err := c.dialSetupOpts(dopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to configure dialer: %v", err)
	}
	opts = append(opts, c.opts.DialOptions...)
	dctx := c.ctx
	if c.opts.DialTimeout > 0 {
		dctx, _ = context.WithTimeout(c.ctx, c.opts.DialTimeout)
	}
	conn, err := grpc.DialContext(dctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
