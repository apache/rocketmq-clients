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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	innerMD "github.com/apache/rocketmq-clients/golang/metadata"
	"github.com/apache/rocketmq-clients/golang/pkg/grpc/middleware/zaplog"
	validator "github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	ErrNoAvailableEndpoints = errors.New("rocketmq: no available endpoints")
)

type ClientConnFunc func(*Config, ...ConnOption) (ClientConn, error)

type ClientConn interface {
	Conn() *grpc.ClientConn
	Close() error
	Config() *Config
}

var _ = ClientConn(&clientConn{})

type clientConn struct {
	opts     connOptions
	creds    credentials.TransportCredentials
	ctx      context.Context
	cancel   context.CancelFunc
	callOpts []grpc.CallOption
	conn     *grpc.ClientConn
	config   *Config
	validate *validator.Validate
}

func NewClientConn(config *Config, opts ...ConnOption) (ClientConn, error) {
	client := &clientConn{
		opts:     defaultConnOptions,
		config:   config,
		validate: validator.New(),
	}
	if client.config == nil {
		return nil, ErrNoAvailableEndpoints
	}
	if err := client.validate.Struct(client.config); err != nil {
		return nil, err
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

	conn, err := client.dial(config.Endpoint)
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

func (c *clientConn) Config() *Config {
	return c.config
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
		c.UnaryClientInterceptor(),
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

func (c *clientConn) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := c.sign(ctx)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

func (c *clientConn) sign(ctx context.Context) context.Context {
	now := time.Now().Format("20060102T150405Z")
	return metadata.AppendToOutgoingContext(ctx,
		innerMD.LanguageKey,
		innerMD.LanguageValue,
		innerMD.ProtocolKey,
		innerMD.ProtocolValue,
		innerMD.RequestID,
		uuid.New().String(),
		innerMD.VersionKey,
		innerMD.VersionValue,
		innerMD.NameSpace,
		c.config.NameSpace,
		innerMD.ClintID,
		c.opts.ID,
		innerMD.DateTime,
		now,
		innerMD.Authorization,
		fmt.Sprintf("%s %s=%s/%s/%s, %s=%s, %s=%s",
			innerMD.EncryptHeader,
			innerMD.Credential,
			c.config.Credentials.AccessKey,
			c.config.Region,
			innerMD.Rocketmq,
			innerMD.SignedHeaders,
			innerMD.DateTime,
			innerMD.Signature,
			func() string {
				h := hmac.New(sha1.New, []byte(c.config.Credentials.AccessSecret))
				h.Write([]byte(now))
				return hex.EncodeToString(h.Sum(nil))
			}(),
		),
	)
}
