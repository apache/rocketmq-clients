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
	"log"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	innerMD "github.com/apache/rocketmq-clients/golang/metadata"
	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/grpc/metadata"
)

type Client interface {
	GetClientID() string
	Sign(ctx context.Context) context.Context
	GetMessageQueues(ctx context.Context, topic string) ([]*v2.MessageQueue, error)
	GracefulStop() error
}

type NewClientFunc func(*Config, ...ClientOption) (Client, error)

var _ = Client(&defaultClient{})

type defaultClient struct {
	config        *Config
	opts          clientOptions
	conn          ClientConn
	msc           v2.MessagingServiceClient
	router        sync.Map
	clientID      string
	clientManager ClientManager
	done          chan struct{}
}

func NewClient(config *Config, opts ...ClientOption) (Client, error) {
	cli := &defaultClient{
		config:   config,
		opts:     defaultNSOptions,
		clientID: fmt.Sprintf("%s@%d@%s", innerMD.Rocketmq, rand.Int(), shortuuid.New()),
	}
	for _, opt := range opts {
		opt.apply(&cli.opts)
	}
	conn, err := cli.opts.clientConnFunc(config.Endpoint, cli.opts.connOptions...)
	if err != nil {
		return nil, err
	}

	cli.conn = conn
	cli.msc = v2.NewMessagingServiceClient(conn.Conn())
	cli.done = make(chan struct{})
	cli.startUp()
	return cli, nil
}

func (cli *defaultClient) GetClientID() string {
	return cli.clientID
}

func (cli *defaultClient) GetMessageQueues(ctx context.Context, topic string) ([]*v2.MessageQueue, error) {
	item, ok := cli.router.Load(topic)
	if ok {
		if ret, ok := item.([]*v2.MessageQueue); ok {
			return ret, nil
		}
	}
	route, err := cli.queryRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	cli.router.Store(topic, route)
	return route, nil
}

func (cli *defaultClient) queryRoute(ctx context.Context, topic string) ([]*v2.MessageQueue, error) {
	ctx = cli.Sign(ctx)
	response, err := cli.msc.QueryRoute(ctx, cli.getQueryRouteRequest(topic))
	if err != nil {
		return nil, err
	}
	if response.GetStatus().GetCode() != v2.Code_OK {
		return nil, fmt.Errorf("QueryRoute err = %s", response.String())
	}

	if len(response.GetMessageQueues()) == 0 {
		return nil, errors.New("rocketmq: no available brokers")
	}
	return response.GetMessageQueues(), nil
}

func (cli *defaultClient) getQueryRouteRequest(topic string) *v2.QueryRouteRequest {
	return &v2.QueryRouteRequest{
		Topic: &v2.Resource{
			ResourceNamespace: cli.config.NameSpace,
			Name:              topic,
		},
		Endpoints: cli.parseTarget(cli.conn.Conn().Target()),
	}
}

func (cli *defaultClient) parseTarget(target string) *v2.Endpoints {
	ret := &v2.Endpoints{
		Scheme: v2.AddressScheme_DOMAIN_NAME,
		Addresses: []*v2.Address{
			{
				Host: "",
				Port: 80,
			},
		},
	}
	var (
		path string
	)
	u, err := url.Parse(target)
	if err != nil {
		path = target
		ret.Scheme = v2.AddressScheme_IPv4
	} else {
		path = u.Path
	}
	paths := strings.Split(path, ":")
	if len(paths) > 0 {
		if port, err := strconv.ParseInt(paths[1], 10, 32); err != nil {
			ret.Addresses[0].Port = int32(port)
		}
	}
	ret.Addresses[0].Host = paths[0]
	return ret
}

func (cli *defaultClient) startUp() {
	log.Printf("Begin to start the rocketmq client, clientId=%s", cli.clientID)
	cli.clientManager = defaultClientManagerRegistry.RegisterClient(cli)
	f := func() {
		cli.router.Range(func(k, v interface{}) bool {
			ctx, _ := context.WithTimeout(context.TODO(), cli.opts.timeout)
			item, _ := cli.queryRoute(ctx, k.(string))
			if !reflect.DeepEqual(item, v) {
				// cli.router.Store(k, item)
				// b, ok := cli.brokers.Load(k)
				// if ok {
				// 	bo, ok := b.(RpcClient)
				// 	if ok {
				// 		bo.SetMessageQueue(item)
				// 	}
				// }
			}
			return true
		})
	}
	ticker.Tick(f, cli.opts.tickerDuration, cli.done)
}

func (cli *defaultClient) GracefulStop() error {
	close(cli.done)
	cli.done <- struct{}{}
	// cli.brokers.Range(func(k, v interface{}) bool {
	// 	broker, ok := v.(RpcClient)
	// 	if ok {
	// 		_ = broker.GracefulStop()
	// 	}
	// 	return true
	// })
	return cli.conn.Close()
}

func (cli *defaultClient) Sign(ctx context.Context) context.Context {
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
		// innerMD.NameSpace,
		// cli.config.NameSpace,
		innerMD.ClintID,
		cli.clientID,
		innerMD.DateTime,
		now,
		innerMD.Authorization,
		fmt.Sprintf("%s %s=%s/%s/%s, %s=%s, %s=%s",
			innerMD.EncryptHeader,
			innerMD.Credential,
			cli.config.Credentials.AccessKey,
			cli.config.Region,
			innerMD.Rocketmq,
			innerMD.SignedHeaders,
			innerMD.DateTime,
			innerMD.Signature,
			func() string {
				h := hmac.New(sha1.New, []byte(cli.config.Credentials.AccessSecret))
				h.Write([]byte(now))
				return hex.EncodeToString(h.Sum(nil))
			}(),
		),
	)
}
