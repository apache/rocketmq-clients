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
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/rocketmq-clients/golang/pkg/ticker"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
)

type NameServer interface {
	GetBroker(ctx context.Context, topic string) (Broker, error)
	GracefulStop() error
}

type NewNameServerFunc func(*Config, ...NameServerOption) (NameServer, error)

var _ = NameServer(&defaultNameServer{})

type defaultNameServer struct {
	opts    nameServerOptions
	conn    ClientConn
	msc     v2.MessagingServiceClient
	router  sync.Map
	brokers sync.Map
	done    chan struct{}
}

func NewNameServer(config *Config, opts ...NameServerOption) (NameServer, error) {
	ns := &defaultNameServer{
		opts: defaultNSOptions,
	}
	for _, opt := range opts {
		opt.apply(&ns.opts)
	}
	conn, err := ns.opts.clientConnFunc(config, ns.opts.connOptions...)
	if err != nil {
		return nil, err
	}

	ns.conn = conn
	ns.msc = v2.NewMessagingServiceClient(conn.Conn())
	ns.done = make(chan struct{})
	ns.sync()
	return ns, nil
}

func (ns *defaultNameServer) GetBroker(ctx context.Context, topic string) (Broker, error) {
	item, ok := ns.brokers.Load(topic)
	if ok {
		if ret, ok := item.(Broker); ok {
			return ret, nil
		}
	}
	route, err := ns.queryRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	ns.router.Store(topic, route)
	b, err := ns.opts.brokerFunc(ns.conn.Config(), route, ns.opts.brokerOptions...)
	if err != nil {
		return nil, err
	}
	ns.brokers.Store(topic, b)
	return b, nil
}

func (ns *defaultNameServer) queryRoute(ctx context.Context, topic string) ([]*v2.MessageQueue, error) {
	response, err := ns.msc.QueryRoute(ctx, ns.getQueryRouteRequest(topic))
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

func (ns *defaultNameServer) getQueryRouteRequest(topic string) *v2.QueryRouteRequest {
	return &v2.QueryRouteRequest{
		Topic: &v2.Resource{
			ResourceNamespace: ns.conn.Config().NameSpace,
			Name:              topic,
		},
		Endpoints: ns.parseTarget(ns.conn.Conn().Target()),
	}
}

func (ns *defaultNameServer) parseTarget(target string) *v2.Endpoints {
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

func (ns *defaultNameServer) sync() {
	f := func() {
		ns.router.Range(func(k, v interface{}) bool {
			ctx, _ := context.WithTimeout(context.TODO(), ns.opts.timeout)
			item, _ := ns.queryRoute(ctx, k.(string))
			if !reflect.DeepEqual(item, v) {
				ns.router.Store(k, item)
				b, ok := ns.brokers.Load(k)
				if ok {
					bo, ok := b.(Broker)
					if ok {
						bo.SetMessageQueue(item)
					}
				}
			}
			return true
		})
	}
	ticker.Tick(f, ns.opts.tickerDuration, ns.done)
}

func (ns *defaultNameServer) GracefulStop() error {
	close(ns.done)
	ns.done <- struct{}{}
	ns.brokers.Range(func(k, v interface{}) bool {
		broker, ok := v.(Broker)
		if ok {
			_ = broker.GracefulStop()
		}
		return true
	})
	return ns.conn.Close()
}
