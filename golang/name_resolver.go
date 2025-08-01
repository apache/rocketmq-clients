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
	"google.golang.org/grpc/resolver"
	"math/rand"
	"strings"
	"time"
)

type rocketmqResolverBuilder struct{}

const (
	DefaultScheme = "ip"
)

func (b *rocketmqResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	endpoints := target.Endpoint()
	r := &RocketmqResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			target.Endpoint(): b.splitEndpoints(endpoints),
		},
	}
	r.start()
	return r, nil
}

func (b *rocketmqResolverBuilder) splitEndpoints(endpoints string) []string {
	result := []string{}
	for _, ep := range strings.Split(endpoints, ";") {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			result = append(result, ep)
		}
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}

func (b *rocketmqResolverBuilder) Scheme() string { return DefaultScheme }

type RocketmqResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	addrsStore map[string][]string
}

func (r *RocketmqResolver) start() {
	addrStrs := r.addrsStore[r.target.Endpoint()]
	addrs := make([]resolver.Address, len(addrStrs))
	for i, s := range addrStrs {
		addrs[i] = resolver.Address{Addr: s}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}
func (r *RocketmqResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *RocketmqResolver) Close()                                {}

func init() {
	resolver.Register(&rocketmqResolverBuilder{})
}
