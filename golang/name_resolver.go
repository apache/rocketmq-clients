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
