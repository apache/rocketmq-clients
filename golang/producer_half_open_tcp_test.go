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
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestProducerRecoversFromHalfOpenTCP exercises the production heartbeat path,
// not gRPC's reconnect-on-EOF path. Only connections accepted before the fault
// are blackholed; a replacement ClientConn can reach the same listening endpoint.
func TestProducerRecoversFromHalfOpenTCP(t *testing.T) {
	// Not parallel: the scoped factory adds local transport options to the real
	// factory. It does not replace RpcClient, ClientConn, or any recovery logic.
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	realNewRpcClient := NewRpcClient
	observed := &halfOpenTCPObservations{}
	var workers sync.WaitGroup
	var backend net.Listener
	var proxy *halfOpenTCPProxy
	var server *grpc.Server

	// Restore the global factory only after Start/Send/GracefulStop, all proxy
	// pumps, Serve, and gRPC handlers have exited, including on a test timeout.
	t.Cleanup(func() {
		cancel()
		if proxy != nil {
			proxy.close()
		}
		if server != nil {
			server.Stop()
		}
		if backend != nil {
			_ = backend.Close()
		}
		workers.Wait()
		NewRpcClient = realNewRpcClient
	})

	var err error
	backend, err = net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy, err = newHalfOpenTCPProxy(ctx, backend.Addr().String(), &observed.heartbeatDeadlines)
	if err != nil {
		t.Fatal(err)
	}
	service := &halfOpenTCPMessagingService{proxy: proxy}
	// Stop must also join the Telemetry handlers blocked in Recv.
	server = grpc.NewServer(grpc.WaitForHandlers(true))
	v2.RegisterMessagingServiceServer(server, service)
	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := server.Serve(backend); err != nil && !errors.Is(err, grpc.ErrServerStopped) && ctx.Err() == nil {
			observed.serverErrors.Add(1)
		}
	}()

	NewRpcClient = func(target string, opts ...RpcClientOption) (RpcClient, error) {
		if observed.blackholing.Load() && observed.heartbeatDeadlines.Load() < 2 {
			observed.earlyReplacements.Add(1)
		}
		localOpts := append([]RpcClientOption(nil), opts...)
		localOpts = append(localOpts, WithRpcClientConnOption(
			WithContext(ctx),
			WithDialOptions(
				// These options are appended after the production TLS default.
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithChainUnaryInterceptor(observed.unary),
			),
		))
		client, err := realNewRpcClient(target, localOpts...)
		if err == nil {
			observed.rpcClients.Add(1)
		}
		return client, err
	}

	result := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		result <- exerciseProducerHalfOpenTCP(ctx, proxy, service, observed)
	}()
	// Start has no context parameter. Supervising the entire workflow, then
	// closing the backend in Cleanup, also unblocks a failed settings handshake.
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("producer TCP-blackhole integration exceeded 70s: accepted=%d closed=%d heartbeat deadlines=%d settings=%d",
			proxy.accepted.Load(), proxy.closed.Load(), observed.heartbeatDeadlines.Load(), service.settings.Load())
	}
	t.Logf("recovered in %s: accepted=%d closed=%d rpc clients=%d heartbeat deadlines=%d settings=%d",
		time.Duration(observed.recoveryNanos.Load()), proxy.accepted.Load(), proxy.closed.Load(),
		observed.rpcClients.Load(), observed.heartbeatDeadlines.Load(), service.settings.Load())
}

type halfOpenTCPObservations struct {
	blackholing        atomic.Bool
	heartbeatOK        atomic.Int64
	heartbeatDeadlines atomic.Int64
	heartbeatErrors    atomic.Int64
	rpcClients         atomic.Int64
	earlyReplacements  atomic.Int64
	serverErrors       atomic.Int64
	recoveryNanos      atomic.Int64
}

func (o *halfOpenTCPObservations) unary(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoke(ctx, method, req, reply, cc, opts...)
	if _, ok := req.(*v2.HeartbeatRequest); ok {
		switch status.Code(err) {
		case codes.OK:
			o.heartbeatOK.Add(1)
		case codes.DeadlineExceeded:
			o.heartbeatDeadlines.Add(1)
		default:
			o.heartbeatErrors.Add(1)
		}
	}
	return err
}

func exerciseProducerHalfOpenTCP(ctx context.Context, proxy *halfOpenTCPProxy, service *halfOpenTCPMessagingService, observed *halfOpenTCPObservations) (resultErr error) {
	const topic = "half-open-tcp"
	producer, err := NewProducer(&Config{Endpoint: proxy.listener.Addr().String()}, WithTopics(topic), WithMaxAttempts(1))
	if err != nil {
		return fmt.Errorf("NewProducer: %w", err)
	}
	defer func() {
		// A normal producer shutdown is not a proxy forwarding failure.
		proxy.stopping.Store(true)
		if producer.isRunning() {
			if err := producer.GracefulStop(); err != nil && resultErr == nil {
				resultErr = fmt.Errorf("GracefulStop: %w", err)
			}
		}
	}()
	if err := producer.Start(); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	cm, ok := producer.getClient().clientManager.(*defaultClientManager)
	if !ok {
		return fmt.Errorf("producer did not start a real client manager")
	}
	if cm.opts.HEART_BEAT_PERIOD != 10*time.Second || producer.getRequestTimeout() != 3*time.Second {
		return fmt.Errorf("test requires the default 10s heartbeat and 3s request timeout")
	}
	if producer.(*defaultProducer).getRetryMaxAttempts() != 1 {
		return fmt.Errorf("settings changed the producer's single-attempt retry policy")
	}

	// Each send starts from a fresh context without shared mutable metadata.
	send := func(parent context.Context) ([]*SendReceipt, error) {
		callCtx, cancel := context.WithTimeout(parent, 4*time.Second)
		defer cancel()
		return producer.Send(callCtx, &Message{Topic: topic, Body: []byte("half-open TCP recovery")})
	}
	initialReceipts, err := send(ctx)
	if err != nil {
		return fmt.Errorf("initial Send: %w", err)
	}
	if len(initialReceipts) != 1 || initialReceipts[0].MessageID == "" || initialReceipts[0].Offset <= 0 {
		return fmt.Errorf("initial Send did not return a valid receipt: %v", initialReceipts)
	}
	// Establish that the real scheduled heartbeat and settings stream work
	// before introducing the fault; never call Heartbeat or a reset seam here.
	if err := waitHalfOpenTCP(ctx, func() bool { return observed.heartbeatOK.Load() > 0 && service.settings.Load() > 0 }); err != nil {
		return fmt.Errorf("healthy heartbeat/settings: %w", err)
	}
	initialClient := cachedHalfOpenTCPClient(cm)
	if initialClient == nil {
		return fmt.Errorf("expected one cached real rpcClient")
	}
	if _, ok := initialClient.conn.(*clientConn); !ok {
		return fmt.Errorf("initial RPC client did not use the real NewClientConn")
	}
	if proxy.closed.Load() != 0 || proxy.healthyErrors.Load() != 0 || observed.serverErrors.Load() != 0 ||
		observed.heartbeatDeadlines.Load() != 0 || observed.heartbeatErrors.Load() != 0 {
		return fmt.Errorf("unhealthy baseline: closed=%d proxy errors=%d server errors=%d heartbeat deadlines=%d other heartbeat errors=%d",
			proxy.closed.Load(), proxy.healthyErrors.Load(), observed.serverErrors.Load(), observed.heartbeatDeadlines.Load(), observed.heartbeatErrors.Load())
	}
	initialSettings := service.settings.Load()
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, 40*time.Second)
	defer cancelRecovery()
	blackholedAt := time.Now()
	observed.blackholing.Store(true)
	initialConnections := proxy.blackholeExisting()
	if initialConnections == 0 || initialReceipts[0].Offset > initialConnections {
		return fmt.Errorf("initial Send did not traverse a connection selected for blackholing")
	}

	if _, err := send(recoveryCtx); status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("Send on the blackholed connection should reach its deadline, got %v", err)
	}
	// Leave recovery to the production heartbeat path. In particular, sends
	// cannot accumulate errors and accidentally substitute for two heartbeats.
	if err := waitHalfOpenTCP(recoveryCtx, func() bool { return observed.heartbeatDeadlines.Load() >= 2 }); err != nil {
		return fmt.Errorf("two real heartbeat deadlines not observed: count=%d accepted=%d closed=%d: %w",
			observed.heartbeatDeadlines.Load(), proxy.accepted.Load(), proxy.closed.Load(), err)
	}

	var recovered []*SendReceipt
	for {
		recovered, err = send(recoveryCtx)
		if err == nil {
			break
		}
		select {
		case <-recoveryCtx.Done():
			return fmt.Errorf("Send did not recover within 40s: accepted=%d closed=%d heartbeat deadlines=%d settings=%d, last error: %w",
				proxy.accepted.Load(), proxy.closed.Load(), observed.heartbeatDeadlines.Load(), service.settings.Load(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if len(recovered) != 1 || recovered[0].MessageID == "" || recovered[0].Offset <= initialConnections {
		return fmt.Errorf("recovered Send did not use a newly accepted TCP connection: %v (initial connections=%d)", recovered, initialConnections)
	}
	newConnection := proxy.connection(recovered[0].Offset)
	if newConnection == nil || newConnection.blackholed.Load() {
		return fmt.Errorf("recovered receipt does not identify a healthy new connection")
	}
	if err := waitHalfOpenTCP(recoveryCtx, func() bool {
		return service.settings.Load() > initialSettings && newConnection.settings.Load() > 0
	}); err != nil {
		return fmt.Errorf("settings were not resynchronized on the recovered connection within 40s: %w", err)
	}
	replacement := cachedHalfOpenTCPClient(cm)
	if replacement == nil || replacement == initialClient || replacement.conn == initialClient.conn || replacement.conn.Conn() == initialClient.conn.Conn() {
		return fmt.Errorf("manager did not replace the cached rpcClient and its actual gRPC ClientConn")
	}
	if _, ok := replacement.conn.(*clientConn); !ok {
		return fmt.Errorf("replacement RPC client did not use the real NewClientConn")
	}
	// Closing the old socket is expected AFTER the second deadline, as part of
	// manager recovery. EOF/FIN/RST before that would make this an ordinary
	// reconnect test rather than a half-open-connection regression test.
	if proxy.earlyCloses.Load() != 0 || observed.earlyReplacements.Load() != 0 || proxy.healthyErrors.Load() != 0 || observed.serverErrors.Load() != 0 {
		return fmt.Errorf("recovery was not exclusively heartbeat-driven: early closes=%d early replacements=%d healthy proxy errors=%d server errors=%d",
			proxy.earlyCloses.Load(), observed.earlyReplacements.Load(), proxy.healthyErrors.Load(), observed.serverErrors.Load())
	}
	if proxy.accepted.Load() <= initialConnections || proxy.discarded.Load() == 0 {
		return fmt.Errorf("missing transport replacement evidence: accepted=%d (initial %d) discarded bytes=%d",
			proxy.accepted.Load(), initialConnections, proxy.discarded.Load())
	}
	elapsed := time.Since(blackholedAt)
	if elapsed > 40*time.Second {
		return fmt.Errorf("recovery exceeded 40s: %s", elapsed)
	}
	observed.recoveryNanos.Store(int64(elapsed))
	return nil
}

func cachedHalfOpenTCPClient(cm *defaultClientManager) *rpcClient {
	cm.rpcClientTableLock.RLock()
	defer cm.rpcClientTableLock.RUnlock()
	if len(cm.rpcClientTable) == 1 {
		for _, entry := range cm.rpcClientTable {
			rpc, _ := entry.rpc.(*rpcClient)
			return rpc
		}
	}
	return nil
}

func waitHalfOpenTCP(ctx context.Context, ready func() bool) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type halfOpenTCPMessagingService struct {
	v2.UnimplementedMessagingServiceServer
	proxy    *halfOpenTCPProxy
	settings atomic.Int64
}

func (s *halfOpenTCPMessagingService) QueryRoute(_ context.Context, req *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	address := s.proxy.listener.Addr().(*net.TCPAddr)
	return &v2.QueryRouteResponse{
		Status: &v2.Status{Code: v2.Code_OK},
		MessageQueues: []*v2.MessageQueue{{
			Topic:      &v2.Resource{Name: req.GetTopic().GetName(), ResourceNamespace: req.GetTopic().GetResourceNamespace()},
			Id:         0,
			Permission: v2.Permission_READ_WRITE,
			Broker: &v2.Broker{
				Name: "loopback-broker",
				Id:   0,
				Endpoints: &v2.Endpoints{
					Scheme:    v2.AddressScheme_IPv4,
					Addresses: []*v2.Address{{Host: "127.0.0.1", Port: int32(address.Port)}},
				},
			},
			AcceptMessageTypes: []v2.MessageType{v2.MessageType_NORMAL},
		}},
	}, nil
}

func (s *halfOpenTCPMessagingService) Telemetry(stream v2.MessagingService_TelemetryServer) error {
	connection, err := s.proxy.connectionForPeer(stream.Context())
	if err != nil {
		return err
	}
	for {
		command, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		incoming := command.GetSettings()
		if incoming.GetPublishing() == nil || incoming.GetBackoffPolicy().GetMaxAttempts() != 1 || incoming.GetRequestTimeout().AsDuration() != 3*time.Second {
			return status.Error(codes.InvalidArgument, "expected producer settings with one attempt and the default 3s timeout")
		}
		// One response per received settings command, without a response loop or
		// hostname assumptions. Preserve the producer's complete retry policy.
		settings := proto.Clone(incoming).(*v2.Settings)
		settings.Metric = &v2.Metric{On: false}
		settings.GetPublishing().MaxBodySize = 4 * 1024 * 1024
		settings.GetPublishing().ValidateMessageType = true
		if err := stream.Send(&v2.TelemetryCommand{
			Status:  &v2.Status{Code: v2.Code_OK},
			Command: &v2.TelemetryCommand_Settings{Settings: settings},
		}); err != nil {
			return err
		}
		s.settings.Add(1)
		connection.settings.Add(1)
	}
}

func (*halfOpenTCPMessagingService) Heartbeat(context.Context, *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	return &v2.HeartbeatResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}

func (s *halfOpenTCPMessagingService) SendMessage(ctx context.Context, req *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	connection, err := s.proxy.connectionForPeer(ctx)
	if err != nil {
		return nil, err
	}
	response := &v2.SendMessageResponse{Status: &v2.Status{Code: v2.Code_OK}}
	for _, message := range req.GetMessages() {
		response.Entries = append(response.Entries, &v2.SendResultEntry{
			Status:    &v2.Status{Code: v2.Code_OK},
			MessageId: message.GetSystemProperties().GetMessageId(),
			// Echo the proxy connection ID in the receipt so a successful Send
			// proves which TCP connection carried it, not merely a new dial.
			Offset: connection.id,
		})
	}
	return response, nil
}

func (*halfOpenTCPMessagingService) NotifyClientTermination(context.Context, *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error) {
	return &v2.NotifyClientTerminationResponse{Status: &v2.Status{Code: v2.Code_OK}}, nil
}

type halfOpenTCPConnection struct {
	id         int64
	client     net.Conn
	backend    net.Conn // Set under the proxy mutex, before starting either pump.
	forwarding sync.RWMutex
	blackholed atomic.Bool
	closed     atomic.Bool
	settings   atomic.Int64
}

type halfOpenTCPProxy struct {
	ctx       context.Context
	cancel    context.CancelFunc
	listener  net.Listener
	backend   string
	deadlines *atomic.Int64
	wg        sync.WaitGroup
	mu        sync.Mutex
	pairs     []*halfOpenTCPConnection
	peers     map[string]*halfOpenTCPConnection

	stopping      atomic.Bool
	accepted      atomic.Int64
	closed        atomic.Int64
	healthyErrors atomic.Int64
	earlyCloses   atomic.Int64
	discarded     atomic.Int64
}

func newHalfOpenTCPProxy(parent context.Context, backend string, deadlines *atomic.Int64) (*halfOpenTCPProxy, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	p := &halfOpenTCPProxy{
		ctx: ctx, cancel: cancel, listener: listener, backend: backend, deadlines: deadlines,
		peers: make(map[string]*halfOpenTCPConnection),
	}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *halfOpenTCPProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			if p.ctx.Err() == nil {
				p.healthyErrors.Add(1)
			}
			return
		}
		p.mu.Lock()
		connection := &halfOpenTCPConnection{id: p.accepted.Add(1), client: client}
		p.pairs = append(p.pairs, connection)
		p.mu.Unlock()

		backend, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(p.ctx, "tcp4", p.backend)
		if err != nil {
			p.closeConnection(connection)
			continue
		}
		p.mu.Lock()
		if p.ctx.Err() != nil || connection.closed.Load() {
			p.mu.Unlock()
			_ = backend.Close()
			p.closeConnection(connection)
			continue
		}
		connection.backend = backend
		p.peers[backend.LocalAddr().String()] = connection
		// Bound even a stuck forwarding Write by the integration deadline.
		if deadline, ok := p.ctx.Deadline(); ok {
			_ = client.SetDeadline(deadline)
			_ = backend.SetDeadline(deadline)
		}
		p.wg.Add(2)
		go p.pump(connection, backend, client)
		go p.pump(connection, client, backend)
		p.mu.Unlock()
	}
}

func (p *halfOpenTCPProxy) pump(connection *halfOpenTCPConnection, dst, src net.Conn) {
	defer p.wg.Done()
	defer p.closeConnection(connection)
	buffer := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			connection.forwarding.RLock()
			var writeErr error
			if connection.blackholed.Load() {
				// Keep reading BOTH directions, ACKing and discarding bytes. Do
				// not close, half-close, stop reading, or forward any faulted data.
				p.discarded.Add(int64(n))
			} else {
				var written int
				written, writeErr = dst.Write(buffer[:n])
				if written != n && writeErr == nil {
					writeErr = io.ErrShortWrite
				}
			}
			connection.forwarding.RUnlock()
			if writeErr != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (p *halfOpenTCPProxy) blackholeExisting() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range p.pairs {
		// Wait for any healthy forwarding write to finish before marking the
		// fault established. Later accepted connections retain forwarding.
		connection.forwarding.Lock()
		connection.blackholed.Store(true)
		connection.forwarding.Unlock()
	}
	return p.accepted.Load()
}

func (p *halfOpenTCPProxy) closeConnection(connection *halfOpenTCPConnection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !connection.closed.CompareAndSwap(false, true) {
		return
	}
	if p.ctx.Err() == nil && !p.stopping.Load() {
		if !connection.blackholed.Load() {
			p.healthyErrors.Add(1)
		} else if p.deadlines.Load() < 2 {
			p.earlyCloses.Add(1)
		}
	}
	_ = connection.client.Close()
	if connection.backend != nil {
		_ = connection.backend.Close()
	}
	p.closed.Add(1)
}

func (p *halfOpenTCPProxy) connectionForPeer(ctx context.Context) (*halfOpenTCPConnection, error) {
	remote, ok := peer.FromContext(ctx)
	if !ok || remote.Addr == nil {
		return nil, status.Error(codes.Internal, "missing TCP peer")
	}
	p.mu.Lock()
	connection := p.peers[remote.Addr.String()]
	p.mu.Unlock()
	if connection == nil {
		return nil, status.Error(codes.Internal, "request bypassed the TCP proxy")
	}
	return connection, nil
}

func (p *halfOpenTCPProxy) connection(id int64) *halfOpenTCPConnection {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range p.pairs {
		if connection.id == id {
			return connection
		}
	}
	return nil
}

func (p *halfOpenTCPProxy) close() {
	p.cancel()
	_ = p.listener.Close()
	p.mu.Lock()
	connections := append([]*halfOpenTCPConnection(nil), p.pairs...)
	p.mu.Unlock()
	for _, connection := range connections {
		p.closeConnection(connection)
	}
	// The accept goroutine remains counted while it can add pump goroutines.
	// Cancellation also makes an in-flight backend Dial exit and close its pair.
	p.wg.Wait()
}
