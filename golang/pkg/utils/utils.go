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

package utils

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/apache/rocketmq-clients/golang/v5/metadata"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/valyala/fastrand"
	"go.opencensus.io/trace"
	MD "google.golang.org/grpc/metadata"
)

type CompressionType int32
type MessageQueueStr string

const (
	Unknown CompressionType = 0
	GZIP    CompressionType = 1
	Zlib    CompressionType = 2
	LZ4     CompressionType = 3
	ZSTD    CompressionType = 4
)

func Mod(n int32, m int) int {
	if int32(m) <= 0 {
		return 0
	}
	i := int32(n % int32(m))
	if i < 0 {
		i += int32(m)
	}
	return int(i)
}

func Mod64(n int64, m int) int {
	if int64(m) <= 0 {
		return 0
	}
	i := int64(n % int64(m))
	if i < 0 {
		i += int64(m)
	}
	return int(i)
}

func ParseAddress(address *v2.Address) string {
	if address == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", address.Host, address.Port)
}

func ParseTarget(target string) (*v2.Endpoints, error) {
	if strings.HasPrefix(target, "ip:///") {
		target = strings.TrimPrefix(target, "ip:///")
	}
	ret := &v2.Endpoints{
		Scheme: v2.AddressScheme_DOMAIN_NAME,
	}
	addressRawList := strings.Split(target, ";")
	for _, path := range addressRawList {
		if len(path) == 0 {
			continue
		}
		address := &v2.Address{
			Host: "",
			Port: 80,
		}
		if u, err := url.Parse(path); err != nil {
			address.Host = path
			ret.Scheme = v2.AddressScheme_IPv4
		} else {
			if u.Host != "" {
				address.Host = u.Host
			}
		}
		paths := strings.Split(path, ":")
		if len(paths) > 1 {
			if port, err2 := strconv.ParseInt(paths[1], 10, 32); err2 == nil {
				address.Port = int32(port)
			}
			address.Host = paths[0]
		} else {
			return nil, fmt.Errorf("parse target failed, target=%s", target)
		}
		ret.Addresses = append(ret.Addresses, address)
	}
	return ret, nil
}

func GetOsDescription() string {
	osName := os.Getenv("os.name")
	if len(osName) == 0 {
		return ""
	}
	version := os.Getenv("os.version")
	if len(version) == 0 {
		return osName
	}
	return osName + " " + version
}

var hostName = ""

func HostName() string {
	if len(hostName) != 0 {
		return hostName
	}
	hostName_, err := os.Hostname()
	if err != nil {
		hostName_ = "HOST_NAME_NOT_FOUND"
	} else {
		hostName = hostName_
	}
	return hostName
}

func MatchMessageType(mq *v2.MessageQueue, messageType v2.MessageType) bool {
	for _, item := range mq.GetAcceptMessageTypes() {
		if item == messageType {
			return true
		}
	}
	return false
}

func MatchCompressionAlgorithm(in []byte) CompressionType {
	if in == nil {
		return Unknown
	}
	if len(in) >= 2 {
		if in[0] == 0x78 {
			return Zlib
		} else if in[0] == 0x1f && in[1] == 0x8b {
			return GZIP
		}
	}
	if len(in) >= 4 {
		if in[0] == 0x04 && in[1] == 0x22 && in[2] == 0x4D && in[3] == 0x18 {
			return LZ4
		} else if in[0] == 0x28 && in[1] == 0xB5 && in[2] == 0x2F && in[3] == 0xFD {
			return ZSTD
		}
	}
	return Unknown
}

func AutoDecode(in []byte) ([]byte, error) {
	compressionType := MatchCompressionAlgorithm(in)
	switch compressionType {
	case Zlib:
		return ZlibDecode(in)
	case GZIP:
		return GZIPDecode(in)
	}
	return in, fmt.Errorf("unknown format")
}

func ZlibDecode(in []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func Lz4Decode(in []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(in))
	return ioutil.ReadAll(reader)
}

func ZstdDecode(in []byte) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func GZIPDecode(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

var clientIdx atomic.Int64 = *atomic.NewInt64(0)

func GenClientID() string {
	hostName := HostName()
	processID := os.Getpid()
	nextIdx := clientIdx.Inc() - 1
	nanotime := time.Now().UnixNano() / 1000
	return fmt.Sprintf("%s@%d@%d@%s", hostName, processID, nextIdx, strconv.FormatInt(nanotime, 36))
}
func EndpointsToString(endpoints *v2.Endpoints) string {
	if endpoints == nil {
		return ""
	}
	var sb strings.Builder
	addresses := endpoints.GetAddresses()
	sort.Slice(addresses, func(i, j int) bool {
		ip1 := net.ParseIP(addresses[i].Host).String()
		ip2 := net.ParseIP(addresses[j].Host).String()
		if ip1 == ip2 {
			return addresses[i].Port < addresses[j].Port
		}
		return ip1 < ip2
	})
	for i, addr := range addresses {
		sb.WriteString(fmt.Sprintf("%s:%d", addr.Host, addr.Port))
		if i != len(addresses)-1 {
			sb.WriteString(";")
		}
	}
	return sb.String()
}

func SelectAnAddress(endpoints *v2.Endpoints) *v2.Address {
	if endpoints == nil {
		return nil
	}
	addresses := endpoints.GetAddresses()
	idx := fastrand.Uint32n(uint32(len(addresses)))
	selectAddress := addresses[idx]
	return selectAddress
}

func CompareEndpoints(e1, e2 *v2.Endpoints) bool {
	if e1 == e2 {
		return true
	}
	if e1 == nil || e2 == nil {
		return false
	}
	if e1.Scheme != e2.Scheme {
		return false
	}

	return CompareAddress(e1.GetAddresses(), e2.GetAddresses())
}

func CompareAddress(a1, a2 []*v2.Address) bool {
	if len(a1) != len(a2) {
		return false
	}
	tmpMap := make(map[string]bool)
	for _, a := range a1 {
		tmpMap[a.String()] = true
	}
	for _, a := range a2 {
		str := a.String()
		if _, ok := tmpMap[str]; ok {
			delete(tmpMap, str)
		} else {
			return false
		}
	}
	return len(tmpMap) == 0
}

func FromTraceParentHeader(header string) (*trace.SpanContext, bool) {
	if len(header) != 55 || header[0] != '0' ||
		header[1] != '0' ||
		header[2] != '-' ||
		header[35] != '-' ||
		header[52] != '-' {
		return nil, false
	}
	trace_id_str := header[3 : 3+32]
	span_id_str := header[36 : 36+16]
	options_str := header[53 : 53+2]
	trace_id, err := hex.DecodeString(trace_id_str)
	if err != nil || len(trace_id) != 16 {
		return nil, false
	}
	span_id, err := hex.DecodeString(span_id_str)
	if err != nil || len(span_id) != 8 {
		return nil, false
	}
	options, err := hex.DecodeString(options_str)
	if err != nil || len(options) != 1 {
		return nil, false
	}
	sc := trace.SpanContext{
		TraceOptions: trace.TraceOptions(options[0]),
	}
	copy(sc.TraceID[:], trace_id[:])
	copy(sc.SpanID[:], span_id[:])
	return &sc, true
}

func ToTraceParentHeader(sc *trace.SpanContext) string {
	if sc == nil {
		return "00-00000000000000000000000000000000-0000000000000000-00"
	}
	return fmt.Sprintf("00-%s-%s-%02s", sc.TraceID.String(), sc.SpanID.String(), strconv.FormatUint(uint64(sc.TraceOptions), 16))
}

func GetenvWithDef(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		val = def
	}
	return val
}

func DumpStacks() string {
	buf := make([]byte, 16384)
	buf = buf[:runtime.Stack(buf, true)]
	return string(buf)
}

func GetMacAddress() []byte {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	for _, netInterface := range netInterfaces {
		macAddr := netInterface.HardwareAddr
		if len(macAddr) == 0 {
			continue
		}
		return macAddr
	}
	return nil
}

func GetRequestID(ctx context.Context) string {
	ret := "nil"
	md, ok := MD.FromOutgoingContext(ctx)
	if ok {
		ret = fmt.Sprintf("%v", md.Get(metadata.RequestID))
	}
	return ret
}

func CountSyncMapSize(m *sync.Map) int32 {
	if m == nil {
		return 0
	}
	cnt := int32(0)
	m.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func IsAssignmentsEmpty(a *[]*v2.Assignment) bool {
	if a == nil {
		return true
	}
	if len(*a) == 0 {
		return true
	}
	return false
}

func CompareAssignments(a *[]*v2.Assignment, b *[]*v2.Assignment) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	l := len(*a)
	if l != len(*b) {
		return false
	}
	for i := 0; i < l; i++ {
		a0 := (*a)[0]
		b0 := (*b)[0]
		if !CompareAssignment(a0, b0) {
			return false
		}
	}
	return true
}

func CompareAssignment(a *v2.Assignment, b *v2.Assignment) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return CompareMessageQueue(a.MessageQueue, b.MessageQueue)
}

func CompareMessageQueue(a *v2.MessageQueue, b *v2.MessageQueue) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return (a.Id == b.Id) && (a.Permission == b.Permission) && CompareResource(a.Topic, b.Topic) && CompareBroker(a.Broker, b.Broker)
}

func CompareResource(a *v2.Resource, b *v2.Resource) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return (a.Name == b.Name) && (a.ResourceNamespace == b.ResourceNamespace)
}

func CompareBroker(a *v2.Broker, b *v2.Broker) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return (a.Name == b.Name) && (a.Id == b.Id) && CompareEndpoints(a.Endpoints, b.Endpoints)
}

func ParseMessageQueue2Str(m *v2.MessageQueue) MessageQueueStr {
	if m == nil {
		return ""
	}
	return MessageQueueStr(m.String())
}

func GetNextAttemptDelay(retryPolicy *v2.RetryPolicy, attempt int) time.Duration {
	if attempt <= 0 {
		// TODO bug when retryPolicy is nil
		return time.Duration(time.Second * 10)
	}
	var delayNanos int64
	if backoff, ok := retryPolicy.GetStrategy().(*v2.RetryPolicy_ExponentialBackoff); ok {
		delayNanos = int64(math.Min(float64(backoff.ExponentialBackoff.Initial.AsDuration().Nanoseconds())*math.Pow(float64(backoff.ExponentialBackoff.Multiplier), 1.0*float64(attempt-1)), float64(backoff.ExponentialBackoff.Max.AsDuration().Nanoseconds())))
		if delayNanos <= 0 {
			delayNanos = 0
		}
	} else if backoff, ok := retryPolicy.GetStrategy().(*v2.RetryPolicy_CustomizedBackoff); ok {
		if attempt > len(backoff.CustomizedBackoff.Next) {
			attempt = len(backoff.CustomizedBackoff.Next)
		}
		delayNanos = backoff.CustomizedBackoff.Next[attempt-1].AsDuration().Nanoseconds()
	}
	return time.Duration(delayNanos)
}
