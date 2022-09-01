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
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-clients/golang/metadata"
	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"github.com/valyala/fastrand"
	"go.opencensus.io/trace"
	MD "google.golang.org/grpc/metadata"
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
	ret := &v2.Endpoints{
		Scheme: v2.AddressScheme_DOMAIN_NAME,
		Addresses: []*v2.Address{
			{
				Host: "",
				Port: 80,
			},
		},
	}
	path := target
	u, err := url.Parse(target)
	if err != nil {
		path = target
		ret.Scheme = v2.AddressScheme_IPv4
	} else {
		if u.Host != "" {
			path = u.Host
		}
	}
	paths := strings.Split(path, ":")
	if len(paths) > 1 {
		if port, err2 := strconv.ParseInt(paths[1], 10, 32); err2 == nil {
			ret.Addresses[0].Port = int32(port)
		}
		ret.Addresses[0].Host = paths[0]
	} else {
		return nil, fmt.Errorf("parse target failed, target=%s", target)
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

func GZIPDecode(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

var clientIdx int64 = 0

func GenClientID() string {
	hostName := HostName()
	processID := os.Getpid()
	nextIdx := atomic.AddInt64(&clientIdx, 1) - 1
	nanotime := time.Now().UnixNano() / 1000
	return fmt.Sprintf("%s@%d@%d@%s", hostName, processID, nextIdx, strconv.FormatInt(nanotime, 36))
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
