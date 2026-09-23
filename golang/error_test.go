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
	"testing"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAsErrRpcStatus(t *testing.T) {
	// Keep unkeyed literals source-compatible with the public two-field type.
	rpcErr := &ErrRpcStatus{int32(v2.Code_TOO_MANY_REQUESTS), "throttled"}
	for _, tc := range []struct {
		name string
		err  error
		want *ErrRpcStatus
	}{
		{name: "nil"},
		{name: "ordinary error", err: errors.New("ordinary")},
		{name: "direct", err: rpcErr, want: rpcErr},
		{name: "wrapped", err: fmt.Errorf("send: %w", rpcErr), want: rpcErr},
		{name: "joined", err: errors.Join(errors.New("other"), fmt.Errorf("send: %w", rpcErr)), want: rpcErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := AsErrRpcStatus(tc.err)
			if got != tc.want || ok != (tc.want != nil) {
				t.Fatalf("AsErrRpcStatus(%v) = (%v, %v), want (%v, %v)", tc.err, got, ok, tc.want, tc.want != nil)
			}
		})
	}
}

func TestNormalizeGrpcErrorResourceExhausted(t *testing.T) {
	originalStatus, err := status.New(codes.ResourceExhausted, "quota exhausted").WithDetails(&errdetails.ErrorInfo{
		Reason: "QUOTA_EXHAUSTED", Domain: "broker",
	})
	if err != nil {
		t.Fatal(err)
	}
	cause := originalStatus.Err()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "direct", err: cause},
		{name: "wrapped", err: fmt.Errorf("rpc: %w", cause)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeGrpcError(tc.err)
			if normalized == tc.err || errors.Unwrap(normalized) != tc.err {
				t.Fatal("normalization must wrap the exact original error")
			}
			outer := fmt.Errorf("send: %w", normalized)
			var rpcErr *ErrRpcStatus
			if !errors.As(outer, &rpcErr) || rpcErr.GetCode() != 42900 || rpcErr.GetMessage() != status.Convert(tc.err).Message() {
				t.Fatalf("unexpected SDK status: %v", rpcErr)
			}
			if got, ok := AsErrRpcStatus(outer); !ok || got != rpcErr {
				t.Fatalf("AsErrRpcStatus() = (%v, %v), want (%v, true)", got, ok, rpcErr)
			}
			if normalized.Error() != rpcErr.Error() {
				t.Fatalf("normalized error = %q, want %q", normalized.Error(), rpcErr.Error())
			}
			if !errors.Is(outer, tc.err) || !errors.Is(outer, cause) {
				t.Fatal("original transport cause is not reachable via errors.Is")
			}
			var origin interface{ GRPCStatus() *status.Status }
			if !errors.As(outer, &origin) || !proto.Equal(origin.GRPCStatus().Proto(), originalStatus.Proto()) {
				t.Fatal("original gRPC status and details are not reachable via errors.As")
			}
			if status.Code(outer) != codes.ResourceExhausted {
				t.Fatalf("gRPC status code = %v, want ResourceExhausted", status.Code(outer))
			}
		})
	}
}

func TestNormalizeGrpcErrorLeavesOtherErrorsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "nil"},
		{name: "success", err: status.Error(codes.OK, "ok")},
		{name: "ordinary", err: errors.New("ordinary failure")},
		{name: "EOF", err: io.EOF},
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "wrapped cancellation", err: fmt.Errorf("rpc: %w", context.Canceled)},
		{name: "protocol throttling", err: &ErrRpcStatus{int32(v2.Code_TOO_MANY_REQUESTS), "throttled"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeGrpcError(tc.err); got != tc.err {
				t.Fatalf("normalizeGrpcError(%v) = %v, want the unchanged error object", tc.err, got)
			}
		})
	}
	for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
		if code == codes.ResourceExhausted {
			continue
		}
		t.Run(code.String(), func(t *testing.T) {
			original := status.Error(code, "other gRPC failure")
			for _, err := range []error{original, fmt.Errorf("rpc: %w", original)} {
				if got := normalizeGrpcError(err); got != err {
					t.Fatalf("normalizeGrpcError(%v) = %v, want the unchanged error object", err, got)
				}
			}
		})
	}
}
