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

package zaplog

import (
	"context"
	"time"

	"github.com/apache/rocketmq-clients/golang/pkg/zaplog/ctxzap"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryClientInterceptor returns a new unary client interceptor that optionally logs the execution of external gRPC calls.
func UnaryClientInterceptor(logger *zap.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		newCtx := ctxzap.WithLogger(ctx, logger)
		err := invoker(newCtx, method, req, reply, cc, opts...)
		l := ctxzap.Logger(newCtx)
		l = l.With(
			zap.Time("in", start),
			zap.Int64("cost", time.Since(start).Milliseconds()),
			zap.String("code", status.Code(err).String()),
			zap.String("method", method),
			zap.String("system", "grpc"),
			zap.String("kind", "client"),
		)
		l.Info("rocketmq logger info")
		if err != nil {
			l.With(
				zap.Error(err),
				zap.Any("request", req),
				zap.Any("response", reply),
			).Error("rocketmq logger error")
		}
		return err
	}
}
