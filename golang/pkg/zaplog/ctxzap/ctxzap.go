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

package ctxzap

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerKey struct{}

type ctxLogger struct {
	logger *zap.Logger
	fields []zapcore.Field
}

var (
	nullLogger      = zap.NewNop()
	StdoutLogger, _ = zap.NewProduction()
)

// WithFields adds zap fields to the logger.
func WithFields(ctx context.Context, fields ...zapcore.Field) {
	l, ok := ctx.Value(loggerKey{}).(*ctxLogger)
	if !ok || l == nil {
		return
	}
	l.fields = append(l.fields, fields...)
}

// Logger takes the call-scoped Logger from zap log middleware.
// It always returns a Logger that has all the zapcore.Field updated.
func Logger(ctx context.Context) *zap.Logger {
	l, ok := ctx.Value(loggerKey{}).(*ctxLogger)
	if !ok || l == nil {
		return nullLogger
	}
	return l.logger.With(l.fields...)
}

// WithLogger adds the zap.Logger to the context for extraction later.
// Returning the new context that has been created.
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, &ctxLogger{
		logger: logger,
	})
}
