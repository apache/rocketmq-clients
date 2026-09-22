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
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
)

// Logger is the logging interface used by RocketMQ clients.
//
// Implementations must be safe for concurrent use. The keyValues arguments are
// interpreted as alternating keys and values. With returns a logger that
// includes the supplied key-value pairs in every log entry.
type Logger interface {
	Debug(msg string, keyValues ...any)
	Info(msg string, keyValues ...any)
	Warn(msg string, keyValues ...any)
	Error(msg string, keyValues ...any)
	With(keyValues ...any) Logger
}

type fatalLogger interface {
	Fatal(msg string, keyValues ...any)
}

type formattedLogger interface {
	Debugf(template string, args ...any)
	Infof(template string, args ...any)
	Warnf(template string, args ...any)
	Errorf(template string, args ...any)
}

// SetLogger replaces the package-level logger used by RocketMQ clients.
//
// SetLogger must be called before creating any producer or consumer. The
// logger must be non-nil and safe for concurrent use. RocketMQ does not take
// ownership of the logger or close resources owned by it.
func SetLogger(logger Logger) {
	if logger == nil {
		panic("rocketmq: logger must not be nil")
	}
	sugarBaseLogger = newInternalLogger(logger)
}

// internalLogger preserves the logging methods used inside the client while
// keeping the public Logger contract small and independent of Zap.
type internalLogger struct {
	logger    Logger
	formatted formattedLogger
}

func newInternalLogger(logger Logger) *internalLogger {
	internal := &internalLogger{logger: logger}
	internal.formatted, _ = logger.(formattedLogger)
	return internal
}

func (l *internalLogger) Debug(args ...any) {
	l.logger.Debug(fmt.Sprint(args...))
}

func (l *internalLogger) Debugf(template string, args ...any) {
	if l.formatted != nil {
		l.formatted.Debugf(template, args...)
		return
	}
	l.logger.Debug(fmt.Sprintf(template, args...))
}

func (l *internalLogger) Info(args ...any) {
	l.logger.Info(fmt.Sprint(args...))
}

func (l *internalLogger) Infof(template string, args ...any) {
	if l.formatted != nil {
		l.formatted.Infof(template, args...)
		return
	}
	l.logger.Info(fmt.Sprintf(template, args...))
}

func (l *internalLogger) Infoln(args ...any) {
	l.logger.Info(strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}

func (l *internalLogger) Warnf(template string, args ...any) {
	if l.formatted != nil {
		l.formatted.Warnf(template, args...)
		return
	}
	l.logger.Warn(fmt.Sprintf(template, args...))
}

func (l *internalLogger) Error(args ...any) {
	l.logger.Error(fmt.Sprint(args...))
}

func (l *internalLogger) Errorf(template string, args ...any) {
	if l.formatted != nil {
		l.formatted.Errorf(template, args...)
		return
	}
	l.logger.Error(fmt.Sprintf(template, args...))
}

func (l *internalLogger) Fatalf(template string, args ...any) {
	message := fmt.Sprintf(template, args...)
	if logger, ok := l.logger.(fatalLogger); ok {
		logger.Fatal(message)
	} else {
		l.logger.Error(message)
	}
	os.Exit(1)
}

func (l *internalLogger) With(keyValues ...any) *internalLogger {
	return newInternalLogger(l.logger.With(keyValues...))
}

type zapLogger struct {
	logger *zap.SugaredLogger
}

var (
	_ Logger          = (*zapLogger)(nil)
	_ formattedLogger = (*zapLogger)(nil)
)

func newZapLogger(logger *zap.Logger) Logger {
	return &zapLogger{logger: logger.WithOptions(zap.AddCallerSkip(2)).Sugar()}
}

func (l *zapLogger) Debug(msg string, keyValues ...any) {
	l.logger.Debugw(msg, keyValues...)
}

func (l *zapLogger) Debugf(template string, args ...any) {
	l.logger.Debugf(template, args...)
}

func (l *zapLogger) Info(msg string, keyValues ...any) {
	l.logger.Infow(msg, keyValues...)
}

func (l *zapLogger) Infof(template string, args ...any) {
	l.logger.Infof(template, args...)
}

func (l *zapLogger) Warn(msg string, keyValues ...any) {
	l.logger.Warnw(msg, keyValues...)
}

func (l *zapLogger) Warnf(template string, args ...any) {
	l.logger.Warnf(template, args...)
}

func (l *zapLogger) Error(msg string, keyValues ...any) {
	l.logger.Errorw(msg, keyValues...)
}

func (l *zapLogger) Errorf(template string, args ...any) {
	l.logger.Errorf(template, args...)
}

func (l *zapLogger) Fatal(msg string, keyValues ...any) {
	l.logger.Fatalw(msg, keyValues...)
}

func (l *zapLogger) With(keyValues ...any) Logger {
	return &zapLogger{logger: l.logger.With(keyValues...)}
}
