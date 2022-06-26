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

type loggerOptions struct {
	filename   string
	maxSize    int
	maxBackups int
	maxAge     int
}

var defaultLoggerOptions = loggerOptions{
	filename:   "/tmp/rocketmq-logger.log",
	maxSize:    500,
	maxBackups: 3,
	maxAge:     7,
}

// A LoggerOption sets options such as filename, etc.
type LoggerOption interface {
	apply(*loggerOptions)
}

// funcLoggerOption wraps a function that modifies loggerOptions into an
// implementation of the LoggerOption interface.
type funcLoggerOption struct {
	f func(*loggerOptions)
}

func (fdo *funcLoggerOption) apply(do *loggerOptions) {
	fdo.f(do)
}

func newFuncLoggerOption(f func(*loggerOptions)) *funcLoggerOption {
	return &funcLoggerOption{
		f: f,
	}
}

// WithFileName returns a LoggerOption that sets filename for logger.
// Note: Default it uses /tmp/zap-logger.log.
func WithFileName(fn string) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.filename = fn
	})
}

// WithMaxSize returns a LoggerOption that sets maxSize for logger.
// MaxSize is the maximum size in megabytes of the log file before it gets rotated.
// Note: Default it uses 500 megabytes.
func WithMaxSize(maxSize int) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.maxSize = maxSize
	})
}

// WithMaxBackups returns a LoggerOption that sets maxBackups for logger.
// MaxBackups is the maximum number of old log files to retain.
// Note: Default it uses 3 .
func WithMaxBackups(maxBackups int) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.maxBackups = maxBackups
	})
}

// WithMaxAge returns a LoggerOption that sets maxAge for logger.
// MaxAge is the maximum number of days to retain old log files based on the timestamp encoded in their filename.
// Note: Default it uses 7 days.
func WithMaxAge(maxAge int) LoggerOption {
	return newFuncLoggerOption(func(o *loggerOptions) {
		o.maxAge = maxAge
	})
}
