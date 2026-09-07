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
	"sync"
	"testing"
)

type recordedLogEntry struct {
	level     string
	message   string
	keyValues []any
}

type recordingLoggerState struct {
	mu      sync.Mutex
	entries []recordedLogEntry
}

func (s *recordingLoggerState) append(entry recordedLogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
}

func (s *recordingLoggerState) find(message string) (recordedLogEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		if entry.message == message {
			return entry, true
		}
	}
	return recordedLogEntry{}, false
}

type recordingLogger struct {
	state     *recordingLoggerState
	keyValues []any
}

var _ Logger = (*recordingLogger)(nil)

func (l *recordingLogger) record(level string, message string, keyValues ...any) {
	allKeyValues := append([]any{}, l.keyValues...)
	allKeyValues = append(allKeyValues, keyValues...)
	l.state.append(recordedLogEntry{
		level:     level,
		message:   message,
		keyValues: allKeyValues,
	})
}

func (l *recordingLogger) Debug(message string, keyValues ...any) {
	l.record("debug", message, keyValues...)
}

func (l *recordingLogger) Info(message string, keyValues ...any) {
	l.record("info", message, keyValues...)
}

func (l *recordingLogger) Warn(message string, keyValues ...any) {
	l.record("warn", message, keyValues...)
}

func (l *recordingLogger) Error(message string, keyValues ...any) {
	l.record("error", message, keyValues...)
}

func (l *recordingLogger) With(keyValues ...any) Logger {
	allKeyValues := append([]any{}, l.keyValues...)
	allKeyValues = append(allKeyValues, keyValues...)
	return &recordingLogger{state: l.state, keyValues: allKeyValues}
}

func TestSetLoggerIsUsedByNewClients(t *testing.T) {
	state := &recordingLoggerState{}
	SetLogger(&recordingLogger{state: state})
	t.Cleanup(ResetLogger)

	config := &Config{
		Endpoint:      "127.0.0.1:8081",
		ConsumerGroup: "test-group",
	}
	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	consumer, err := NewSimpleConsumer(config)
	if err != nil {
		t.Fatalf("failed to create simple consumer: %v", err)
	}

	sugarBaseLogger.Infof("package log: %d", 1)
	defaultProducer := producer.(*defaultProducer)
	defaultConsumer := consumer.(*defaultSimpleConsumer)
	defaultProducer.cli.log.Info("producer log")
	defaultConsumer.cli.log.Warnf("consumer log: %s", "warning")

	assertRecordedLog(t, state, "package log: 1", "info", "")
	assertRecordedLog(t, state, "producer log", "info", defaultProducer.cli.GetClientID())
	assertRecordedLog(t, state, "consumer log: warning", "warn", defaultConsumer.cli.GetClientID())
}

func assertRecordedLog(t *testing.T, state *recordingLoggerState, message string, level string, clientID string) {
	t.Helper()
	entry, ok := state.find(message)
	if !ok {
		t.Fatalf("log entry %q was not recorded", message)
	}
	if entry.level != level {
		t.Fatalf("log entry %q has level %q, want %q", message, entry.level, level)
	}
	if clientID == "" {
		if len(entry.keyValues) != 0 {
			t.Fatalf("package log has unexpected key-values: %v", entry.keyValues)
		}
		return
	}
	if len(entry.keyValues) != 2 || entry.keyValues[0] != "client_id" || entry.keyValues[1] != clientID {
		t.Fatalf("log entry %q has key-values %v, want [client_id %s]", message, entry.keyValues, clientID)
	}
}
