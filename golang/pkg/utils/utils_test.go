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
	"compress/gzip"
	"testing"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
)

func TestMod(t *testing.T) {
	r := Mod(769024950, 4820134)
	if r != 2623644 {
		t.Error()
	}
	r = Mod(-500246785, 9326848)
	if r != 3403007 {
		t.Error()
	}
}

func TestMod64(t *testing.T) {
	r := Mod64(769024950, 4820134)
	if r != 2623644 {
		t.Error()
	}
	r = Mod64(-500246785, 9326848)
	if r != 3403007 {
		t.Error()
	}
}

func TestParseAddress(t *testing.T) {
	r := ParseAddress(nil)
	if r != "" {
		t.Error()
	}
	r = ParseAddress(&v2.Address{
		Host: "127.0.0.1",
		Port: 80,
	})
	if r != "127.0.0.1:80" {
		t.Error()
	}
}

func TestParseTarget(t *testing.T) {
	_, err := ParseTarget("127.0.0.1")
	if err == nil {
		t.Error(err)
	}
	_, err = ParseTarget("127")
	if err == nil {
		t.Error(err)
	}
	_, err = ParseTarget("127.0.0.1:80")
	if err != nil {
		t.Error(err)
	}
}

func TestMatchMessageType(t *testing.T) {
	if MatchMessageType(&v2.MessageQueue{}, v2.MessageType_DELAY) {
		t.Error()
	}
	if MatchMessageType(&v2.MessageQueue{
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_NORMAL},
	}, v2.MessageType_DELAY) {
		t.Error()
	}
	if !MatchMessageType(&v2.MessageQueue{
		AcceptMessageTypes: []v2.MessageType{v2.MessageType_DELAY},
	}, v2.MessageType_DELAY) {
		t.Error()
	}
}

func TestGZIPDecode(t *testing.T) {
	_, err := GZIPDecode([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	if err != gzip.ErrHeader {
		t.Error()
	}
	bytes, err := GZIPDecode([]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 42, 202, 79, 206, 78, 45, 201, 45, 212, 77, 206, 201, 76, 205, 43, 209, 77, 207, 7, 0, 0, 0, 255, 255, 1, 0, 0, 255, 255, 97, 36, 132, 114, 18, 0, 0, 0})
	if err != nil {
		t.Error()
	}
	if string(bytes) != "rocketmq-client-go" {
		t.Error()
	}
}

func TestSelectAnAddress(t *testing.T) {
	if SelectAnAddress(nil) != nil {
		t.Error()
	}
	address := SelectAnAddress(&v2.Endpoints{
		Addresses: []*v2.Address{
			{
				Host: "127.0.0.1",
				Port: 80,
			},
			{
				Host: "127.0.0.1",
				Port: 81,
			},
			{
				Host: "127.0.0.1",
				Port: 82,
			},
		},
	})
	if address == nil {
		t.Error()
	}
	if address.Host != "127.0.0.1" {
		t.Error()
	}
	if address.Port < 80 || address.Port > 82 {
		t.Error()
	}
}

func TestCompareEndpoints(t *testing.T) {
	if CompareEndpoints(nil, nil) != true {
		t.Error()
	}
	if CompareEndpoints(&v2.Endpoints{}, nil) != false {
		t.Error()
	}
	if CompareEndpoints(
		&v2.Endpoints{
			Addresses: []*v2.Address{
				{
					Host: "127.0.0.1",
					Port: 80,
				},
				{
					Host: "127.0.0.1",
					Port: 81,
				},
				{
					Host: "127.0.0.1",
					Port: 82,
				},
			},
		},
		&v2.Endpoints{
			Addresses: []*v2.Address{
				{
					Host: "127.0.0.1",
					Port: 80,
				},
				{
					Host: "127.0.0.1",
					Port: 81,
				},
			},
		}) {
		t.Error()
	}
	if !CompareEndpoints(
		&v2.Endpoints{
			Addresses: []*v2.Address{
				{
					Host: "127.0.0.1",
					Port: 80,
				},
				{
					Host: "127.0.0.1",
					Port: 81,
				},
				{
					Host: "127.0.0.1",
					Port: 82,
				},
			},
		},
		&v2.Endpoints{
			Addresses: []*v2.Address{
				{
					Host: "127.0.0.1",
					Port: 82,
				},
				{
					Host: "127.0.0.1",
					Port: 81,
				},
				{
					Host: "127.0.0.1",
					Port: 80,
				},
			},
		}) {
		t.Error()
	}
}
