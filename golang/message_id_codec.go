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
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-clients/golang/pkg/utils"
)

/**
The codec for the message-id.

Codec here provides the following two functions:

1. Provide decoding function of message-id of all versions above v0.

2. Provide a generator of message-id of v1 version.

The message-id of versions above V1 consists of 17 bytes in total. The first two bytes represent the version
number. For V1, these two bytes are 0x0001.

V1 message id example

  ┌──┬────────────┬────┬────────┬────────┐
  │01│56F7E71C361B│21BC│024CCDBE│00000000│
  └──┴────────────┴────┴────────┴────────┘


V1 version message id generation rules

                    process id(lower 2bytes)
                            ▲
mac address(lower 6bytes)   │   sequence number(big endian)
                   ▲        │          ▲ (4bytes)
                   │        │          │
             ┌─────┴─────┐ ┌┴┐ ┌───┐ ┌─┴─┐
      0x01+  │     6     │ │2│ │ 4 │ │ 4 │
             └───────────┘ └─┘ └─┬─┘ └───┘
                                 │
                                 ▼
          seconds since 2021-01-01 00:00:00(UTC+0)
                        (lower 4bytes)

*/
type MessageIdCodec interface {
	NextMessageId() MessageId
	Decode(messageId string) MessageId
}

var _ MessageIdCodec = &messageIdCodec{}

const (
	MESSAGE_ID_LENGTH_FOR_V1_OR_LATER        = 34
	MESSAGE_ID_VERSION_V0             string = "00"
	MESSAGE_ID_VERSION_V1             string = "01"
)

var (
	messageIdCodecInstance  MessageIdCodec
	processFixedStringV1    string
	secondsSinceCustomEpoch int64
	secondsStartTimestamp   int64
	seconds                 int64
	sequence                int32
)

func init() {
	var buffer bytes.Buffer
	prefix0 := utils.GetMacAddress()
	if len(prefix0) < 6 {
		prefix0 = make([]byte, 6)
		_, err := rand.Read(prefix0)
		if err != nil {
			sugarBaseLogger.Fatalf("failed to get mac address: %v", err)
		}
	}
	if err := binary.Write(&buffer, binary.BigEndian, prefix0); err != nil {
		sugarBaseLogger.Fatalf("failed to write buffer: %v", err)
	}

	pid := os.Getpid()

	if err := binary.Write(&buffer, binary.BigEndian, uint16(pid)); err != nil {
		sugarBaseLogger.Fatalf("failed to write pid: %v", err)
	}

	encodedStr := hex.EncodeToString(buffer.Bytes())
	processFixedStringV1 = strings.ToUpper(encodedStr)

	secondsSinceCustomEpoch = time.Now().Unix() - time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	// TODO Implement System.nanoTime() in golang, see https://github.com/golang/go/issues/16658
	secondsStartTimestamp = time.Now().Unix()
	seconds = deltaSeconds()

	sequence = -1

	messageIdCodecInstance = &messageIdCodec{}
}

func GetMessageIdCodecInstance() MessageIdCodec {
	return messageIdCodecInstance
}

type messageIdCodec struct{}

func deltaSeconds() int64 {

	return time.Now().Unix() - secondsStartTimestamp + secondsSinceCustomEpoch
}

func (mic *messageIdCodec) NextMessageId() MessageId {
	var buffer bytes.Buffer

	deltaSeconds := deltaSeconds()
	if seconds != deltaSeconds {
		seconds = deltaSeconds
	}

	if err := binary.Write(&buffer, binary.BigEndian, uint32(deltaSeconds)); err != nil {
		return nil
	}
	if err := binary.Write(&buffer, binary.BigEndian, uint32(atomic.AddInt32(&sequence, 1))); err != nil {
		return nil
	}

	suffix := processFixedStringV1 + strings.ToUpper(hex.EncodeToString(buffer.Bytes()))

	return NewMessageId(MESSAGE_ID_VERSION_V1, suffix)
}

func (mic *messageIdCodec) Decode(messageId string) MessageId {
	if MESSAGE_ID_LENGTH_FOR_V1_OR_LATER != len(messageId) {
		return NewMessageId(MESSAGE_ID_VERSION_V0, messageId)
	}
	return NewMessageId(messageId[0:2], messageId[2:])
}
