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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextMessageId(t *testing.T) {
	messageId := GetMessageIdCodecInstance().NextMessageId()
	assert.Equal(t, MESSAGE_ID_LENGTH_FOR_V1_OR_LATER, len(messageId.String()))
}

func TestNextMessageIdWithNoRepetition(t *testing.T) {
	messageIds := map[string]MessageId{}
	messageIdCount := 64
	for i := 0; i < messageIdCount; i++ {
		messageId := GetMessageIdCodecInstance().NextMessageId()
		messageIds[messageId.String()] = messageId
	}
	assert.Equal(t, messageIdCount, len(messageIds))
}

func TestDecode(t *testing.T) {
	messageIdString := "0156F7E71C361B21BC024CCDBE00000000"
	messageId := GetMessageIdCodecInstance().Decode(messageIdString)
	assert.Equal(t, MESSAGE_ID_VERSION_V1, messageId.GetVersion())
	assert.Equal(t, messageIdString, messageId.String())
}
