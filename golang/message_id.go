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

// MessageId Abstract message id
type MessageId interface {
	// GetVersion Get the version of the messageId
	GetVersion() string
	// String string-formed string id
	String() string
}

var _ MessageId = &messageId{}

func NewMessageId(version, suffix string) MessageId {
	return &messageId{
		version: version,
		suffix:  suffix,
	}
}

type messageId struct {
	version string
	suffix  string
}

func (m *messageId) GetVersion() string {
	return m.version
}

func (m *messageId) String() string {
	if MESSAGE_ID_VERSION_V0 == m.version {
		return m.suffix
	}
	return m.version + m.suffix
}
