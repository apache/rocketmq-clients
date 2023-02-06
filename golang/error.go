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
	"errors"
	"fmt"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
)

type ErrRpcStatus struct {
	Code    int32
	Message string
}

func (err *ErrRpcStatus) GetCode() int32 {
	return err.Code
}

func (err *ErrRpcStatus) GetMessage() string {
	return err.Message
}

func (err *ErrRpcStatus) Error() string {
	codeName, ok := v2.Code_name[err.Code]
	if !ok {
		codeName = string(err.Code)
	}
	return fmt.Sprintf("CODE: %s, MESSAGE: %s", codeName, err.Message)
}

var _ = error(&ErrRpcStatus{})

func AsErrRpcStatus(err error) (*ErrRpcStatus, bool) {
	if err == nil {
		return nil, false
	}
	target, ok := err.(*ErrRpcStatus)
	if ok {
		return target, true
	}
	err = errors.Unwrap(err)
	return AsErrRpcStatus(err)
}
