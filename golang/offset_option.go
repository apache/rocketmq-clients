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

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

type offsetOptionType int

const (
	offsetOptionTypePolicy offsetOptionType = iota
	offsetOptionTypeOffset
	offsetOptionTypeTailN
	offsetOptionTypeTimestamp
)

// OffsetOption specifies the starting offset for lite topic consumption.
type OffsetOption struct {
	optionType offsetOptionType
	value      int64
}

var (
	// LastOffset starts consuming from the last consumed offset of the consumer group.
	LastOffset = OffsetOption{optionType: offsetOptionTypePolicy, value: int64(v2.OffsetOption_LAST)}
	// MinOffset starts consuming from the minimum available offset.
	MinOffset = OffsetOption{optionType: offsetOptionTypePolicy, value: int64(v2.OffsetOption_MIN)}
	// MaxOffset starts consuming from the maximum available offset, skipping existing messages.
	MaxOffset = OffsetOption{optionType: offsetOptionTypePolicy, value: int64(v2.OffsetOption_MAX)}
)

// NewOffsetOptionWithOffset creates an OffsetOption from a specific offset.
func NewOffsetOptionWithOffset(offset int64) (OffsetOption, error) {
	if offset < 0 {
		return OffsetOption{}, fmt.Errorf("offset must be greater than or equal to 0")
	}
	return OffsetOption{optionType: offsetOptionTypeOffset, value: offset}, nil
}

// NewOffsetOptionWithTailN creates an OffsetOption from the last N messages.
func NewOffsetOptionWithTailN(tailN int64) (OffsetOption, error) {
	if tailN < 0 {
		return OffsetOption{}, fmt.Errorf("tailN must be greater than or equal to 0")
	}
	return OffsetOption{optionType: offsetOptionTypeTailN, value: tailN}, nil
}

// NewOffsetOptionWithTimestamp creates an OffsetOption from a Unix millisecond timestamp.
func NewOffsetOptionWithTimestamp(timestamp int64) (OffsetOption, error) {
	if timestamp < 0 {
		return OffsetOption{}, fmt.Errorf("timestamp must be greater than or equal to 0")
	}
	return OffsetOption{optionType: offsetOptionTypeTimestamp, value: timestamp}, nil
}

func (option OffsetOption) toProtobuf() *v2.OffsetOption {
	switch option.optionType {
	case offsetOptionTypePolicy:
		return &v2.OffsetOption{
			OffsetType: &v2.OffsetOption_Policy_{
				Policy: v2.OffsetOption_Policy(option.value),
			},
		}
	case offsetOptionTypeOffset:
		return &v2.OffsetOption{
			OffsetType: &v2.OffsetOption_Offset{
				Offset: option.value,
			},
		}
	case offsetOptionTypeTailN:
		return &v2.OffsetOption{
			OffsetType: &v2.OffsetOption_TailN{
				TailN: option.value,
			},
		}
	case offsetOptionTypeTimestamp:
		return &v2.OffsetOption{
			OffsetType: &v2.OffsetOption_Timestamp{
				Timestamp: option.value,
			},
		}
	default:
		return nil
	}
}
