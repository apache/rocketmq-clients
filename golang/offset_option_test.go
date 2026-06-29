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

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

func TestOffsetOptionToProtobuf(t *testing.T) {
	tests := []struct {
		name        string
		option      OffsetOption
		offsetType  interface{}
		expectValue int64
	}{
		{
			name:        "last offset",
			option:      LastOffset,
			offsetType:  &v2.OffsetOption_Policy_{},
			expectValue: int64(v2.OffsetOption_LAST),
		},
		{
			name:        "offset",
			option:      mustOffsetOption(NewOffsetOptionWithOffset(100)),
			offsetType:  &v2.OffsetOption_Offset{},
			expectValue: 100,
		},
		{
			name:        "tail n",
			option:      mustOffsetOption(NewOffsetOptionWithTailN(10)),
			offsetType:  &v2.OffsetOption_TailN{},
			expectValue: 10,
		},
		{
			name:        "timestamp",
			option:      mustOffsetOption(NewOffsetOptionWithTimestamp(1234567890)),
			offsetType:  &v2.OffsetOption_Timestamp{},
			expectValue: 1234567890,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protobufOption := tt.option.toProtobuf()
			switch tt.offsetType.(type) {
			case *v2.OffsetOption_Policy_:
				if _, ok := protobufOption.GetOffsetType().(*v2.OffsetOption_Policy_); !ok {
					t.Fatalf("expected policy offset type, got %T", protobufOption.GetOffsetType())
				}
				if int64(protobufOption.GetPolicy()) != tt.expectValue {
					t.Errorf("expected policy %d, got %d", tt.expectValue, protobufOption.GetPolicy())
				}
			case *v2.OffsetOption_Offset:
				if _, ok := protobufOption.GetOffsetType().(*v2.OffsetOption_Offset); !ok {
					t.Fatalf("expected offset type, got %T", protobufOption.GetOffsetType())
				}
				if protobufOption.GetOffset() != tt.expectValue {
					t.Errorf("expected offset %d, got %d", tt.expectValue, protobufOption.GetOffset())
				}
			case *v2.OffsetOption_TailN:
				if _, ok := protobufOption.GetOffsetType().(*v2.OffsetOption_TailN); !ok {
					t.Fatalf("expected tail_n type, got %T", protobufOption.GetOffsetType())
				}
				if protobufOption.GetTailN() != tt.expectValue {
					t.Errorf("expected tail_n %d, got %d", tt.expectValue, protobufOption.GetTailN())
				}
			case *v2.OffsetOption_Timestamp:
				if _, ok := protobufOption.GetOffsetType().(*v2.OffsetOption_Timestamp); !ok {
					t.Fatalf("expected timestamp type, got %T", protobufOption.GetOffsetType())
				}
				if protobufOption.GetTimestamp() != tt.expectValue {
					t.Errorf("expected timestamp %d, got %d", tt.expectValue, protobufOption.GetTimestamp())
				}
			}
		})
	}
}

func TestOffsetOptionRejectsNegativeValue(t *testing.T) {
	if _, err := NewOffsetOptionWithOffset(-1); err == nil {
		t.Fatal("expected error for negative offset")
	}
	if _, err := NewOffsetOptionWithTailN(-1); err == nil {
		t.Fatal("expected error for negative tailN")
	}
	if _, err := NewOffsetOptionWithTimestamp(-1); err == nil {
		t.Fatal("expected error for negative timestamp")
	}
}

func mustOffsetOption(option OffsetOption, err error) OffsetOption {
	if err != nil {
		panic(err)
	}
	return option
}
