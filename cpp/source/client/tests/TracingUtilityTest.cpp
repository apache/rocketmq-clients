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
#include "TracingUtility.h"
#include "gtest/gtest.h"
#include <iostream>

using namespace opentelemetry::trace;

ROCKETMQ_NAMESPACE_BEGIN

template <typename T>
static std::string hex(const T& id_item) {
  char buf[T::kSize * 2];
  id_item.ToLowerBase16(buf);
  return std::string(buf, sizeof(buf));
}

TEST(TracingUtilityTest, testInject) {
  constexpr uint8_t buf_span[] = {1, 2, 3, 4, 5, 6, 7, 8};
  constexpr uint8_t buf_trace[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  SpanContext span_context{TraceId{buf_trace}, SpanId{buf_span}, TraceFlags{true}, true};
  EXPECT_EQ(TracingUtility::injectSpanContextToTraceParent(span_context),
            "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01");
}

TEST(TracingUtilityTest, testExtract) {
  SpanContext span_context =
      TracingUtility::extractContextFromTraceParent("00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01");

  EXPECT_EQ(hex(span_context.trace_id()), "0102030405060708090a0b0c0d0e0f10");
  EXPECT_EQ(hex(span_context.span_id()), "0102030405060708");
  EXPECT_TRUE(span_context.IsSampled());
  EXPECT_TRUE(span_context.IsRemote());
}

ROCKETMQ_NAMESPACE_END