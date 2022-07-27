// Copyright 2019, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "opencensus/trace/propagation/grpc_trace_bin.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {
namespace propagation {
namespace {

MATCHER(IsValid, "is a valid SpanContext") { return arg.IsValid(); }
MATCHER(IsInvalid, "is an invalid SpanContext") { return !arg.IsValid(); }

TEST(GrpcTraceBinTest, ParseFull) {
  constexpr unsigned char header_data[] = {
      0,                                               // version_id
      0,                                               // trace_id field
      0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
      0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
      1,                                               // span_id field
      0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
      2,                                               // trace_options field
      1,                                               // is sampled
  };
  absl::string_view header(reinterpret_cast<const char*>(header_data),
                           sizeof(header_data));
  SpanContext ctx = FromGrpcTraceBinHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("64656667686970717273747576777879-8182838485868788-01",
            ctx.ToString());
  EXPECT_EQ(header, ToGrpcTraceBinHeader(ctx));
}

TEST(GrpcTraceBinTest, ParseNotSampled) {
  constexpr unsigned char header_data[] = {
      0,                                               // version_id
      0,                                               // trace_id field
      0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
      0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
      1,                                               // span_id field
      0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
      2,                                               // trace_options field
      0,                                               // not sampled
  };
  absl::string_view header(reinterpret_cast<const char*>(header_data),
                           sizeof(header_data));
  SpanContext ctx = FromGrpcTraceBinHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_FALSE(ctx.trace_options().IsSampled())
      << "Expecting to not be sampled.";
  EXPECT_EQ("64656667686970717273747576777879-8182838485868788-00",
            ctx.ToString());
  EXPECT_EQ(header, ToGrpcTraceBinHeader(ctx));
}

TEST(GrpcTraceBinTest, ExpectedFailures) {
#define INVALID(hdr)                                                 \
  EXPECT_THAT(FromGrpcTraceBinHeader(absl::string_view(              \
                  reinterpret_cast<const char*>(hdr), sizeof(hdr))), \
              IsInvalid())
  INVALID("");
  {
    constexpr unsigned char header[] = {
        0,                                               // version_id
        0,                                               // trace_id field
        0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
        0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
        1,                                               // span_id field
        0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
        2,                                               // options missing
    };
    INVALID(header) << "Too short.";
  }
  {
    constexpr unsigned char header[] = {
        123,                                             // version_id
        0,                                               // trace_id field
        0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
        0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
        1,                                               // span_id field
        0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
        2,                                               // trace_options field
        1,                                               // tracing enabled
    };
    INVALID(header) << "Wrong version_id.";
  }
  {
    constexpr unsigned char header[] = {
        0,                                               // version_id
        0,                                               // trace_id field
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // lo
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // hi
        1,                                               // span_id field
        0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
        2,                                               // trace_options field
        1,                                               // tracing enabled
    };
    INVALID(header) << "Invalid trace_id.";
  }
  {
    constexpr unsigned char header[] = {
        0,                                               // version_id
        0,                                               // trace_id field
        0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
        0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
        1,                                               // span_id field
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // span_id
        2,                                               // trace_options field
        1,                                               // tracing enabled
    };
    INVALID(header) << "Invalid span_id.";
  }
#undef INVALID
}

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
