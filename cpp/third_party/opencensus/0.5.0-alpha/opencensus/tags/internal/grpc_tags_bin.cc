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

#include "opencensus/tags/propagation/grpc_tags_bin.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/strings/string_view.h"
#include "opencensus/common/internal/varint.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

using opencensus::common::AppendVarint32;
using opencensus::common::ParseVarint32;

namespace opencensus {
namespace tags {
namespace propagation {
namespace {

constexpr char kVersionId = '\0';
constexpr char kTagFieldId = '\0';
constexpr int kMaxLen = 8192;

}  // namespace

bool FromGrpcTagsBinHeader(absl::string_view header, TagMap* out) {
  std::unordered_map<std::string, absl::string_view> keys_vals;
  if (header.length() < 1) {
    return false;  // Too short.
  }
  if (header.length() > kMaxLen) {
    return false;  // Too long.
  }
  if (header[0] != kVersionId) {
    return false;  // Wrong version.
  }
  header = header.substr(1);
  while (header.length() > 0) {
    // Parse tag field id.
    if (header[0] != kTagFieldId) {
      return false;  // Wrong field id.
    }
    header = header.substr(1);

    // Parse key.
    absl::string_view key;
    {
      uint32_t key_len;
      if (!ParseVarint32(&header, &key_len)) {
        return false;  // Invalid key_len.
      }
      if (key_len > header.length()) {
        return false;  // Key len longer than remaining buffer.
      }
      key = header.substr(0, key_len);
      header = header.substr(key_len);
    }

    // Parse val.
    absl::string_view val;
    {
      uint32_t val_len;
      if (!ParseVarint32(&header, &val_len)) {
        return false;  // Invalid val_len.
      }
      if (val_len > header.length()) {
        return false;  // Val len longer than remaining buffer.
      }
      val = header.substr(0, val_len);
      header = header.substr(val_len);
    }

    // Drop empty keys.
    if (!key.empty()) {
      // For duplicate keys, last wins.
      keys_vals[std::string(key)] = val;
    }
  }

  // Convert to tagmap.
  std::vector<std::pair<opencensus::tags::TagKey, std::string>> tags;
  tags.reserve(keys_vals.size());
  for (const auto& kv : keys_vals) {
    tags.emplace_back(TagKey::Register(kv.first), std::string(kv.second));
  }
  *out = TagMap(std::move(tags));
  return true;
}

std::string ToGrpcTagsBinHeader(const TagMap& tags) {
  std::string out;
  out.push_back(kVersionId);
  for (const auto& key_val : tags.tags()) {
    const auto& key = key_val.first;
    const auto& val = key_val.second;
    out.push_back(kTagFieldId);
    AppendVarint32(key.name().length(), &out);
    out.append(key.name());
    AppendVarint32(val.length(), &out);
    // Encoded value must be UTF-8.
    out.append(val);
    if (out.size() > kMaxLen) {
      break;
    }
  }
  if (out.size() > kMaxLen) {
    return "";
  }
  return out;
}

}  // namespace propagation
}  // namespace tags
}  // namespace opencensus
