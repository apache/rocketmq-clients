// Copyright 2018, OpenCensus Authors
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

#include "opencensus/tags/tag_key.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace opencensus {
namespace tags {

class TagKeyRegistry {
 public:
  static TagKeyRegistry* Get() {
    static TagKeyRegistry* global_tag_key_registry = new TagKeyRegistry;
    return global_tag_key_registry;
  }

  TagKey Register(absl::string_view name) ABSL_LOCKS_EXCLUDED(mu_);

  const std::string& TagKeyName(TagKey key) const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::ReaderMutexLock l(&mu_);
    return *registered_tag_keys_[key.id_];
  }

 private:
  mutable absl::Mutex mu_;
  // The registered tag keys. Tag key ids are indices into this vector.  Strings
  // are allocated individually so that they don't move around when the vector
  // storage moves due to resize.
  std::vector<std::unique_ptr<std::string>> registered_tag_keys_
      ABSL_GUARDED_BY(mu_);
  // A map from names to IDs.
  // TODO: change to string_view when a suitable hash is available.
  std::unordered_map<std::string, uint64_t> id_map_ ABSL_GUARDED_BY(mu_);
};

TagKey TagKeyRegistry::Register(absl::string_view name) {
  absl::MutexLock l(&mu_);
  const std::string string_name(name);
  const auto it = id_map_.find(string_name);
  if (it == id_map_.end()) {
    const uint64_t id = registered_tag_keys_.size();
    registered_tag_keys_.emplace_back(absl::make_unique<std::string>(name));
    id_map_.emplace_hint(it, string_name, id);
    return TagKey(id);
  }
  return TagKey(it->second);
}

TagKey TagKey::Register(absl::string_view name) {
  return TagKeyRegistry::Get()->Register(name);
}

const std::string& TagKey::name() const {
  return TagKeyRegistry::Get()->TagKeyName(*this);
}

}  // namespace tags
}  // namespace opencensus
