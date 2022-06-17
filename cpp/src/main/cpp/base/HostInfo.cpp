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
#include "HostInfo.h"
#include "absl/strings/match.h"
#include "rocketmq/RocketMQ.h"
#include <cstdlib>
#include <cstring>

ROCKETMQ_NAMESPACE_BEGIN

const char* HostInfo::ENV_LABEL_SITE = "SIGMA_APP_SITE";
const char* HostInfo::ENV_LABEL_UNIT = "SIGMA_APP_UNIT";
const char* HostInfo::ENV_LABEL_APP = "SIGMA_APP_NAME";
const char* HostInfo::ENV_LABEL_STAGE = "SIGMA_APP_STAGE";

HostInfo::HostInfo() {
  getEnv(ENV_LABEL_SITE, site_);
  getEnv(ENV_LABEL_UNIT, unit_);
  getEnv(ENV_LABEL_APP, app_);
  getEnv(ENV_LABEL_STAGE, stage_);
}

void HostInfo::getEnv(const char* env, std::string& holder) {
  if (!strlen(env)) {
    return;
  }

  char* value = getenv(env);
  if (nullptr != value) {
    holder.clear();
    holder.append(value);
  }
}

bool HostInfo::hasHostInfo() const {
  return !unit_.empty() && !stage_.empty();
}

std::string HostInfo::queryString() const {
  if (!hasHostInfo()) {
    return std::string();
  }

  std::string query_string("labels=");
  appendLabel(query_string, "site", site_);
  appendLabel(query_string, "unit", unit_);
  appendLabel(query_string, "app", app_);
  appendLabel(query_string, "stage", stage_);
  return query_string;
}

void HostInfo::appendLabel(std::string& query_string, const char* key, const std::string& value) {
  if (value.empty()) {
    return;
  }

  if (absl::EndsWith(query_string, "=")) {
    query_string.append(key).append(":").append(value);
  } else {
    query_string.append(",").append(key).append(":").append(value);
  }
}
ROCKETMQ_NAMESPACE_END