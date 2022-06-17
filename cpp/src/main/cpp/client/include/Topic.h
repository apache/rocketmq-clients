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
#pragma once

#include <string>
#include <unordered_map>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class Topic {
public:
  Topic(std::string resource_namespace, std::string name)
      : resource_namespace_(std::move(resource_namespace)), name_(std::move(name)) {
  }

  const std::string& resourceNamespace() const {
    return resource_namespace_;
  }

  const std::string& name() const {
    return name_;
  }

  bool operator==(const Topic& other) const {
    return resource_namespace_ == other.resource_namespace_ && name_ == other.name_;
  }

  bool operator<(const Topic& other) const {
    if (resource_namespace_ < other.resource_namespace_) {
      return true;
    } else if (resource_namespace_ > other.resource_namespace_) {
      return false;
    }

    if (name_ < other.name_) {
      return true;
    } else if (name_ > other.name_) {
      return false;
    }
    return false;
  }

private:
  std::string resource_namespace_;
  std::string name_;
};

ROCKETMQ_NAMESPACE_END