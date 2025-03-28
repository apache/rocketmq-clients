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

#include "rocketmq/Configuration.h"

ROCKETMQ_NAMESPACE_BEGIN

ConfigurationBuilder Configuration::newBuilder() {
  return {};
}

ConfigurationBuilder& ConfigurationBuilder::withEndpoints(std::string endpoints) {
  configuration_.endpoints_ = std::move(endpoints);
  return *this;
}

ConfigurationBuilder& ConfigurationBuilder::withNamespace(std::string resource_namespace) {
  configuration_.resource_namespace_ = std::move(resource_namespace);
  return *this;
}

ConfigurationBuilder& ConfigurationBuilder::withCredentialsProvider(std::shared_ptr<CredentialsProvider> provider) {
  configuration_.credentials_provider_ = std::move(provider);
  return *this;
}

ConfigurationBuilder& ConfigurationBuilder::withRequestTimeout(std::chrono::milliseconds request_timeout) {
  configuration_.request_timeout_ = request_timeout;
  return *this;
}

ConfigurationBuilder& ConfigurationBuilder::withSsl(bool with_ssl) {
  configuration_.tls_ = with_ssl;
  return *this;
}

Configuration ConfigurationBuilder::build() {
  return std::move(configuration_);
}

ROCKETMQ_NAMESPACE_END