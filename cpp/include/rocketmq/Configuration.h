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

#include <chrono>
#include <memory>
#include <string>

#include "ConfigurationDefaults.h"
#include "CredentialsProvider.h"

ROCKETMQ_NAMESPACE_BEGIN

class ConfigurationBuilder;

class Configuration {
public:
  static ConfigurationBuilder newBuilder();

  const std::string& endpoints() const {
    return endpoints_;
  }

  const std::string& resourceNamespace() const {
    return resource_namespace_;
  }

  CredentialsProviderPtr credentialsProvider() const {
    return credentials_provider_;
  }

  std::chrono::milliseconds requestTimeout() const {
    return request_timeout_;
  }

  bool withSsl() const {
    return tls_;
  }

protected:
  friend class ConfigurationBuilder;

  Configuration() = default;

private:
  std::string               endpoints_;
  std::string               resource_namespace_;
  CredentialsProviderPtr    credentials_provider_;
  std::chrono::milliseconds request_timeout_{ConfigurationDefaults::RequestTimeout};
  bool tls_ = true;
};

class ConfigurationBuilder {
public:
  ConfigurationBuilder& withEndpoints(std::string endpoints);

  ConfigurationBuilder& withNamespace(std::string resource_namespace);

  ConfigurationBuilder& withCredentialsProvider(std::shared_ptr<CredentialsProvider> provider);

  ConfigurationBuilder& withRequestTimeout(std::chrono::milliseconds request_timeout);

  ConfigurationBuilder& withSsl(bool with_ssl);

  Configuration build();

private:
  Configuration configuration_;
};

ROCKETMQ_NAMESPACE_END