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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

#include "HttpClient.h"
#include "rocketmq/CredentialsProvider.h"

ROCKETMQ_NAMESPACE_BEGIN

class StsCredentialsProviderImpl : public CredentialsProvider {
public:
  explicit StsCredentialsProviderImpl(std::string ram_role_name);

  ~StsCredentialsProviderImpl() override;

  Credentials getCredentials() override;

  void withHttpClient(std::unique_ptr<HttpClient> http_client) {
    http_client_ = std::move(http_client);
  }

private:
  static const char* RAM_ROLE_HOST;
  static const char* RAM_ROLE_URL_PREFIX;
  static const char* FIELD_ACCESS_KEY;
  static const char* FIELD_ACCESS_SECRET;
  static const char* FIELD_SESSION_TOKEN;
  static const char* FIELD_EXPIRATION;
  static const char* EXPIRATION_DATE_TIME_FORMAT;

  std::string ram_role_name_;

  std::string access_key_ GUARDED_BY(mtx_);
  std::string access_secret_ GUARDED_BY(mtx_);
  std::string session_token_ GUARDED_BY(mtx_);
  std::chrono::system_clock::time_point expiration_;

  absl::Mutex mtx_;
  void refresh() LOCKS_EXCLUDED(mtx_);

  std::unique_ptr<HttpClient> http_client_;
};

ROCKETMQ_NAMESPACE_END