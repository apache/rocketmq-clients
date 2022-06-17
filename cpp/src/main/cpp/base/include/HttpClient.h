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

#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

enum class HttpProtocol : int8_t
{
  HTTP = 1,
  HTTPS = 2,
};

enum class HttpStatus : int
{
  OK = 200,
  INTERNAL = 500,
};

class HttpClient {
public:
  virtual ~HttpClient() = default;

  virtual void start() = 0;

  virtual void shutdown() = 0;

  virtual void
  get(HttpProtocol protocol, const std::string& host, std::uint16_t port, const std::string& path,
      const std::function<void(int, const std::multimap<std::string, std::string>&, const std::string&)>& cb) = 0;
};

ROCKETMQ_NAMESPACE_END