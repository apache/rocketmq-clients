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
#include "HttpClientImpl.h"

#include <memory>
#include <string>

#include "fmt/format.h"
#include "spdlog/spdlog.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

void HttpClientImpl::start() {
}

void HttpClientImpl::shutdown() {
}

/**
 * @brief We current implement this function in sync mode since async http request in CURL is sort of unnecessarily
 * complex.
 *
 * @param protocol
 * @param host
 * @param port
 * @param path
 * @param cb
 */
void HttpClientImpl::get(
    HttpProtocol protocol, const std::string& host, std::uint16_t port, const std::string& path,
    const std::function<void(int, const std::multimap<std::string, std::string>&, const std::string&)>& cb) {

  std::string key;
  switch (protocol) {
    case HttpProtocol::HTTP:
      key = fmt::format("http://{}:{}", host, port);
      break;
    case HttpProtocol::HTTPS:
      key = fmt::format("https://{}:{}", host, port);
      break;
  }

  std::shared_ptr<httplib::Client> client;
  {
    absl::MutexLock lk(&clients_mtx_);
    if (clients_.contains(key)) {
      client = clients_[key];
    }

    if (!client || !client->is_valid()) {
      client = std::make_shared<httplib::Client>(key);
      clients_.insert_or_assign(key, client);
    }
  }

  if (!client || !client->is_valid()) {
    int code = 400;
    std::multimap<std::string, std::string> headers;
    std::string response;
    cb(code, headers, response);
    return;
  }

  auto res = client->Get(path.c_str());

  std::multimap<std::string, std::string> headers;
  for (auto& header : headers) {
    headers.insert({header.first, header.second});
  }

  cb(res->status, headers, res->body);
}

ROCKETMQ_NAMESPACE_END