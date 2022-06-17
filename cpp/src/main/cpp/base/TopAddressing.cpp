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
#include "TopAddressing.h"

#include "HttpClientImpl.h"
#include "absl/memory/memory.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "spdlog/spdlog.h"
#include <utility>

ROCKETMQ_NAMESPACE_BEGIN

TopAddressing::TopAddressing() : TopAddressing("jmenv.tbsite.net", 8080, "/rocketmq/nsaddr") {
}

TopAddressing::TopAddressing(std::string host, int port, std::string path)
    : host_(std::move(host)), port_(port), path_(std::move(path)), http_client_(absl::make_unique<HttpClientImpl>()) {
  http_client_->start();
}

TopAddressing::~TopAddressing() {
  http_client_->shutdown();
}

void TopAddressing::fetchNameServerAddresses(const std::function<void(bool, const std::vector<std::string>&)>& cb) {
  SPDLOG_DEBUG("Prepare to send HTTP request, timeout=3s");
  std::string base(fmt::format("http://{}:{}", host_, port_));
  // Append host info if necessary.
  std::string query_string(path_);

  if (absl::StrContains(query_string, "?")) {
    query_string.append("&");
  } else {
    query_string.append("?");
  }

  if (host_info_.hasHostInfo()) {
    query_string.append(host_info_.queryString());
  } else {
    query_string.append("nofix=1");
  }

  auto callback = [cb](int code, const std::multimap<std::string, std::string>& metadata, const std::string& body) {
    SPDLOG_DEBUG("Receive HTTP response. Code: {}, body: {}", code, body);
    if (static_cast<int>(HttpStatus::OK) == code) {
      cb(true, absl::StrSplit(body, ';'));
    } else {
      std::vector<std::string> name_server_list;
      cb(false, name_server_list);
    }
  };

  http_client_->get(HttpProtocol::HTTP, host_, port_, query_string, callback);
}

void TopAddressing::injectHttpClient(std::unique_ptr<HttpClient> http_client) {
  http_client_->shutdown();
  http_client_.swap(http_client);
}

ROCKETMQ_NAMESPACE_END