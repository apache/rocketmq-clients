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
#include "RemotingCommand.h"

#include <atomic>
#include <cstdint>
#include <memory>

#include "LanguageCode.h"
#include "absl/memory/memory.h"

#include "QueryRouteRequestHeader.h"

ROCKETMQ_NAMESPACE_BEGIN

std::int32_t RemotingCommand::nextRequestId() {
  static std::atomic_int32_t request_id{0};
  return request_id.fetch_add(1, std::memory_order_relaxed);
}

RemotingCommand RemotingCommand::createRequest(RequestCode code, CommandCustomHeader* ext_fields) {
  RemotingCommand command;
  command.code_ = static_cast<std::int32_t>(code);
  command.ext_fields_ = ext_fields;
  return command;
}

RemotingCommand RemotingCommand::createResponse(ResponseCode code, CommandCustomHeader* ext_fields) {
  RemotingCommand response;
  response.code_ = static_cast<std::int32_t>(code);
  response.ext_fields_ = ext_fields;
  response.flag_ |= (1 << RPC_TYPE_RESPONSE);
  return response;
}

void RemotingCommand::encodeHeader(google::protobuf::Value& root) {
  auto fields = root.mutable_struct_value()->mutable_fields();

  google::protobuf::Value code;
  code.set_number_value(code_);
  fields->insert({"code", code});

  google::protobuf::Value language;
  switch (language_) {
    case LanguageCode::CPP: {
      language.set_string_value("CPP");
      break;
    }
    case LanguageCode::JAVA: {
      language.set_string_value("JAVA");
      break;
    }
    case LanguageCode::GO: {
      language.set_string_value("GO");
      break;
    }
    case LanguageCode::DOTNET: {
      language.set_string_value("DOTNET");
      break;
    }
    default: {
      language.set_string_value("OTHER");
      break;
    }
  }
  fields->insert({"language", language});

  if (version_) {
    google::protobuf::Value version;
    version.set_number_value(version_);
    fields->insert({"version", version});
  }

  google::protobuf::Value opaque;
  opaque.set_number_value(opaque_);
  fields->insert({"opaque", opaque});

  google::protobuf::Value flag;
  flag.set_number_value(flag_);
  fields->insert({"flag", flag});

  if (!remark_.empty()) {
    google::protobuf::Value remark;
    remark.set_string_value(remark_);
    fields->insert({"remark", remark});
  }

  if (ext_fields_) {
    google::protobuf::Value ext_fields;
    ext_fields_->encode(ext_fields);
    fields->insert({"extFields", ext_fields});
  }
}

const std::uint8_t RemotingCommand::RPC_TYPE_RESPONSE = 0;

const std::uint8_t RemotingCommand::RPC_TYPE_ONE_WAY = 1;

ROCKETMQ_NAMESPACE_END