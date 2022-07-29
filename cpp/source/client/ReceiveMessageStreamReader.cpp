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

#include "ReceiveMessageStreamReader.h"

#include <chrono>

#include "apache/rocketmq/v2/definition.pb.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

ReceiveMessageStreamReader::ReceiveMessageStreamReader(std::weak_ptr<ClientManager> client_manager,
                                                       rmq::MessagingService::Stub* stub,
                                                       std::string peer_address,
                                                       rmq::ReceiveMessageRequest request,
                                                       std::unique_ptr<ReceiveMessageContext> context)
    : client_manager_(std::move(client_manager)),
      stub_(stub),
      peer_address_(std::move(peer_address)),
      request_(std::move(request)),
      context_(std::move(context)) {
  for (const auto& entry : context_->metadata) {
    client_context_.AddMetadata(entry.first, entry.second);
  }
  client_context_.set_deadline(std::chrono::system_clock::now() + context_->timeout);

  stub_->async()->ReceiveMessage(&client_context_, &request_, this);
  result_.source_host = peer_address_;
  StartCall();
  StartRead(&response_);
}

void ReceiveMessageStreamReader::OnReadDone(bool ok) {
  if (ok) {
    SPDLOG_DEBUG("ReceiveMessageStreamReader#OnReadDone: ok={}", ok);
  } else {
    if (result_.messages.empty() && !ec_) {
      SPDLOG_WARN("ReceiveMessageStreamReader#OnReadDone: ok={}", ok);
      ec_ = ErrorCode::BadGateway;
    } else {
      SPDLOG_DEBUG("ReceiveMessageStreamReader#OnReadDone reached end-of-stream");
    }
    return;
  }

  SPDLOG_DEBUG("ReceiveMessageStreamReader#OnReadDone: response={}", response_.DebugString());
  switch (response_.content_case()) {
    case rmq::ReceiveMessageResponse::ContentCase::kStatus: {
      SPDLOG_DEBUG("ReceiveMessageResponse.status.message={}", response_.status().message());
      switch (response_.status().code()) {
        case rmq::Code::OK: {
          break;
        }
        case rmq::Code::ILLEGAL_TOPIC: {
          ec_ = ErrorCode::IllegalTopic;
          break;
        }

        case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
          ec_ = ErrorCode::IllegalConsumerGroup;
          break;
        }

        case rmq::Code::ILLEGAL_FILTER_EXPRESSION: {
          ec_ = ErrorCode::IllegalFilterExpression;
          break;
        }

        case rmq::Code::CLIENT_ID_REQUIRED: {
          ec_ = ErrorCode::InternalClientError;
          break;
        }

        case rmq::Code::TOPIC_NOT_FOUND: {
          ec_ = ErrorCode::TopicNotFound;
          break;
        }

        case rmq::Code::CONSUMER_GROUP_NOT_FOUND: {
          ec_ = ErrorCode::ConsumerGroupNotFound;
          break;
        }

        case rmq::Code::TOO_MANY_REQUESTS: {
          ec_ = ErrorCode::TooManyRequests;
          break;
        }

        case rmq::Code::MESSAGE_NOT_FOUND: {
          ec_ = ErrorCode::NoContent;
          break;
        }

        case rmq::Code::UNAUTHORIZED: {
          ec_ = ErrorCode::Unauthorized;
          break;
        }

        case rmq::Code::FORBIDDEN: {
          ec_ = ErrorCode::Forbidden;
          break;
        }

        case rmq::Code::INTERNAL_SERVER_ERROR: {
          ec_ = ErrorCode::InternalServerError;
          break;
        }

        case rmq::Code::PROXY_TIMEOUT: {
          ec_ = ErrorCode::GatewayTimeout;
          break;
        }

        default: {
          ec_ = ErrorCode::NotSupported;
          SPDLOG_WARN("Unsupported code={}", response_.status().code());
          break;
        }
      }
      break;
    }
    case rmq::ReceiveMessageResponse::ContentCase::kMessage: {
      auto client_manager = client_manager_.lock();
      auto message = client_manager->wrapMessage(response_.message());
      auto raw = const_cast<Message*>(message.get());
      raw->mutableExtension().target_endpoint = peer_address_;
      if (message) {
        result_.messages.push_back(message);
      }
      break;
    }
    default:
      break;
  }

  StartRead(&response_);
}

void ReceiveMessageStreamReader::OnDone(const grpc::Status& s) {
  if (!s.ok()) {
    SPDLOG_WARN("ReceiveMessageStreamReader#OnDone: status.ok={}, status.error_message={}", s.ok(), s.error_message());
  } else {
    SPDLOG_DEBUG("ReceiveMessageStreamReader#OnDone: status.ok={}", s.ok());
  }

  status_ = s;
  context_->callback(ec_, result_);
  delete this;
}

ROCKETMQ_NAMESPACE_END