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

#include "RocketMQ.h"

#include <cstdint>
#include <system_error>
#include <type_traits>

ROCKETMQ_NAMESPACE_BEGIN

enum class ErrorCode : int {
  Success = 0,

  /**
   * @brief Client state not as expected. Call Producer#start() first.
   *
   */
  IllegalState = 10000,

  /**
   * @brief To publish FIFO messages, only synchronous API is supported.
   */
  BadRequestAsyncPubFifoMessage = 10100,

  /**
   * @brief 102XX is used for client side error.
   *
   */
  InternalClientError = 10200,

  /**
   * @brief Broker has processed the request but is not going to return any content.
   */
  NoContent = 20400,

  /**
   * @brief Bad configuration. For example, negative max-attempt-times.
   *
   */
  BadConfiguration = 30000,

  /**
   * @brief Generic code, representing multiple return results.
   *
   */
  MultipleResults = 30100,

  /**
   * @brief The server cannot process the request due to apparent client-side
   * error. For example, topic contains invalid character or is excessively
   * long.
   *
   */
  BadRequest = 40000,

  /**
   * @brief The access point is illegal
   *
   */
  IllegalAccessPoint = 40001,

  IllegalTopic = 40002,
  IllegalConsumerGroup = 40003,
  IllegalMessageTag = 40004,
  IllegalMessageKey = 40005,
  IllegalMessageGroup = 40006,
  IllegalMessageProperty = 40007,
  InvalidTransactionId = 40008,
  IllegalMessageId = 40009,
  IllegalFilterExpression = 40010,
  InvalidReceiptHandle = 40011,

  /**
   * @brief Message property conflicts with its type.
   */
  MessagePropertyConflictWithType = 40012,

  // Client type is not recognized by server
  UnsupportedClientType = 40013,

  // Received message is corrupted, generally failing to pass integrity checksum validation.
  MessageCorrupted = 40014,

  // Request is rejected due to missing of x-mq-client-id HTTP header in the metadata frame.
  ClientIdRequired = 40015,

  /**
   * @brief Authentication failed. Possibly caused by invalid credentials.
   *
   */
  Unauthorized = 40100,

  /**
   * @brief Credentials are understood by server but authenticated user does not
   * have privilege to perform the requested action.
   *
   */
  Forbidden = 40300,

  /**
   * @brief Topic not found, which should be created through console or
   * administration API before hand.
   *
   */
  NotFound = 40400,

  MessageNotFound = 40401,

  TopicNotFound = 404002,

  ConsumerGroupNotFound = 404003,

  /**
   * @brief Timeout when connecting, reading from or writing to brokers.
   *
   */
  RequestTimeout = 40800,

  /**
   * @brief Message body is too large.
   *
   */
  PayloadTooLarge = 41300,

  /**
   * @brief Message body size exceeds limited allowed by server.
   */
  MessageBodyTooLarge = 41301,

  /**
   * @brief Message body is empty.
   */
  MessageBodyEmpty = 41302,

  /**
   * @brief When trying to perform an action whose dependent procedure state is
   * not right, this code will be used.
   * 1. Acknowledge a message that is not previously received;
   * 2. Commit/Rollback a transactional message that does not exist;
   * 3. Commit an offset which is greater than maximum of partition;
   */
  PreconditionRequired = 42800,

  /**
   * @brief Quota exhausted. The user has sent too many requests in a given
   * amount of time.
   *
   */
  TooManyRequests = 42900,

  /**
   * @brief The server is unwilling to process the request because either an
   * individual header field, or all the header fields collectively, are too
   * large
   *
   */
  HeaderFieldsTooLarge = 43100,

  // Message properties total size exceeds the threshold.
  MessagePropertiesTooLarge = 43101,

  /**
   * @brief A server operator has received a legal demand to deny access to a
   * resource or to a set of resources that includes the requested resource.
   *
   */
  UnavailableForLegalReasons = 45100,

  /**
   * @brief Server side interval error
   *
   */
  InternalServerError = 50000,

  /**
   * @brief The server either does not recognize the request method, or it lacks
   * the ability to fulfil the request.
   *
   */
  NotImplemented = 50100,

  /**
   * @brief The server was acting as a gateway or proxy and received an invalid
   * response from the upstream server.
   *
   */
  BadGateway = 50200,

  /**
   * @brief The server cannot handle the request (because it is overloaded or
   * down for maintenance). Generally, this is a temporary state.
   *
   */
  ServiceUnavailable = 50300,

  /**
   * @brief The server was acting as a gateway or proxy and did not receive a
   * timely response from the upstream server.
   *
   */
  GatewayTimeout = 50400,

  /**
   * @brief The server does not support the protocol version used in the
   * request.
   *
   */
  NotSupported = 50500,

  ProtocolUnsupported = 50501,

  VerifyFifoMessageUnsupported = 50502,

  /**
   * @brief The server is unable to store the representation needed to complete
   * the request.
   *
   */
  InsufficientStorage = 50700,
};

std::error_code make_error_code(ErrorCode code);

ROCKETMQ_NAMESPACE_END

namespace std {
template <>
struct is_error_code_enum<ROCKETMQ_NAMESPACE::ErrorCode> : true_type {};
} // namespace std