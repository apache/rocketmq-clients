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
  IllegalState = 1,

  /**
   * @brief Broker has processed the request but is not going to return any content.
   */
  NoContent = 204,

  /**
   * @brief Bad configuration. For example, negative max-attempt-times.
   *
   */
  BadConfiguration = 300,

  /**
   * @brief The server cannot process the request due to apprent client-side
   * error. For example, topic contains invalid character or is excessively
   * long.
   *
   */
  BadRequest = 400,

  /**
   * @brief To publish FIFO messages, only synchronous API is supported.
   */
  BadRequestAsyncPubFifoMessage = 40001,

  /**
   * @brief Authentication failed. Possibly caused by invalid credentials.
   *
   */
  Unauthorized = 401,

  /**
   * @brief Credentials are understood by server but authenticated user does not
   * have privilege to perform the requested action.
   *
   */
  Forbidden = 403,

  /**
   * @brief Topic not found, which should be created through console or
   * administration API before hand.
   *
   */
  NotFound = 404,

  TopicNotFound = 404001,

  GroupNotFound = 404002,

  /**
   * @brief Timeout when connecting, reading from or writing to brokers.
   *
   */
  RequestTimeout = 408,

  /**
   * @brief Message body is too large.
   *
   */
  PayloadTooLarge = 413,

  /**
   * @brief When trying to perform an action whose dependent procedure state is
   * not right, this code will be used.
   * 1. Acknowledge a message that is not previously received;
   * 2. Commit/Rollback a transactional message that does not exist;
   * 3. Commit an offset which is greater than maximum of partition;
   */
  PreconditionRequired = 428,

  /**
   * @brief Quota exchausted. The user has sent too many requests in a given
   * amount of time.
   *
   */
  TooManyRequest = 429,

  /**
   * @brief The server is unwilling to process the request because either an
   * individual header field, or all the header fields collectively, are too
   * large
   *
   */
  HeaderFieldsTooLarge = 431,

  /**
   * @brief A server operator has received a legal demand to deny access to a
   * resource or to a set of resources that includes the requested resource.
   *
   */
  UnavailableForLegalReasons = 451,

  /**
   * @brief Server side interval error
   *
   */
  InternalServerError = 500,

  /**
   * @brief The server either does not recognize the request method, or it lacks
   * the ability to fulfil the request.
   *
   */
  NotImplemented = 501,

  /**
   * @brief The server was acting as a gateway or proxy and received an invalid
   * response from the upstream server.
   *
   */
  BadGateway = 502,

  /**
   * @brief The server cannot handle the request (because it is overloaded or
   * down for maintenance). Generally, this is a temporary state.
   *
   */
  ServiceUnavailable = 503,

  /**
   * @brief The server was acting as a gateway or proxy and did not receive a
   * timely response from the upstream server.
   *
   */
  GatewayTimeout = 504,

  /**
   * @brief The server does not support the protocol version used in the
   * request.
   *
   */
  ProtocolVersionNotSupported = 505,

  /**
   * @brief The server is unable to store the representation needed to complete
   * the request.
   *
   */
  InsufficientStorage = 507,
};

std::error_code make_error_code(ErrorCode code);

ROCKETMQ_NAMESPACE_END

namespace std {
template <>
struct is_error_code_enum<ROCKETMQ_NAMESPACE::ErrorCode> : true_type {};
} // namespace std