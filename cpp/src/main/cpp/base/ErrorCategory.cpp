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
#include "rocketmq/ErrorCategory.h"

#include "rocketmq/ErrorCode.h"

ROCKETMQ_NAMESPACE_BEGIN

std::string ErrorCategory::message(int code) const {
  ErrorCode ec = static_cast<ErrorCode>(code);
  switch (ec) {
    case ErrorCode::Success:
      return "Success";

    case ErrorCode::NoContent:
      return "Broker has processed the request but is not going to return any content.";

    case ErrorCode::IllegalState:
      return "Client state illegal. Forgot to call start()?";

    case ErrorCode::BadConfiguration:
      return "Bad configuration.";

    case ErrorCode::BadRequest:
      return "Message is ill-formed. Check validity of your topic, tag, "
             "etc";

    case ErrorCode::BadRequestAsyncPubFifoMessage:
      return "Publishing of FIFO messages is only allowed synchronously";

    case ErrorCode::Unauthorized:
      return "Authentication failed. Possibly caused by invalid credentials.";

    case ErrorCode::Forbidden:
      return "Authenticated user does not have privilege to perform the "
             "requested action";

    case ErrorCode::NotFound:
      return "Request resource not found, which should be created through console or "
             "administration API before hand.";
    case ErrorCode::TopicNotFound:
      return "Topic is not found. Verify the request topic has already been created through console or management API";

    case ErrorCode::ConsumerGroupNotFound:
      return "Group is not found. Verify the request group has already been created through console or management API";

    case ErrorCode::RequestTimeout:
      return "Timeout when connecting, reading from or writing to brokers.";

    case ErrorCode::PayloadTooLarge:
      return "Message body is too large.";

    case ErrorCode::PreconditionRequired:
      return "State of dependent procedure is not right";

    case ErrorCode::TooManyRequests:
      return "Quota exhausted. The user has sent too many requests in a given "
             "amount of time.";

    case ErrorCode::UnavailableForLegalReasons:
      return "A server operator has received a legal demand to deny access to "
             "a resource or to a set of resources that "
             "includes the requested resource.";

    case ErrorCode::HeaderFieldsTooLarge:
      return "The server is unwilling to process the request because either an "
             "individual header field, or all the header fields collectively, "
             "are too large";

    case ErrorCode::InternalServerError:
      return "Server side interval error";

    case ErrorCode::NotImplemented:
      return "The server either does not recognize the request method, or it "
             "lacks the ability to fulfil the request.";

    case ErrorCode::BadGateway:
      return "The server was acting as a gateway or proxy and received an "
             "invalid response from the upstream server.";

    case ErrorCode::ServiceUnavailable:
      return "The server cannot handle the request (because it is overloaded "
             "or down for maintenance). Generally, this "
             "is a temporary state.";

    case ErrorCode::GatewayTimeout:
      return "The server was acting as a gateway or proxy and did not receive "
             "a timely response from the upstream "
             "server.";

    case ErrorCode::ProtocolUnsupported:
      return "The server does not support the protocol version used in the "
             "request.";

    case ErrorCode::InsufficientStorage:
      return "The server is unable to store the representation needed to "
             "complete the request.";

    case ErrorCode::MultipleResults:
      return "Multiple results are available";

    case ErrorCode::IllegalAccessPoint:
      return "Access point is either malformed or invalid";

    case ErrorCode::IllegalTopic: {
      return "Topic is illegal either due to length is too long or invalid character is included";
    }

    case ErrorCode::IllegalConsumerGroup: {
      return "ConsumerGroup is illegal due to its length is too long or invalid character is included";
    }

    case ErrorCode::IllegalMessageTag: {
      return "Format of message tag is illegal.";
    }
    case ErrorCode::InternalClientError: {
      return "Internal client error";
    }
    case ErrorCode::IllegalMessageKey: {
      return "Message key is not legal";
    }
    case ErrorCode::IllegalMessageProperty: {
      return "One or multiple message properties is not legal";
    }
    case ErrorCode::IllegalMessageGroup: {
      return "Message group is invalid";
    }
    case ErrorCode::InvalidTransactionId: {
      return "Transaction ID is invalid";
    }
    case ErrorCode::IllegalMessageId: {
      return "Message ID is invalid";
    }
    case ErrorCode::IllegalFilterExpression: {
      return "Filter expression is malformed";
    }
    case ErrorCode::InvalidReceiptHandle: {
      return "Receipt handle is invalid";
    }
    case ErrorCode::MessagePropertyConflictWithType: {
      return "Message property conflicts with message type";
    }
    case ErrorCode::UnsupportedClientType: {
      return "Server does not support the client type claimed";
    }
    case ErrorCode::MessageCorrupted: {
      return "Message is corrupted";
    }
    case ErrorCode::ClientIdRequired: {
      return "x-mq-client-id header meta-data is required";
    }
    case ErrorCode::MessageNotFound: {
      return "No new messages are available at the moment";
    }
    case ErrorCode::MessageBodyTooLarge: {
      return "Size of message body exceeds limits";
    }
    case ErrorCode::MessagePropertiesTooLarge: {
      return "Size of message properties exceeds limits";
    }
    case ErrorCode::NotSupported: {
      return "Action is not supported";
    }
    case ErrorCode::VerifyFifoMessageUnsupported: {
      return "Verify FIFO message is not supported";
    }
  }

  return "";
}

ROCKETMQ_NAMESPACE_END