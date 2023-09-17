/**
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

import { Status, Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import { BadRequestException } from './BadRequestException';
import { ForbiddenException } from './ForbiddenException';
import { InternalErrorException } from './InternalErrorException';
import { NotFoundException } from './NotFoundException';
import { PayloadTooLargeException } from './PayloadTooLargeException';
import { PaymentRequiredException } from './PaymentRequiredException';
import { ProxyTimeoutException } from './ProxyTimeoutException';
import { RequestHeaderFieldsTooLargeException } from './RequestHeaderFieldsTooLargeException';
import { TooManyRequestsException } from './TooManyRequestsException';
import { UnauthorizedException } from './UnauthorizedException';
import { UnsupportedException } from './UnsupportedException';

export class StatusChecker {
  static check(status?: Status.AsObject, requestId?: string) {
    if (!status) return;
    switch (status.code) {
      case Code.OK:
      case Code.MULTIPLE_RESULTS:
        return;
      case Code.BAD_REQUEST:
      case Code.ILLEGAL_ACCESS_POINT:
      case Code.ILLEGAL_TOPIC:
      case Code.ILLEGAL_CONSUMER_GROUP:
      case Code.ILLEGAL_MESSAGE_TAG:
      case Code.ILLEGAL_MESSAGE_KEY:
      case Code.ILLEGAL_MESSAGE_GROUP:
      case Code.ILLEGAL_MESSAGE_PROPERTY_KEY:
      case Code.INVALID_TRANSACTION_ID:
      case Code.ILLEGAL_MESSAGE_ID:
      case Code.ILLEGAL_FILTER_EXPRESSION:
      case Code.ILLEGAL_INVISIBLE_TIME:
      case Code.ILLEGAL_DELIVERY_TIME:
      case Code.INVALID_RECEIPT_HANDLE:
      case Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE:
      case Code.UNRECOGNIZED_CLIENT_TYPE:
      case Code.MESSAGE_CORRUPTED:
      case Code.CLIENT_ID_REQUIRED:
      case Code.ILLEGAL_POLLING_TIME:
        throw new BadRequestException(status.code, status.message, requestId);
      case Code.UNAUTHORIZED:
        throw new UnauthorizedException(status.code, status.message, requestId);
      case Code.PAYMENT_REQUIRED:
        throw new PaymentRequiredException(status.code, status.message, requestId);
      case Code.FORBIDDEN:
        throw new ForbiddenException(status.code, status.message, requestId);
      case Code.MESSAGE_NOT_FOUND:
        return;
      case Code.NOT_FOUND:
      case Code.TOPIC_NOT_FOUND:
      case Code.CONSUMER_GROUP_NOT_FOUND:
        throw new NotFoundException(status.code, status.message, requestId);
      case Code.PAYLOAD_TOO_LARGE:
      case Code.MESSAGE_BODY_TOO_LARGE:
        throw new PayloadTooLargeException(status.code, status.message, requestId);
      case Code.TOO_MANY_REQUESTS:
        throw new TooManyRequestsException(status.code, status.message, requestId);
      case Code.REQUEST_HEADER_FIELDS_TOO_LARGE:
      case Code.MESSAGE_PROPERTIES_TOO_LARGE:
        throw new RequestHeaderFieldsTooLargeException(status.code, status.message, requestId);
      case Code.INTERNAL_ERROR:
      case Code.INTERNAL_SERVER_ERROR:
      case Code.HA_NOT_AVAILABLE:
        throw new InternalErrorException(status.code, status.message, requestId);
      case Code.PROXY_TIMEOUT:
      case Code.MASTER_PERSISTENCE_TIMEOUT:
      case Code.SLAVE_PERSISTENCE_TIMEOUT:
        throw new ProxyTimeoutException(status.code, status.message, requestId);
      case Code.UNSUPPORTED:
      case Code.VERSION_UNSUPPORTED:
      case Code.VERIFY_FIFO_MESSAGE_UNSUPPORTED:
        throw new UnsupportedException(status.code, status.message, requestId);
      default:
        throw new UnsupportedException(status.code, status.message, requestId);
    }
  }
}
