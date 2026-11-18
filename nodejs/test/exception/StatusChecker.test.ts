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

import { strict as assert } from 'node:assert';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import * as exceptions from '../../src/exception';
import { checkReceiveStatus } from '../../src/exception/receiveStatus';
import { buildStatus } from '../support/protobuf';

// 每组与同基点 Java StatusChecker 的映射对应，未映射枚举显式进入 default。
const groups = [
  [ exceptions.BadRequestException, [
    Code.BAD_REQUEST, Code.ILLEGAL_ACCESS_POINT, Code.ILLEGAL_TOPIC, Code.ILLEGAL_CONSUMER_GROUP,
    Code.ILLEGAL_MESSAGE_TAG, Code.ILLEGAL_MESSAGE_KEY, Code.ILLEGAL_MESSAGE_GROUP,
    Code.ILLEGAL_MESSAGE_PROPERTY_KEY, Code.ILLEGAL_LITE_TOPIC, Code.INVALID_TRANSACTION_ID,
    Code.ILLEGAL_MESSAGE_ID, Code.ILLEGAL_FILTER_EXPRESSION, Code.ILLEGAL_INVISIBLE_TIME,
    Code.ILLEGAL_DELIVERY_TIME, Code.INVALID_RECEIPT_HANDLE, Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
    Code.UNRECOGNIZED_CLIENT_TYPE, Code.MESSAGE_CORRUPTED, Code.CLIENT_ID_REQUIRED, Code.ILLEGAL_POLLING_TIME,
  ]],
  [ exceptions.UnauthorizedException, [ Code.UNAUTHORIZED ]],
  [ exceptions.PaymentRequiredException, [ Code.PAYMENT_REQUIRED ]],
  [ exceptions.ForbiddenException, [ Code.FORBIDDEN ]],
  [ exceptions.NotFoundException, [ Code.MESSAGE_NOT_FOUND, Code.NOT_FOUND, Code.TOPIC_NOT_FOUND, Code.CONSUMER_GROUP_NOT_FOUND ]],
  [ exceptions.PayloadTooLargeException, [ Code.PAYLOAD_TOO_LARGE, Code.MESSAGE_BODY_TOO_LARGE ]],
  [ exceptions.PayloadEmptyException, [ Code.MESSAGE_BODY_EMPTY ]],
  [ exceptions.TooManyRequestsException, [ Code.TOO_MANY_REQUESTS ]],
  [ exceptions.RequestHeaderFieldsTooLargeException, [ Code.REQUEST_HEADER_FIELDS_TOO_LARGE, Code.MESSAGE_PROPERTIES_TOO_LARGE ]],
  [ exceptions.InternalErrorException, [ Code.INTERNAL_ERROR, Code.INTERNAL_SERVER_ERROR, Code.HA_NOT_AVAILABLE ]],
  [ exceptions.ProxyTimeoutException, [ Code.PROXY_TIMEOUT, Code.MASTER_PERSISTENCE_TIMEOUT, Code.SLAVE_PERSISTENCE_TIMEOUT ]],
  [ exceptions.UnsupportedException, [
    Code.UNSUPPORTED, Code.VERSION_UNSUPPORTED, Code.VERIFY_FIFO_MESSAGE_UNSUPPORTED,
    Code.CODE_UNSPECIFIED, Code.ILLEGAL_OFFSET, Code.OFFSET_NOT_FOUND, Code.REQUEST_TIMEOUT,
    Code.PRECONDITION_FAILED, Code.NOT_IMPLEMENTED, Code.FAILED_TO_CONSUME_MESSAGE,
  ]],
  [ exceptions.LiteTopicQuotaExceededException, [ Code.LITE_TOPIC_QUOTA_EXCEEDED ]],
  [ exceptions.LiteSubscriptionQuotaExceededException, [ Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED ]],
] as const;

function verifyException(error: unknown, name: string, code: number, requestId = 'request-42', message = 'server message') {
  assert(error instanceof exceptions.ClientException);
  assert(error instanceof Error);
  assert.equal(error.name, name);
  assert.equal(error.code, code);
  assert.equal(error.message, `[request-id=${requestId}, response-code=${code}] ${message}`);
  return true;
}

describe('StatusChecker 与异常兼容性', () => {
  it('枚举映射表覆盖 proto 的每个 Code', () => {
    const covered = [ Code.OK, Code.MULTIPLE_RESULTS, ...groups.flatMap(([ , codes ]) => [ ...codes ]) ];
    assert.equal(new Set(covered).size, covered.length);
    assert.deepEqual(covered.sort((a, b) => a - b), Object.values(Code).filter(v => typeof v === 'number').sort((a, b) => a - b));
  });

  for (const [ Exception, codes ] of groups) {
    for (const code of codes) {
      it(`状态 ${code} 映射为 ${Exception.name} 并保留 requestId`, () => {
        assert.throws(() => exceptions.StatusChecker.check(buildStatus(code, 'server message').toObject(), 'request-42'), error => {
          assert(error instanceof Exception);
          return verifyException(error, Exception.name, code);
        });
      });
    }
  }

  for (const code of [ Code.OK, Code.MULTIPLE_RESULTS ]) {
    it(`状态 ${code} 在两种检查方式下均成功`, () => {
      exceptions.StatusChecker.check(buildStatus(code).toObject());
      checkReceiveStatus(buildStatus(code).toObject());
    });
  }

  it('MESSAGE_NOT_FOUND 仅 receive 成功，普通 check 抛 NotFoundException', () => {
    const status = buildStatus(Code.MESSAGE_NOT_FOUND).toObject();
    checkReceiveStatus(status, 'receive-id');
    assert.throws(() => exceptions.StatusChecker.check(status, 'ack-id'), exceptions.NotFoundException);
  });

  it('receive 的其他错误保留类型和 ID', () => {
    assert.throws(() => checkReceiveStatus(buildStatus(Code.UNAUTHORIZED, 'server message').toObject(), 'request-42'), error =>
      verifyException(error, 'UnauthorizedException', Code.UNAUTHORIZED));
  });

  for (const check of [ exceptions.StatusChecker.check, checkReceiveStatus ]) {
    it(`${check.name} 缺失 status 时抛内部错误`, () => {
      assert.throws(() => check(undefined, 'request-42'), error =>
        verifyException(error, 'InternalErrorException', Code.INTERNAL_SERVER_ERROR, 'request-42', 'status was not set by server'));
    });
  }

  it('未知 code 抛 UnsupportedException，保留原始 code', () => {
    assert.throws(() => exceptions.StatusChecker.check({ code: 99999 as Code, message: 'server message' }, 'request-42'), error =>
      verifyException(error, 'UnsupportedException', 99999));
  });

  const standardExceptions = [
    exceptions.ClientException, exceptions.BadRequestException, exceptions.ForbiddenException,
    exceptions.InternalErrorException, exceptions.LiteTopicQuotaExceededException, exceptions.NotFoundException,
    exceptions.PayloadEmptyException, exceptions.PayloadTooLargeException, exceptions.PaymentRequiredException,
    exceptions.ProxyTimeoutException, exceptions.RequestHeaderFieldsTooLargeException,
    exceptions.TooManyRequestsException, exceptions.UnauthorizedException, exceptions.UnsupportedException,
  ];
  for (const Exception of standardExceptions) {
    it(`${Exception.name} 保留现有构造参数顺序及可选 ID`, () => {
      verifyException(new Exception(40100, 'server message', 'request-42'), Exception.name, 40100);
      assert.equal(new Exception(40100, 'server message').message, '[request-id=undefined, response-code=40100] server message');
    });
  }

  it('LiteSubscriptionQuotaExceededException 保留 (code, requestId, message) 顺序', () => {
    verifyException(new exceptions.LiteSubscriptionQuotaExceededException(42902, 'request-42', 'server message'),
      'LiteSubscriptionQuotaExceededException', 42902);
    assert.equal(new exceptions.LiteSubscriptionQuotaExceededException(42902, null, '').message,
      '[request-id=undefined, response-code=42902] ');
  });

  it('InternalErrorException 的 retryable 为有类型的 true', () => {
    const error = new exceptions.InternalErrorException(50000, 'temporary');
    const retryable: true = error.retryable;
    assert.equal(retryable, true);
    assert.equal(error.isRetryable(), true);
  });
});
