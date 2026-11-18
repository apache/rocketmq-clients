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
import * as sinon from 'sinon';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import { ClientType, Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageRequest, AckMessageResponse, ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse,
  ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse,
  ReceiveMessageRequest, ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { SimpleConsumer } from '../../src/consumer/SimpleConsumer';
import { BadRequestException, ForbiddenException, InternalErrorException } from '../../src/exception';
import { GeneralMessage, MessageHookPoints, MessageHookPointsStatus, MessageInterceptorContext } from '../../src/hook';
import { MessageView } from '../../src/message';
import { MessageQueue } from '../../src/route';
import { createResource } from '../../src/util';
import { createFakeRpcClientManager, createSilentLogger, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildStatus } from '../support/protobuf';

class ConsumerHarness extends SimpleConsumer {
  testClientType = ClientType.SIMPLE_CONSUMER;

  protected getClientType() {
    return this.testClientType ?? ClientType.SIMPLE_CONSUMER;
  }

  buildRequest(mq: MessageQueue) {
    return this.wrapReceiveMessageRequest(5, mq, new FilterExpression('tag'), 15000, 30000);
  }

  receiveRpc(request: ReceiveMessageRequest, mq: MessageQueue) {
    return this.receiveMessage(request, mq, 30000);
  }

  ackRpc(message: MessageView) {
    return this.ackMessage(message);
  }

  invisibleRpc(message: MessageView) {
    return this.invisibleDuration(message, 15000);
  }
}

describe('Consumer', () => {
  let consumer: ConsumerHarness;
  let rpc: ReturnType<typeof createFakeRpcClientManager>;
  let logger: ReturnType<typeof createSilentLogger>;
  const interceptor = {
    doBefore: sinon.stub<[MessageInterceptorContext, GeneralMessage[]], void>(),
    doAfter: sinon.stub<[MessageInterceptorContext, GeneralMessage[]], void>(),
  };
  const mq = buildRoute().messageQueues[0];

  beforeEach(() => {
    interceptor.doBefore.resetHistory();
    interceptor.doAfter.resetHistory();
    logger = createSilentLogger();
    consumer = new ConsumerHarness({ endpoints: '127.0.0.1:8081', namespace: '', consumerGroup: 'group',
      requestTimeout: 3000, subscriptions: new Map([[ 'test-topic', FilterExpression.SUB_ALL ]]),
      logger, messageInterceptor: [ interceptor ] });
    rpc = installFakeRpcClientManager(consumer);
  });

  afterEach(async () => {
    await consumer.shutdown();
    sinon.restore();
  });

  function checkHook(point: MessageHookPoints, status: MessageHookPointsStatus, messages: GeneralMessage[]) {
    sinon.assert.calledOnce(interceptor.doBefore);
    sinon.assert.calledOnce(interceptor.doAfter);
    assert.strictEqual(interceptor.doBefore.firstCall.args[0].getMessageHookPoints(), point);
    assert.strictEqual(interceptor.doAfter.firstCall.args[0].getMessageHookPoints(), point);
    assert.strictEqual(interceptor.doAfter.firstCall.args[0].getStatus(), status);
    assert.deepStrictEqual(interceptor.doAfter.firstCall.args[1], messages);
    sinon.assert.callOrder(interceptor.doBefore, interceptor.doAfter);
  }

  it('receive 构造完整请求并按 requestTimeout + awaitDuration 设置 RPC 超时', async () => {
    const request = consumer.buildRequest(mq);
    assert.strictEqual(request.getGroup()?.getName(), 'group');
    assert.deepStrictEqual(request.getMessageQueue()?.toObject(), mq.toProtobuf().toObject());
    assert.strictEqual(request.getFilterExpression()?.getExpression(), 'tag');
    assert.strictEqual(request.getBatchSize(), 5);
    assert.strictEqual(request.getAutoRenew(), false);
    assert.strictEqual(request.getInvisibleDuration()?.getSeconds(), 15);
    assert.strictEqual(request.getLongPollingTimeout()?.getSeconds(), 30);
    rpc.receiveMessage.resolves([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    assert.deepStrictEqual(await consumer.receiveRpc(request, mq), []);
    sinon.assert.calledWithExactly(rpc.receiveMessage, mq.broker.endpoints, request, 33000);
    checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.OK, []);
    assert.deepStrictEqual(interceptor.doBefore.firstCall.args[1], []);
  });

  [ false, true ].forEach(timestampFirst => {
    it(`解析 STATUS、MESSAGE 和 DELIVERY_TIMESTAMP 组合（timestampFirst=${timestampFirst}）`, async () => {
      const time = new Date(1700000000123);
      const timestamp = new ReceiveMessageResponse().setDeliveryTimestamp(Timestamp.fromDate(time));
      const messages = [
        new ReceiveMessageResponse().setMessage(buildMessagePB({ messageId: 'a' })),
        new ReceiveMessageResponse().setMessage(buildMessagePB({ messageId: 'b' })),
      ];
      rpc.receiveMessage.resolves([
        ...(timestampFirst ? [ timestamp ] : []), ...messages,
        new ReceiveMessageResponse().setStatus(buildStatus()), ...(timestampFirst ? [] : [ timestamp ]),
      ]);
      const result = await consumer.receiveRpc(consumer.buildRequest(mq), mq);
      assert.deepStrictEqual(result.map(message => message.messageId), [ 'a', 'b' ]);
      assert.ok(result.every(message => message.transportDeliveryTimestamp?.getTime() === time.getTime()));
      assert.ok(result.every(message => message.endpoints === mq.broker.endpoints));
      checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.OK, result);
    });
  });

  it('MESSAGE_NOT_FOUND 返回空列表并记录 OK hook', async () => {
    rpc.receiveMessage.resolves([ new ReceiveMessageResponse().setStatus(buildStatus(Code.MESSAGE_NOT_FOUND)) ]);
    assert.deepStrictEqual(await consumer.receiveRpc(consumer.buildRequest(mq), mq), []);
    checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.OK, []);
  });

  ([
    [ Code.BAD_REQUEST, BadRequestException ], [ Code.FORBIDDEN, ForbiddenException ],
    [ Code.INTERNAL_SERVER_ERROR, InternalErrorException ],
  ] as const).forEach(([ code, exception ]) => {
    it(`receive 失败状态 ${code} 映射异常并记录一次 ERROR hook`, async () => {
      rpc.receiveMessage.resolves([ new ReceiveMessageResponse().setStatus(buildStatus(code)) ]);
      await assert.rejects(consumer.receiveRpc(consumer.buildRequest(mq), mq), exception);
      checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.ERROR, []);
    });
  });

  it('receive 传输失败保留异常对象并记录 ERROR hook', async () => {
    const error = new Error('deadline exceeded');
    rpc.receiveMessage.rejects(error);
    await assert.rejects(consumer.receiveRpc(consumer.buildRequest(mq), mq), reason => reason === error);
    checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.ERROR, []);
  });

  it('未知 receive content 发出 warn 并继续处理有效响应', async () => {
    rpc.receiveMessage.resolves([ new ReceiveMessageResponse(), new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    assert.deepStrictEqual(await consumer.receiveRpc(consumer.buildRequest(mq), mq), []);
    sinon.assert.calledOnce(logger.warn);
    assert.ok(logger.warn.firstCall.args.includes(consumer.clientId));
    checkHook(MessageHookPoints.RECEIVE, MessageHookPointsStatus.OK, []);
  });

  [ ClientType.SIMPLE_CONSUMER, ClientType.PUSH_CONSUMER,
    ClientType.LITE_SIMPLE_CONSUMER, ClientType.LITE_PUSH_CONSUMER ].forEach(type => {
    ([ 'ack', 'invisible' ] as const).forEach(operation => {
      it(`${operation} 构造完整字段并按 clientType=${type} 传递 liteTopic`, async () => {
        consumer.testClientType = type;
        const message = new MessageView(buildMessagePB({ liteTopic: 'lite' }), mq);
        const isLite = type === ClientType.LITE_SIMPLE_CONSUMER || type === ClientType.LITE_PUSH_CONSUMER;
        if (operation === 'ack') {
          rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
          await consumer.ackRpc(message);
          const [ endpoints, request, timeout ] = rpc.ackMessage.firstCall.args;
          assert.strictEqual(endpoints, message.endpoints);
          assert.strictEqual(timeout, 3000);
          assert.strictEqual(request.getGroup()?.getName(), 'group');
          assert.strictEqual(request.getTopic()?.getName(), message.topic);
          assert.strictEqual(request.getEntriesList()[0].getMessageId(), message.messageId);
          assert.strictEqual(request.getEntriesList()[0].getReceiptHandle(), message.receiptHandle);
          assert.strictEqual(request.getEntriesList()[0].getLiteTopic(), isLite ? 'lite' : '');
          checkHook(MessageHookPoints.ACK, MessageHookPointsStatus.OK, [ message ]);
        } else {
          rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse()
            .setStatus(buildStatus()).setReceiptHandle('new-handle'));
          assert.strictEqual(await consumer.invisibleRpc(message), 'new-handle');
          const [ endpoints, request, timeout ] = rpc.changeInvisibleDuration.firstCall.args;
          assert.strictEqual(endpoints, message.endpoints);
          assert.strictEqual(timeout, 3000);
          assert.strictEqual(request.getGroup()?.getName(), 'group');
          assert.strictEqual(request.getTopic()?.getName(), message.topic);
          assert.strictEqual(request.getMessageId(), message.messageId);
          assert.strictEqual(request.getReceiptHandle(), message.receiptHandle);
          assert.strictEqual(request.getInvisibleDuration()?.getSeconds(), 15);
          assert.strictEqual(request.getLiteTopic(), isLite ? 'lite' : '');
          checkHook(MessageHookPoints.CHANGE_INVISIBLE_DURATION, MessageHookPointsStatus.OK, [ message ]);
        }
        assert.deepStrictEqual(interceptor.doBefore.firstCall.args[1], [ message ]);
      });
    });
  });

  ([ 'ack', 'invisible' ] as const).forEach(operation => {
    [ false, true ].forEach(transportFailure => {
      it(`${operation} 失败时抛出异常并记录 ERROR hook（transportFailure=${transportFailure}）`, async () => {
        const message = new MessageView(buildMessagePB(), mq);
        if (operation === 'ack') {
          rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus(Code.FORBIDDEN)));
          if (transportFailure) rpc.ackMessage.rejects(new Error('transport'));
          await assert.rejects(consumer.ackRpc(message), transportFailure ? /transport/ : ForbiddenException);
          checkHook(MessageHookPoints.ACK, MessageHookPointsStatus.ERROR, [ message ]);
        } else {
          rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse().setStatus(buildStatus(Code.FORBIDDEN)));
          if (transportFailure) rpc.changeInvisibleDuration.rejects(new Error('transport'));
          await assert.rejects(consumer.invisibleRpc(message), transportFailure ? /transport/ : ForbiddenException);
          checkHook(MessageHookPoints.CHANGE_INVISIBLE_DURATION, MessageHookPointsStatus.ERROR, [ message ]);
        }
      });
    });
  });

  ([ 'ack', 'invisible', 'dlq' ] as const).forEach(operation => {
    [ Code.OK, Code.FORBIDDEN ].forEach(code => {
      it(`ViaRpc ${operation} 保留响应对象与调用参数，状态 ${code} 由调用方处理`, async () => {
        const message = new MessageView(buildMessagePB(), mq);
        let result: unknown;
        let response: unknown;
        let point: MessageHookPoints;
        if (operation === 'ack') {
          const request = new AckMessageRequest().setGroup(createResource('group')).setTopic(createResource(message.topic));
          request.addEntries().setMessageId(message.messageId).setReceiptHandle(message.receiptHandle);
          response = new AckMessageResponse().setStatus(buildStatus(code));
          rpc.ackMessage.resolves(response as AckMessageResponse);
          result = await consumer.ackMessageViaRpc(message.endpoints, request, 1234, message);
          sinon.assert.calledWithExactly(rpc.ackMessage, message.endpoints, request, 1234);
          point = MessageHookPoints.ACK;
        } else if (operation === 'invisible') {
          const request = new ChangeInvisibleDurationRequest().setMessageId(message.messageId);
          response = new ChangeInvisibleDurationResponse().setStatus(buildStatus(code));
          rpc.changeInvisibleDuration.resolves(response as ChangeInvisibleDurationResponse);
          result = await consumer.changeInvisibleDurationViaRpc(message.endpoints, request, 1234, message);
          sinon.assert.calledWithExactly(rpc.changeInvisibleDuration, message.endpoints, request, 1234);
          point = MessageHookPoints.CHANGE_INVISIBLE_DURATION;
        } else {
          const request = new ForwardMessageToDeadLetterQueueRequest().setGroup(createResource('group'))
            .setTopic(createResource(message.topic))
            .setMessageId(message.messageId)
            .setReceiptHandle(message.receiptHandle)
            .setDeliveryAttempt(3)
            .setMaxDeliveryAttempts(3);
          response = new ForwardMessageToDeadLetterQueueResponse().setStatus(buildStatus(code));
          rpc.forwardMessageToDeadLetterQueue.resolves(response as ForwardMessageToDeadLetterQueueResponse);
          result = await consumer.forwardMessageToDeadLetterQueueViaRpc(message.endpoints, request, 1234, message);
          sinon.assert.calledWithExactly(rpc.forwardMessageToDeadLetterQueue, message.endpoints, request, 1234);
          point = MessageHookPoints.FORWARD_TO_DLQ;
        }
        assert.strictEqual(result, response);
        checkHook(point, code === Code.OK ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR, [ message ]);
      });
    });

    it(`ViaRpc ${operation} 传输异常仍抛出且记录一次 ERROR hook`, async () => {
      const message = new MessageView(buildMessagePB(), mq);
      const error = new Error('transport failure');
      rpc.ackMessage.rejects(error);
      rpc.changeInvisibleDuration.rejects(error);
      rpc.forwardMessageToDeadLetterQueue.rejects(error);
      const [ pending, point ] = operation === 'ack'
        ? [ consumer.ackMessageViaRpc(message.endpoints, new AckMessageRequest(), 3000, message), MessageHookPoints.ACK ]
        : operation === 'invisible'
          ? [ consumer.changeInvisibleDurationViaRpc(message.endpoints, new ChangeInvisibleDurationRequest(), 3000, message),
            MessageHookPoints.CHANGE_INVISIBLE_DURATION ]
          : [ consumer.forwardMessageToDeadLetterQueueViaRpc(message.endpoints,
            new ForwardMessageToDeadLetterQueueRequest(), 3000, message), MessageHookPoints.FORWARD_TO_DLQ ];
      await assert.rejects(pending, reason => reason === error);
      checkHook(point, MessageHookPointsStatus.ERROR, [ message ]);
    });
  });
});
