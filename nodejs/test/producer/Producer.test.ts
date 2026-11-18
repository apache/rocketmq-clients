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
import { Code, MessageType, Permission, TransactionResolution, TransactionSource } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  EndTransactionResponse,
  RecallMessageResponse,
  RecoverOrphanedTransactionCommand,
  SendMessageRequest,
} from '../../proto/apache/rocketmq/v2/service_pb';
import {
  BadRequestException,
  ForbiddenException,
  InternalErrorException,
  NotFoundException,
  TooManyRequestsException,
  UnsupportedException,
} from '../../src/exception';
import { Producer, RecallReceipt } from '../../src/producer';
import { Endpoints } from '../../src/route';
import { createSilentLogger } from '../support/fakes';
import { buildMessagePB, buildRoute, buildSettings, buildStatus } from '../support/protobuf';
import { BROKERS, routeResponse, sendResponse, startProducer, TOPIC } from './harness';

describe('Producer', () => {
  let producer: Producer | undefined;

  afterEach(async () => {
    sinon.restore();
    await producer?.shutdown();
    producer = undefined;
  });

  async function start(options: Parameters<typeof startProducer>[0] = {}) {
    const started = await startProducer(options);
    producer = started.producer;
    return started;
  }

  function message(options: Record<string, unknown> = {}) {
    return { topic: TOPIC, body: Buffer.from('body'), ...options };
  }

  function sentTo(rpc: Awaited<ReturnType<typeof startProducer>>['rpc'], call: number) {
    return (rpc.sendMessage.getCall(call).args[0] as Endpoints).facade;
  }

  function isolatedOf(producer: Producer) {
    return (producer as unknown as { isolated: Map<string, Endpoints> }).isolated;
  }

  describe('send', () => {
    it('rejects when the producer is not running', async () => {
      producer = new Producer({ endpoints: '127.0.0.1:8080', namespace: '', logger: createSilentLogger() });
      await assert.rejects(producer.send(message()), /Producer is not running now/);
    });

    it('returns the receipt without retrying after the first attempt succeeds', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.resolves(sendResponse([{ messageId: 'id-1' }]));

      const receipt = await producer.send(message({ tag: 'tag-a', keys: [ 'key-a' ] }));

      sinon.assert.calledOnce(rpc.sendMessage);
      assert.equal(receipt.messageId, 'id-1');
      assert.equal(receipt.recallHandle, 'recall-handle-0');
      assert.ok(BROKERS.some(b => b.endpoints === receipt.endpoints.facade));
      const request = rpc.sendMessage.firstCall.args[1] as SendMessageRequest;
      const sent = request.getMessagesList()[0];
      assert.equal(sent.getTopic()!.getName(), TOPIC);
      assert.equal(sent.getSystemProperties()!.getTag(), 'tag-a');
      assert.equal(sent.getSystemProperties()!.getMessageType(), MessageType.NORMAL);
      assert.equal(sent.getSystemProperties()!.getQueueId(), receipt.messageQueue.queueId);
    });

    it('retries immediately on another broker and isolates the failed endpoints', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.onFirstCall().rejects(new Error('network failure'));
      rpc.sendMessage.onSecondCall().resolves(sendResponse());

      await producer.send(message());

      sinon.assert.calledTwice(rpc.sendMessage);
      assert.notEqual(sentTo(rpc, 0), sentTo(rpc, 1));
      assert.deepEqual([ ...isolatedOf(producer).keys() ], [ sentTo(rpc, 0) ]);
    });

    it('retries on a failure status returned by the remote', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.onFirstCall().resolves(sendResponse([{}], Code.INTERNAL_ERROR));
      rpc.sendMessage.onSecondCall().resolves(sendResponse([{ messageId: 'id-2' }]));

      const receipt = await producer.send(message());

      sinon.assert.calledTwice(rpc.sendMessage);
      assert.equal(receipt.messageId, 'id-2');
    });

    it('backs off before retrying a throttled request', async () => {
      const { producer, rpc } = await start();
      // initial=1s, multiplier=2: the delay before the 2nd attempt is 2s.
      producer.publishingSettings.sync(buildSettings());
      rpc.sendMessage.onFirstCall().resolves(sendResponse([{}], Code.TOO_MANY_REQUESTS));
      rpc.sendMessage.onSecondCall().resolves(sendResponse());
      const clock = sinon.useFakeTimers({ toFake: [ 'setTimeout' ] });

      const sending = producer.send(message());
      await clock.tickAsync(1999);
      sinon.assert.calledOnce(rpc.sendMessage);
      await clock.tickAsync(1);
      await sending;
      sinon.assert.calledTwice(rpc.sendMessage);
    });

    it('throws the last error after running out of attempts', async () => {
      const { producer, rpc } = await start({ maxAttempts: 2 });
      rpc.sendMessage.onFirstCall().rejects(new Error('first failure'));
      rpc.sendMessage.onSecondCall().rejects(new Error('second failure'));

      await assert.rejects(producer.send(message()), /second failure/);
      sinon.assert.calledTwice(rpc.sendMessage);
    });

    it('surfaces the typed exception of a throttled request after running out of attempts', async () => {
      const { producer, rpc } = await start({ maxAttempts: 1 });
      rpc.sendMessage.resolves(sendResponse([{}], Code.TOO_MANY_REQUESTS));

      await assert.rejects(producer.send(message()), TooManyRequestsException);
    });

    it('fails without retrying when the receipt count does not match the message count', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.resolves(sendResponse([]));

      await assert.rejects(producer.send(message()), (err: Error) => {
        assert.ok(err instanceof InternalErrorException);
        assert.match(err.message, /received send receipt's quantity 0 is not equal to sent message's quantity 1/);
        return true;
      });
      sinon.assert.calledOnce(rpc.sendMessage);
      assert.equal(isolatedOf(producer).size, 0);
    });

    it('sends a FIFO message to the queue selected by its message group', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.resolves(sendResponse());

      const first = await producer.send(message({ messageGroup: 'order-1' }));
      const second = await producer.send(message({ messageGroup: 'order-1' }));

      assert.ok(first.messageQueue.equals(second.messageQueue));
      const sent = (rpc.sendMessage.firstCall.args[1] as SendMessageRequest).getMessagesList()[0];
      assert.equal(sent.getSystemProperties()!.getMessageType(), MessageType.FIFO);
      assert.equal(sent.getSystemProperties()!.getMessageGroup(), 'order-1');
    });

    it('rejects a message type the topic does not accept', async () => {
      const { producer, rpc } = await start();
      rpc.queryRoute.resolves(routeResponse(undefined, [ MessageType.FIFO ]));

      await assert.rejects(producer.send(message()), /Current message type not match with topic accept message types/);
      sinon.assert.notCalled(rpc.sendMessage);
    });

    it('sends to every master queue over successive sends', async () => {
      const { producer, rpc } = await start();
      rpc.queryRoute.resolves(routeResponse(buildRoute({ topic: TOPIC, brokers: BROKERS, queuesPerBroker: 4 })));
      rpc.sendMessage.resolves(sendResponse());

      const picked = new Set<string>();
      for (let i = 0; i < 12; i++) {
        const receipt = await producer.send(message());
        picked.add(`${receipt.messageQueue.broker.name}#${receipt.messageQueue.queueId}`);
      }
      assert.equal(picked.size, 12);
    });
  });

  describe('route update', () => {
    function refreshRoutes(producer: Producer) {
      return (producer as unknown as { updateRoutes(): Promise<void> }).updateRoutes();
    }

    it('sends to the queues of the refreshed route', async () => {
      const { producer, rpc } = await start();
      rpc.sendMessage.resolves(sendResponse());
      await producer.send(message());

      const moved = [{ name: 'broker-d', endpoints: '127.0.0.1:8084' }];
      rpc.queryRoute.resolves(routeResponse(buildRoute({ topic: TOPIC, brokers: moved })));
      await refreshRoutes(producer);

      for (let i = 0; i < 3; i++) {
        const receipt = await producer.send(message());
        assert.equal(receipt.endpoints.facade, '127.0.0.1:8084');
      }
    });

    it('keeps the previous queues when the refreshed route has no writable queue', async () => {
      const { producer, rpc, logger } = await start();
      rpc.sendMessage.resolves(sendResponse());
      await producer.send(message());

      rpc.queryRoute.resolves(routeResponse(buildRoute({ topic: TOPIC, brokers: BROKERS, permission: Permission.READ })));
      await refreshRoutes(producer);

      sinon.assert.calledWithMatch(logger.error, /Failed to update publishing load balancer/);
      const receipt = await producer.send(message());
      assert.ok(BROKERS.some(b => b.endpoints === receipt.endpoints.facade));
    });

    it('fails to send to a topic whose first route has no writable queue', async () => {
      const { producer, rpc } = await start();
      rpc.queryRoute.resolves(routeResponse(buildRoute({ topic: TOPIC, brokers: BROKERS, permission: Permission.READ })));

      await assert.rejects(producer.send(message()), /No writable message queue found/);
      sinon.assert.notCalled(rpc.sendMessage);
    });
  });

  describe('recallMessage', () => {
    it('sends the recall request to the access point and returns the receipt', async () => {
      const { producer, rpc } = await start({ namespace: 'ns' });
      rpc.recallMessage.resolves(new RecallMessageResponse().setStatus(buildStatus()).setMessageId('id-1'));

      const receipt = await producer.recallMessage(TOPIC, 'handle-1');

      assert.ok(receipt instanceof RecallReceipt);
      assert.equal(receipt.messageId, 'id-1');
      const [ endpoints, request ] = rpc.recallMessage.firstCall.args;
      assert.equal(endpoints.facade, '127.0.0.1:8080');
      assert.equal(request.getTopic()!.getName(), TOPIC);
      assert.equal(request.getTopic()!.getResourceNamespace(), 'ns');
      assert.equal(request.getRecallHandle(), 'handle-1');
    });

    const cases = [
      [ Code.BAD_REQUEST, BadRequestException ],
      [ Code.INVALID_RECEIPT_HANDLE, BadRequestException ],
      [ Code.FORBIDDEN, ForbiddenException ],
      [ Code.NOT_FOUND, NotFoundException ],
      [ Code.TOPIC_NOT_FOUND, NotFoundException ],
      [ Code.MESSAGE_NOT_FOUND, NotFoundException ],
      [ Code.TOO_MANY_REQUESTS, TooManyRequestsException ],
      [ Code.INTERNAL_ERROR, InternalErrorException ],
      [ Code.UNSUPPORTED, UnsupportedException ],
    ] as const;
    for (const [ code, exception ] of cases) {
      it(`maps status code ${code} to ${exception.name}`, async () => {
        const { producer, rpc } = await start();
        rpc.recallMessage.resolves(new RecallMessageResponse().setStatus(buildStatus(code, 'recall failed')));

        await assert.rejects(producer.recallMessage(TOPIC, 'handle-1'), (err: Error & { code: number }) => {
          assert.ok(err instanceof exception, `expected ${exception.name}, got ${err.constructor.name}`);
          assert.equal(err.code, code);
          assert.match(err.message, /recall failed/);
          return true;
        });
      });
    }

    it('rejects when the producer is not running', async () => {
      producer = new Producer({ endpoints: '127.0.0.1:8080', namespace: '', logger: createSilentLogger() });
      await assert.rejects(producer.recallMessage(TOPIC, 'handle-1'), /Producer is not running now/);
    });

    it('rejects a blank topic or an empty recall handle', async () => {
      const { producer, rpc } = await start();
      await assert.rejects(producer.recallMessage('', 'handle-1'), /Topic is invalid/);
      await assert.rejects(producer.recallMessage('  ', 'handle-1'), /Topic is invalid/);
      await assert.rejects(producer.recallMessage(TOPIC, ''), /Recall handle is invalid/);
      sinon.assert.notCalled(rpc.recallMessage);
    });
  });

  describe('onRecoverOrphanedTransactionCommand', () => {
    const endpoints = new Endpoints('127.0.0.1:8081');

    function command(messagePB = buildMessagePB({ messageId: 'id-1', messageType: MessageType.TRANSACTION })) {
      return new RecoverOrphanedTransactionCommand().setTransactionId('tx-1').setMessage(messagePB);
    }

    it('ignores the command when no checker is registered', async () => {
      const { producer, rpc, logger } = await start();
      await producer.onRecoverOrphanedTransactionCommand(endpoints, command());
      sinon.assert.notCalled(rpc.endTransaction);
      sinon.assert.calledWithMatch(logger.error, /No transaction checker registered/);
    });

    it('ignores the command when the message cannot be decoded', async () => {
      const check = sinon.stub().resolves(TransactionResolution.COMMIT);
      const { producer, rpc, logger } = await start({ checker: { check } });
      const messagePB = buildMessagePB({ messageId: 'id-1' });
      messagePB.getSystemProperties()!.clearBodyDigest();

      await producer.onRecoverOrphanedTransactionCommand(endpoints, command(messagePB));

      sinon.assert.notCalled(check);
      sinon.assert.notCalled(rpc.endTransaction);
      sinon.assert.calledWithMatch(logger.error, /Failed to decode message/);
    });

    it('ignores a command without a message instead of throwing', async () => {
      const check = sinon.stub().resolves(TransactionResolution.COMMIT);
      const { producer, rpc, logger } = await start({ checker: { check } });

      await producer.onRecoverOrphanedTransactionCommand(endpoints,
        new RecoverOrphanedTransactionCommand().setTransactionId('tx-1'));

      sinon.assert.notCalled(check);
      sinon.assert.notCalled(rpc.endTransaction);
      sinon.assert.calledWithMatch(logger.error, /message is absent/);
    });

    for (const resolution of [ TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED, null ]) {
      it(`does not end the transaction when the checker returns ${resolution}`, async () => {
        const check = sinon.stub().resolves(resolution);
        const { producer, rpc } = await start({ checker: { check } });

        await producer.onRecoverOrphanedTransactionCommand(endpoints, command());

        sinon.assert.calledOnce(check);
        sinon.assert.notCalled(rpc.endTransaction);
      });
    }

    it('ends the transaction with the checker resolution as a server check', async () => {
      const check = sinon.stub().resolves(TransactionResolution.COMMIT);
      const { producer, rpc } = await start({ checker: { check } });
      rpc.endTransaction.resolves(new EndTransactionResponse().setStatus(buildStatus()));

      await producer.onRecoverOrphanedTransactionCommand(endpoints, command());

      assert.equal(check.firstCall.args[0].messageId, 'id-1');
      const [ target, request ] = rpc.endTransaction.firstCall.args;
      assert.equal(target, endpoints);
      assert.equal(request.getMessageId(), 'id-1');
      assert.equal(request.getTransactionId(), 'tx-1');
      assert.equal(request.getTopic()!.getName(), 'test-topic');
      assert.equal(request.getResolution(), TransactionResolution.COMMIT);
      assert.equal(request.getSource(), TransactionSource.SOURCE_SERVER_CHECK);
    });

    it('logs instead of throwing when the checker fails', async () => {
      const check = sinon.stub().rejects(new Error('checker failure'));
      const { producer, rpc, logger } = await start({ checker: { check } });

      await producer.onRecoverOrphanedTransactionCommand(endpoints, command());

      sinon.assert.notCalled(rpc.endTransaction);
      sinon.assert.calledWithMatch(logger.error, /Exception raised while checking the transaction/);
    });
  });
});
