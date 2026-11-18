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
import { MessageType, TransactionResolution, TransactionSource } from '../../proto/apache/rocketmq/v2/definition_pb';
import { EndTransactionResponse, SendMessageRequest } from '../../proto/apache/rocketmq/v2/service_pb';
import { ILogger } from '../../src/client';
import { Producer, Transaction } from '../../src/producer';
import { buildStatus } from '../support/protobuf';
import { sendResponse, startProducer, TOPIC } from './harness';

describe('Transaction', () => {
  let producer: Producer | undefined;
  const checker = { check: async () => TransactionResolution.COMMIT };

  afterEach(async () => {
    sinon.restore();
    await producer?.shutdown();
    producer = undefined;
  });

  async function start(logger?: ILogger) {
    const started = await startProducer({ checker, ...(logger ? { logger } : {}) });
    producer = started.producer;
    started.rpc.sendMessage.resolves(sendResponse([{ messageId: 'id-1', transactionId: 'tx-1' }]));
    started.rpc.endTransaction.resolves(new EndTransactionResponse().setStatus(buildStatus()));
    return started;
  }

  function message() {
    return { topic: TOPIC, body: Buffer.from('body') };
  }

  for (const [ name, resolution ] of [[ 'commit', TransactionResolution.COMMIT ], [ 'rollback', TransactionResolution.ROLLBACK ]] as const) {
    it(`${name} ends the sent message with resolution ${resolution}`, async () => {
      const { producer, rpc } = await start();
      const transaction = producer.beginTransaction();
      const receipt = await producer.send(message(), transaction);

      await transaction[name]();

      const sent = (rpc.sendMessage.firstCall.args[1] as SendMessageRequest).getMessagesList()[0];
      assert.equal(sent.getSystemProperties()!.getMessageType(), MessageType.TRANSACTION);
      sinon.assert.calledOnce(rpc.endTransaction);
      const [ endpoints, request ] = rpc.endTransaction.firstCall.args;
      assert.equal(endpoints, receipt.endpoints);
      assert.equal(request.getMessageId(), 'id-1');
      assert.equal(request.getTransactionId(), 'tx-1');
      assert.equal(request.getTopic()!.getName(), TOPIC);
      assert.equal(request.getResolution(), resolution);
      assert.equal(request.getSource(), TransactionSource.SOURCE_CLIENT);
    });

    it(`${name} rejects an empty transaction`, async () => {
      const { producer, rpc } = await start();
      await assert.rejects(producer.beginTransaction()[name](), /Transactional message has not been sent yet/);
      sinon.assert.notCalled(rpc.endTransaction);
    });

    it(`${name} works with a logger without debug`, async () => {
      const logger = { info: sinon.stub(), warn: sinon.stub(), error: sinon.stub() };
      const { producer, rpc } = await start(logger);
      const transaction = producer.beginTransaction();
      await producer.send(message(), transaction);

      await transaction[name]();

      sinon.assert.calledOnce(rpc.endTransaction);
    });
  }

  it('logs through the logger of the producer', async () => {
    const { producer, logger } = await start();
    const transaction = producer.beginTransaction();
    await producer.send(message(), transaction);

    await transaction.commit();

    sinon.assert.calledWith(logger.info, 'Begin to commit transaction, messageCount=%d, clientId=%s', 1, producer.clientId);
    sinon.assert.calledWith(logger.debug, 'Commit transaction successfully, messageCount=%d, clientId=%s', 1, producer.clientId);
  });

  it('rejects more messages than the threshold', async () => {
    const { producer, rpc } = await start();
    const transaction = producer.beginTransaction();
    await producer.send(message(), transaction);

    await assert.rejects(producer.send(message(), transaction), /Message in transaction has exceeded the threshold=1/);
    sinon.assert.calledOnce(rpc.sendMessage);
  });

  it('retries a failed transactional message and ends it on the broker that accepted it', async () => {
    const { producer, rpc } = await start();
    rpc.sendMessage.onFirstCall().rejects(new Error('network failure'));
    const transaction = producer.beginTransaction();

    const receipt = await producer.send(message(), transaction);
    await transaction.commit();

    sinon.assert.calledTwice(rpc.sendMessage);
    const [ failed, accepted ] = rpc.sendMessage.getCalls().map(call => call.args[0].facade);
    assert.notEqual(failed, accepted);
    assert.equal(receipt.endpoints.facade, accepted);
    assert.equal(rpc.endTransaction.firstCall.args[0].facade, accepted);
  });

  it('throws the last error after running out of attempts for a transactional message', async () => {
    const { producer, rpc } = await start();
    rpc.sendMessage.rejects(new Error('network failure'));

    await assert.rejects(producer.send(message(), producer.beginTransaction()), /network failure/);
    sinon.assert.calledThrice(rpc.sendMessage);
  });

  it('rejects transactional messages with FIFO, delay, lite or priority semantics', async () => {
    const { producer, rpc } = await start();
    for (const extra of [{ messageGroup: 'g' }, { delay: 1000 }, { liteTopic: 'lite' }, { priority: 1 }]) {
      await assert.rejects(producer.send({ ...message(), ...extra }, producer.beginTransaction()),
        /Transactional message should not set messageGroup, liteTopic, deliveryTimestamp or priority/);
    }
    sinon.assert.notCalled(rpc.sendMessage);
  });

  it('cannot begin without a checker or before startup', async () => {
    const started = await startProducer();
    producer = started.producer;
    assert.throws(() => started.producer.beginTransaction(), /Transaction checker should not be null/);

    const stopped = new Producer({ endpoints: '127.0.0.1:8080', namespace: '', checker, logger: started.logger });
    assert.throws(() => stopped.beginTransaction(), /Producer is not running now/);
    await stopped.shutdown();
  });

  it('keeps the single-argument constructor', async () => {
    const { producer } = await start();
    const transaction = new Transaction(producer);
    assert.ok(transaction.tryAddMessage(message()).messageId);
  });
});
