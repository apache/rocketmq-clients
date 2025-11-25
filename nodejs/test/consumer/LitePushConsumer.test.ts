import { randomUUID } from 'node:crypto';
import { strict as assert } from 'node:assert';
import {
  LitePushConsumer,
  Producer,
  ConsumeResult,
  MessageView,
} from '../../src';
import { endpoints, sessionCredentials, namespace } from '../helper';

describe('test/consumer/LitePushConsumer.test.ts', () => {
  let producer: Producer | null = null;
  let litePushConsumer: LitePushConsumer | null = null;
  let receivedMessage: MessageView | null = null;
  let messageReceivedPromise: Promise<void> | null = null;
  let resolveMessageReceived: (() => void) | null = null;

  beforeEach(() => {
    receivedMessage = null;
    messageReceivedPromise = new Promise((resolve) => {
      resolveMessageReceived = resolve;
    });
  });

  afterEach(async () => {
    if (producer) {
      await producer.shutdown();
      producer = null;
    }
    if (litePushConsumer) {
      await litePushConsumer.shutdown();
      litePushConsumer = null;
    }
  });

  const mockMessageListener = async (message: MessageView): Promise<ConsumeResult> => {
    receivedMessage = message;
    resolveMessageReceived?.();
    return ConsumeResult.SUCCESS;
  };

  it('should receive message after subscribeLite and not receive after unsubscribeLite', async () => {
    const liteTopic = `lite-topic-${randomUUID()}`;
    const tag = `nodejs-unittest-tag-${randomUUID()}`;
    const consumerGroup = `nodejs-unittest-group-${randomUUID()}`;
    
    producer = new Producer({
      endpoints,
      namespace,
      sessionCredentials
    });
    await producer.startup();

    litePushConsumer = new LitePushConsumer({
      endpoints,
      namespace,
      sessionCredentials,
      consumerGroup,
    }, mockMessageListener);
    await litePushConsumer.startup();

    // Subscribe to a lite topic
    await litePushConsumer.subscribeLite(liteTopic, tag); // Pass tag directly

    const expectedBody = { hello: 'world' };
    const receipt = await producer.send({
      topic: liteTopic,
      tag,
      body: Buffer.from(JSON.stringify(expectedBody)),
    });
    assert(receipt.messageId);

    // Wait for the message to be received by the mock listener
    await messageReceivedPromise;

    assert(receivedMessage);
    assert.equal(receivedMessage?.messageId, receipt.messageId);
    assert.deepEqual(JSON.parse(receivedMessage!.body.toString()), expectedBody);

    // Reset for the next part of the test
    receivedMessage = null;
    messageReceivedPromise = new Promise((resolve) => {
      resolveMessageReceived = resolve;
    });

    // Unsubscribe from the lite topic
    await litePushConsumer.unsubscribeLite(liteTopic);
    
    const unexpectedBody = { hello: 'no-one' };
    const receipt2 = await producer.send({
      topic: liteTopic,
      tag,
      body: Buffer.from(JSON.stringify(unexpectedBody)),
    });
    assert(receipt2.messageId);

    // Give some time for the consumer to potentially receive the message (it should not)
    await new Promise(resolve => setTimeout(resolve, 5000)); 

    // After unsubscribing, we should not have received the message
    assert.equal(receivedMessage, null, 'Message should not have been received after unsubscribing');
  });
});
