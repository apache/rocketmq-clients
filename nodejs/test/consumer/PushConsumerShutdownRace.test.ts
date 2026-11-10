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

/**
 * Regression test for the "bare receive loop after shutdown" race:
 *
 * An assignment scan RPC in flight when shutdown() starts used to resolve
 * AFTER shutdown had cleared the process queue table; its .then callback
 * (#syncProcessQueue) had no running-state guard, so it created a brand-new
 * ProcessQueue (never dropped/aborted) and started its receive loop. All
 * lifecycle components (heartbeat/settings/route/scan timers, telemetry,
 * RPC manager) were already shut down, yet the resurrected loop kept
 * issuing ReceiveMessageRequest forever — the exact "zombie" shape seen on
 * rmq-cn-vi74t2gev05 (receive only, no heartbeat/telemetry/assignment/
 * lite-subscription sync, settings null on proxy).
 *
 * Fixed by guarding #syncProcessQueue/#createProcessQueue with a shutdown
 * flag and checking #dropped in the ProcessQueue fetch entry points; this
 * test asserts the late assignment can no longer resurrect the loop.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import {
  AddressScheme,
  Assignment as AssignmentPB,
  Broker as BrokerPB,
  Code,
  Endpoints as EndpointsPB,
  MessageQueue as MessageQueuePB,
  Permission,
  Resource,
  Status,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  QueryAssignmentResponse,
  QueryRouteResponse,
  ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';

function makeEndpointsPb(): EndpointsPB {
  const endpointsPb = new EndpointsPB();
  endpointsPb.setScheme(AddressScheme.IPV4);
  endpointsPb.addAddresses().setHost('127.0.0.1').setPort(8081);
  return endpointsPb;
}

function makeMessageQueuePb(): MessageQueuePB {
  const brokerPb = new BrokerPB();
  brokerPb.setName('broker-a');
  brokerPb.setId(0);
  brokerPb.setEndpoints(makeEndpointsPb());
  const mqPb = new MessageQueuePB();
  mqPb.setId(0);
  mqPb.setTopic(new Resource().setName('topic-a'));
  mqPb.setBroker(brokerPb);
  mqPb.setPermission(Permission.READ_WRITE);
  return mqPb;
}

function okStatus(): Status {
  return new Status().setCode(Code.OK);
}

describe('PushConsumer shutdown vs in-flight assignment scan race', () => {
  it('does not resurrect a receive loop when the in-flight scan settles after shutdown', async () => {
    const consumer = new PushConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'cg-race',
      subscriptions: new Map([[ 'topic-a', new FilterExpression('*') ]]),
      messageListener: async () => 0 as never,
    } as never);

    let receiveCalls = 0;
    let rpcClosed = false;
    // Resolved manually to control exactly when the in-flight scan RPC
    // settles relative to shutdown().
    let resolveAssignment!: (resp: QueryAssignmentResponse) => void;
    const assignmentHeld = new Promise<QueryAssignmentResponse>(resolve => {
      resolveAssignment = resolve;
    });

    const fakeRpc: any = {
      queryRoute: async () => {
        const resp = new QueryRouteResponse();
        resp.setStatus(okStatus());
        resp.addMessageQueues(makeMessageQueuePb());
        return resp;
      },
      queryAssignment: () => assignmentHeld,
      receiveMessage: async () => {
        receiveCalls++;
        // Yield one macrotask per call (stands in for the network RTT) so a
        // resurrected loop cannot starve the event loop of timers.
        await new Promise(r => setImmediate(r));
        const resp = new ReceiveMessageResponse();
        resp.setStatus(okStatus());
        return [ resp ];
      },
      notifyClientTermination: async () => ({ getStatus: () => okStatus() }),
      telemetry: () => {
        const stream: any = new EventEmitter();
        stream.write = () => true;
        stream.end = () => undefined;
        return stream;
      },
      close: () => { rpcClosed = true; },
    };
    (consumer as any).rpcClientManager = fakeRpc;

    // Start the 5s assignment scanner (normally started by startup()); the
    // first scan fires after 1s and its queryAssignment RPC stays in flight
    // because the fake holds the promise.
    (consumer as any).startAssignmentScanning();
    await new Promise(r => setTimeout(r, 1100));

    // Shutdown while the scan RPC is in flight: no inflight receives and no
    // cached messages, so the drain waits pass immediately and shutdown()
    // completes (timers cleared, NotifyClientTermination sent, RPC manager
    // closed) while the scan RPC is still pending.
    await consumer.shutdown();
    assert.strictEqual(consumer.isRunning(), false);
    assert.strictEqual(rpcClosed, true, 'rpc manager must have been closed');
    assert.strictEqual(receiveCalls, 0, 'no receive may have been issued yet');

    // The in-flight scan settles AFTER shutdown: the shutdown guard must
    // prevent #syncProcessQueue from creating any process queue.
    const assignmentResp = new QueryAssignmentResponse();
    assignmentResp.setStatus(okStatus());
    assignmentResp.addAssignments(new AssignmentPB().setMessageQueue(makeMessageQueuePb()));
    resolveAssignment(assignmentResp);

    await new Promise(r => setTimeout(r, 300));

    // The consumer is shut down and must stay silent: no resurrected loop.
    assert.strictEqual(receiveCalls, 0,
      `receive loop was resurrected after shutdown, receiveCalls=${receiveCalls}`);
    assert.strictEqual(consumer.processQueues().length, 0,
      'no process queue may be created after shutdown started');
  });
});
