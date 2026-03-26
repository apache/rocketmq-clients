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

import { Producer } from '..';
import { topics, endpoints, sessionCredentials, namespace, tag } from './ProducerSingleton';
import pkg from 'rocketmq-client-nodejs/proto/apache/rocketmq/v2/definition_pb.js';
const { TransactionResolution } = pkg;


(async () => {
  const producer = new Producer({
    endpoints,
    namespace,
    sessionCredentials,
    maxAttempts: 2,
    checker: {
      async check(messageView) {
        console.log(messageView);
        return TransactionResolution.COMMIT;
      },
    },
  });
  await producer.startup();
  const transaction = producer.beginTransaction();
  const receipt = await producer.send({
    topic: topics.transaction,
    tag,
    body: Buffer.from(JSON.stringify({
      hello: 'rocketmq-client-nodejs world 😄',
      now: Date(),
    })),
  }, transaction);
  await transaction.commit();
  console.log(receipt);
})();
