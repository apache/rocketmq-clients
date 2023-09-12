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

/* eslint-disable @typescript-eslint/no-var-requires */
const { Producer } = require('..');

async function main() {
  const producer = new Producer({
    endpoints: '127.0.0.1:8081',
    logger: console,
  });
  await producer.startup();

  const receipt = await producer.send({
    topic: 'TopicTest',
    tag: 'nodejs-demo',
    body: Buffer.from(JSON.stringify({
      hello: 'rocketmq-client-nodejs world 😄',
      now: Date(),
    })),
  });
  console.log(receipt);

  await producer.shutdown();
  // process.exit(0);
}

main();
