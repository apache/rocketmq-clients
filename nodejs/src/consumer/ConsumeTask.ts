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

import { MessageView } from '../message';
import { ConsumeResult } from './ConsumeResult';
import { MessageListener } from './MessageListener';

export class ConsumeTask {
  readonly #messageListener: MessageListener;
  readonly #messageView: MessageView;

  constructor(_clientId: string, messageListener: MessageListener, messageView: MessageView) {
    this.#messageListener = messageListener;
    this.#messageView = messageView;
  }

  async call(): Promise<ConsumeResult> {
    try {
      const result = await this.#messageListener.consume(this.#messageView);
      return result;
    } catch (e) {
      // Message listener raised an exception while consuming messages
      return ConsumeResult.FAILURE;
    }
  }
}
