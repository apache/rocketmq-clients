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

import { LiteSubscriptionManager } from './LiteSubscriptionManager';
import { OffsetOption } from './OffsetOption';

// Both Lite consumers share lifecycle ordering and entry validation; callers supply parent operations.
// Keep this helper out of consumer/index.ts exports and preserve existing Impl and manager entry points.
export class LiteConsumerSupport extends LiteSubscriptionManager {
  async startConsumer(startParent: () => Promise<void>): Promise<void> {
    await startParent();
    this.startUp();
  }

  async shutdownConsumer(shutdownParent: () => Promise<void>): Promise<void> {
    this.shutdown();
    await shutdownParent();
  }

  async subscribeLite(liteTopic: string, offsetOption?: OffsetOption | null): Promise<void> {
    this.validateArgument(liteTopic);
    await super.subscribeLite(liteTopic, offsetOption ?? null);
  }

  async unsubscribeLite(liteTopic: string): Promise<void> {
    this.validateArgument(liteTopic);
    await super.unsubscribeLite(liteTopic);
  }

  private validateArgument(liteTopic: string): void {
    if (!liteTopic || liteTopic.trim().length === 0) {
      throw new Error('liteTopic should not be blank');
    }
  }
}
