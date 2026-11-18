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
import { ClientType, FilterType, Settings as SettingsPB, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { SimpleSubscriptionSettings } from '../../src/consumer/SimpleSubscriptionSettings';
import { Endpoints } from '../../src/route';
import { createDuration } from '../../src/util';
import { buildSettings } from '../support/protobuf';

describe('FilterExpression', () => {
  it('SUB_ALL 默认使用 TAG 和 *，普通表达式默认 TAG', () => {
    assert.strictEqual(FilterExpression.SUB_ALL.expression, '*');
    assert.strictEqual(FilterExpression.SUB_ALL.filterType, FilterType.TAG);
    const proto = FilterExpression.SUB_ALL.toProtobuf();
    assert.strictEqual(proto.getExpression(), '*');
    assert.strictEqual(proto.getType(), FilterType.TAG);
    assert.strictEqual(new FilterExpression('tag').toProtobuf().getType(), FilterType.TAG);
  });

  it('SQL92 表达式按 SQL 类型写入协议', () => {
    const expression = new FilterExpression('a > 1', FilterType.SQL);
    assert.strictEqual(expression.toProtobuf().getType(), FilterType.SQL);
    assert.strictEqual(expression.toProtobuf().getExpression(), 'a > 1');
  });

  it('保留 equals、hashCode、toString 公开行为', () => {
    const expression = new FilterExpression('tag');
    assert.ok(expression.equals(expression));
    assert.ok(expression.equals(new FilterExpression('tag')));
    assert.ok(!expression.equals(new FilterExpression('another')));
    assert.ok(!expression.equals(new FilterExpression('tag', FilterType.SQL)));
    assert.ok(!expression.equals(null as unknown as FilterExpression));
    assert.strictEqual(expression.hashCode(), new FilterExpression('tag').hashCode());
    assert.strictEqual(expression.toString(), `FilterExpression(${FilterType.TAG},tag)`);
  });
});

describe('SimpleSubscriptionSettings', () => {
  function createSettings() {
    const expressions = new Map([
      [ 'topic-a', FilterExpression.SUB_ALL ], [ 'topic-b', new FilterExpression('a > 1', FilterType.SQL) ],
    ]);
    const endpoints = new Endpoints('127.0.0.1:8081');
    const settings = new SimpleSubscriptionSettings('ns', 'client', ClientType.SIMPLE_CONSUMER,
      endpoints, 'group', 3000, 30000, expressions);
    return { settings, expressions, endpoints };
  }

  it('toProtobuf 包含类型、accessPoint、requestTimeout、group、longPollingTimeout、过滤表达式和 userAgent', () => {
    const { settings, endpoints } = createSettings();
    const proto = settings.toProtobuf();
    assert.strictEqual(proto.getClientType(), ClientType.SIMPLE_CONSUMER);
    assert.deepStrictEqual(proto.getAccessPoint()?.toObject(), endpoints.toProtobuf().toObject());
    assert.deepStrictEqual(proto.getRequestTimeout()?.toObject(), createDuration(3000).toObject());
    const subscription = proto.getSubscription()!;
    assert.strictEqual(subscription.getGroup()?.getName(), 'group');
    assert.deepStrictEqual(subscription.getLongPollingTimeout()?.toObject(), createDuration(30000).toObject());
    assert.deepStrictEqual(subscription.getSubscriptionsList().map(entry => [
      entry.getTopic()?.getName(), entry.getExpression()?.getType(), entry.getExpression()?.getExpression(),
    ]), [[ 'topic-a', FilterType.TAG, '*' ], [ 'topic-b', FilterType.SQL, 'a > 1' ]]);
    assert.ok(proto.getUserAgent()?.getVersion());
  });

  it('toProtobuf 反映同一个订阅 Map 的追加与删除', () => {
    const { settings, expressions } = createSettings();
    expressions.delete('topic-a');
    expressions.set('topic-c', new FilterExpression('tag'));
    assert.deepStrictEqual(settings.toProtobuf().getSubscription()?.getSubscriptionsList()
      .map(entry => entry.getTopic()?.getName()), [ 'topic-b', 'topic-c' ]);
  });

  it('sync 不覆盖客户端配置，接受 subscription 与其他类型 settings', () => {
    const { settings } = createSettings();
    const snapshot = settings.toProtobuf().toObject();
    const issued = buildSettings({ clientType: ClientType.SIMPLE_CONSUMER,
      subscription: new Subscription().setLongPollingTimeout(createDuration(1000)) });
    for (const proto of [ issued, buildSettings(), new SettingsPB() ]) {
      assert.doesNotThrow(() => settings.sync(proto));
      assert.deepStrictEqual(settings.toProtobuf().toObject(), snapshot);
    }
    assert.strictEqual(settings.longPollingTimeout, 30000);
  });
});
