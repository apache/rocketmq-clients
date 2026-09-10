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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Endpoints } from '../../src/route';
import { IpResolver } from '../../src/route/IpNameResolver';

type Endpoint = { addresses: { host: string; port: number }[] };

function makeTarget(path: string) {
  return { scheme: 'ip', authority: '', path } as any;
}

/**
 * Drive the resolver with a capturing listener and return the resolved endpoints.
 * The listener is invoked on nextTick, so we await it.
 */
function resolve(path: string): Promise<Endpoint[]> {
  return new Promise((resolvePromise, rejectPromise) => {
    const listener = (endpointList: any, _attrs: any, _cfg: any, _note: string) => {
      if (endpointList.ok) {
        resolvePromise(endpointList.value as Endpoint[]);
      } else {
        rejectPromise(new Error(endpointList.error.details));
      }
    };
    const resolver = new IpResolver(makeTarget(path), listener as any, {} as any);
    resolver.updateResolution();
  });
}

function toKeySet(eps: Endpoint[]): Set<string> {
  return new Set(eps.map(e => `${e.addresses[0].host}:${e.addresses[0].port}`));
}

describe('IpNameResolver (custom ip scheme, mirrors Java IpNameResolverFactory)', () => {
  it('parses comma-separated host:port and resolves every address', async () => {
    const eps = await resolve('127.0.0.1:10911,127.0.0.2:10912');
    assert.strictEqual(eps.length, 2);
    const keys = toKeySet(eps);
    assert.ok(keys.has('127.0.0.1:10911'));
    assert.ok(keys.has('127.0.0.2:10912'));
    for (const ep of eps) {
      assert.strictEqual(ep.addresses.length, 1, 'each address becomes its own EquivalentAddressGroup');
    }
  });

  it('handles bracketed IPv6 addresses', async () => {
    const eps = await resolve('[::1]:10911,[fe80::1]:10912');
    const keys = toKeySet(eps);
    assert.ok(keys.has('::1:10911'));
    assert.ok(keys.has('fe80::1:10912'));
  });

  it('applies default port 80 when omitted', async () => {
    const eps = await resolve('127.0.0.1');
    assert.strictEqual(eps.length, 1);
    assert.strictEqual(eps[0].addresses[0].port, 80);
  });

  it('reports an error for an unparseable address', async () => {
    await assert.rejects(() => resolve('localhost:notaport'));
  });

  it('shuffles the address order (Fisher-Yates)', async () => {
    const original = Math.random;
    // Force a deterministic Fisher-Yates permutation. With random=0:
    //   input [a,b,c] -> i=2: swap(2,0)=[c,b,a] -> i=1: swap(1,0)=[b,c,a]
    Math.random = () => 0;
    try {
      const eps = await resolve('10.0.0.1:80,10.0.0.2:80,10.0.0.3:80');
      const order = eps.map(e => e.addresses[0].host);
      assert.deepStrictEqual(order, ['10.0.0.2', '10.0.0.3', '10.0.0.1']);
    } finally {
      Math.random = original;
    }
  });

  it('returns a permutation (all original addresses, no duplicates) across runs', async () => {
    const input = '10.0.0.1:80,10.0.0.2:80,10.0.0.3:80,10.0.0.4:80';
    const expected = new Set(['10.0.0.1:80', '10.0.0.2:80', '10.0.0.3:80', '10.0.0.4:80']);
    for (let i = 0; i < 20; i++) {
      const eps = await resolve(input);
      assert.strictEqual(eps.length, 4);
      const keys = toKeySet(eps);
      assert.deepStrictEqual(keys, expected);
    }
  });
});

describe('Endpoints.getGrpcTarget uses the custom ip scheme', () => {
  it('emits ip: scheme for IPv4/IPv6 endpoints', () => {
    assert.ok(new Endpoints('127.0.0.1:10911;127.0.0.2:10912').getGrpcTarget().startsWith('ip:'));
    assert.ok(new Endpoints('[::1]:10911;[fe80::1]:10912').getGrpcTarget().startsWith('ip:'));
  });

  it('keeps dns: scheme for domain names', () => {
    assert.ok(new Endpoints('example.com:8080;example.org:8081').getGrpcTarget().startsWith('dns:'));
  });
});
