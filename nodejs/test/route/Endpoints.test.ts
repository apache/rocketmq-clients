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
 * Tests for the gRPC target resolver scheme: getGrpcTarget() routes IP
 * addresses through the custom `ip` name resolver (registered by
 * IpNameResolver) and prefixes domain names with dns:, so grpc-js resolves
 * multi-address and bare-IPv6 targets correctly.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Endpoints } from '../../src/route';
import {
  AddressScheme,
  Endpoints as EndpointsPB,
} from '../../proto/apache/rocketmq/v2/definition_pb';

function endpointsFromPb(scheme: AddressScheme, host: string, port: number): Endpoints {
  const pb = new EndpointsPB();
  pb.setScheme(scheme);
  pb.addAddresses().setHost(host).setPort(port);
  return new Endpoints(pb.toObject());
}

describe('Endpoints.getGrpcTarget with resolver scheme (custom ip resolver)', () => {
  it('should prefix ip: scheme for IPv4 addresses', () => {
    assert.strictEqual(new Endpoints('127.0.0.1:10911').getGrpcTarget(), 'ip:127.0.0.1:10911');
  });

  it('should prefix ip: scheme for multiple IPv4 addresses', () => {
    const target = new Endpoints('127.0.0.1:8081;127.0.0.2:8082').getGrpcTarget();
    assert.strictEqual(target, 'ip:127.0.0.1:8081,127.0.0.2:8082');
  });

  it('should prefix ip: scheme with brackets for IPv6 addresses', () => {
    assert.strictEqual(endpointsFromPb(AddressScheme.IPV6, '::1', 10911).getGrpcTarget(), 'ip:[::1]:10911');
  });

  it('should prefix dns: scheme for domain names', () => {
    assert.strictEqual(new Endpoints('example.com:8080').getGrpcTarget(), 'dns:example.com:8080');
  });
});
