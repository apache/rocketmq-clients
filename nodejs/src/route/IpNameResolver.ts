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

import { experimental, Metadata, status as Status, ChannelOptions } from '@grpc/grpc-js';

/**
 * Custom gRPC name resolver registered under the `ip` scheme, mirroring the Java
 * client's `IpNameResolverFactory`.
 *
 * The Java resolver converts each socket address into its own
 * `EquivalentAddressGroup` and then {@code Collections.shuffle}s the list, so the
 * gRPC load balancer starts from a random address instead of always the first
 * one. RocketMQ proxies are typically deployed as multiple endpoints behind one
 * `Endpoints`, and shuffling spreads clients across them.
 *
 * grpc-js models an `EquivalentAddressGroup` as a single `Endpoint` carrying one
 * or more equivalent `SubchannelAddress`es, so we emit one `Endpoint` per address
 * and shuffle the array — exactly the Java semantics.
 */
const DEFAULT_PORT = 80;

/**
 * Fisher–Yates shuffle (pure, does not mutate the input array).
 */
function shuffle<T>(input: readonly T[]): T[] {
  const arr = input.slice();
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
  return arr;
}

class IpResolver {
  private readonly listener: experimental.ResolverListener;
  private endpoints: experimental.Endpoint[] = [];
  private error: { code: Status; details: string; metadata: Metadata } | null = null;
  private hasReturnedResult = false;

  constructor(target: experimental.GrpcUri, listener: experimental.ResolverListener, _channelOptions: ChannelOptions) {
    this.listener = listener;
    const pathList = (target.path || '').split(',').map(s => s.trim()).filter(Boolean);
    const addresses: experimental.SubchannelAddress[] = [];
    for (const path of pathList) {
      const hostPort = experimental.splitHostPort(path);
      if (!hostPort) {
        this.error = {
          code: Status.UNAVAILABLE,
          details: `Failed to parse ip address: ${path}`,
          metadata: new Metadata(),
        };
        return;
      }
      addresses.push({
        host: hostPort.host,
        port: hostPort.port ?? DEFAULT_PORT,
      });
    }
    this.endpoints = shuffle(addresses).map(address => ({ addresses: [address] }));
  }

  updateResolution(): void {
    if (this.hasReturnedResult) {
      return;
    }
    this.hasReturnedResult = true;
    // Defer the callback to the next tick, mirroring the built-in ip resolver so
    // listeners are never invoked synchronously from the constructor path.
    process.nextTick(() => {
      if (this.error) {
        this.listener(
          experimental.statusOrFromError(this.error),
          {},
          null,
          '',
        );
      } else {
        this.listener(
          experimental.statusOrFromValue(this.endpoints),
          {},
          null,
          '',
        );
      }
    });
  }

  destroy(): void {
    this.hasReturnedResult = false;
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  static getDefaultAuthority(target: experimental.GrpcUri): string {
    return (target.path || '').split(',')[0] || 'localhost';
  }
}

// Register the resolver so that gRPC targets prefixed with `ip:` are handled by
// this class. Registration is idempotent at module-load time.
experimental.registerResolver('ip', IpResolver);

export { IpResolver };
