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

import { isIPv4, isIPv6 } from 'node:net';
import { hashCodeOfString } from '../util';
import { Address, AddressScheme, Endpoints as EndpointsPB } from '../../proto/apache/rocketmq/v2/definition_pb';
// Side-effect import: registers the custom `ip` name resolver (shuffle + multi-address
// load balancing), mirroring the Java client's IpNameResolverFactory.
import './IpNameResolver';

const DEFAULT_PORT = 80;

export class Endpoints {
  readonly addressesList: Address.AsObject[];
  readonly scheme: AddressScheme;
  /**
   * URI path for grpc target, e.g:
   * 127.0.0.1:10911[,127.0.0.2:10912]
   */
  readonly facade: string;

  constructor(endpoints: string | EndpointsPB.AsObject) {
    if (typeof endpoints === 'string') {
      const splits = endpoints.split(';');
      this.addressesList = [];
      for (const endpoint of splits) {
        // Strip the optional http:// or https:// prefix, mirroring the Java client.
        const candidate = endpoint.trim().replace(/^https?:\/\//, '');
        let host: string;
        let port: number;
        if (candidate.startsWith('[')) {
          // Bracketed IPv6 address, e.g. [::1]:10911 or [fe80::1]
          const match = candidate.match(/^\[([^\]]+)\](?::(\d+))?$/);
          if (!match) {
            throw new TypeError(`Invalid IPv6 endpoint: ${endpoint}`);
          }
          host = match[1];
          if (!isIPv6(host)) throw new TypeError(`Invalid IPv6 endpoint: ${endpoint}`);
          port = match[2] ? Number(match[2]) : DEFAULT_PORT;
        } else if (isIPv6(candidate)) {
          // Bare IPv6 address without brackets, e.g. ::1
          host = candidate;
          port = DEFAULT_PORT;
        } else {
          // IPv4 address or domain name, e.g. 127.0.0.1:10911, example.com:80
          const index = candidate.lastIndexOf(':');
          if (index > 0) {
            host = candidate.substring(0, index);
            const portText = candidate.substring(index + 1);
            if (!/^\d+$/.test(portText)) throw new TypeError(`Invalid endpoint port: ${endpoint}`);
            port = Number(portText);
          } else {
            host = candidate;
            port = DEFAULT_PORT;
          }
        }
        if (!host || (host.includes(':') && !isIPv6(host))) {
          throw new TypeError(`Invalid endpoint host: ${endpoint}`);
        }
        if (!Number.isInteger(port) || port < 1 || port > 65535) {
          throw new TypeError(`Invalid endpoint port: ${endpoint}`);
        }
        const scheme = isIPv4(host) ? AddressScheme.IPV4
          : isIPv6(host) ? AddressScheme.IPV6 : AddressScheme.DOMAIN_NAME;
        if (this.addressesList.length > 0 && this.scheme !== scheme) {
          throw new TypeError('Mixed address schemes are not supported');
        }
        this.scheme = scheme;
        this.addressesList.push({ host, port });
      }
    } else {
      this.scheme = endpoints.scheme;
      this.addressesList = endpoints.addressesList;
    }
    this.facade = this.addressesList.map(addr => `${addr.host}:${addr.port}`).join(',');
  }

  /**
   * gRPC target with resolver scheme prefix:
   * - IP addresses:    ip:127.0.0.1:10911,127.0.0.2:10912 (custom resolver, IPv6 hosts bracketed)
   * - Domain names:    dns:example.com:8080,example.org:8081
   */
  getGrpcTarget() {
    const targets = this.addressesList.map(addr => {
      const host = this.scheme === AddressScheme.IPV6 ? `[${addr.host}]` : addr.host;
      return `${host}:${addr.port}`;
    }).join(',');
    switch (this.scheme) {
      case AddressScheme.IPV4:
      case AddressScheme.IPV6:
        // Route IP addresses through the custom `ip` resolver registered in
        // IpNameResolver, which shuffles the address list for multi-proxy load
        // balancing (equivalent to Java's IpNameResolverFactory).
        return `ip:${targets}`;
      case AddressScheme.DOMAIN_NAME:
        return `dns:${targets}`;
      default:
        return targets;
    }
  }

  toString() {
    return this.facade;
  }

  toProtobuf() {
    const endpoints = new EndpointsPB();
    endpoints.setScheme(this.scheme);
    for (const address of this.addressesList) {
      endpoints.addAddresses().setHost(address.host).setPort(address.port);
    }
    return endpoints;
  }

  equals(other: Endpoints): boolean {
    if (this === other) return true;
    if (!other) return false;
    if (this.scheme !== other.scheme) return false;
    if (this.facade !== other.facade) return false;
    if (this.addressesList.length !== other.addressesList.length) return false;
    for (let i = 0; i < this.addressesList.length; i++) {
      const addr1 = this.addressesList[i];
      const addr2 = other.addressesList[i];
      if (addr1.host !== addr2.host || addr1.port !== addr2.port) {
        return false;
      }
    }
    return true;
  }

  hashCode(): number {
    let hash = 17;
    // eslint-disable-next-line no-bitwise
    hash = (hash * 31 + this.scheme) | 0;
    // eslint-disable-next-line no-bitwise
    hash = (hash * 31 + hashCodeOfString(this.facade)) | 0;
    for (const addr of this.addressesList) {
      // eslint-disable-next-line no-bitwise
      hash = (hash * 31 + hashCodeOfString(addr.host)) | 0;
      // eslint-disable-next-line no-bitwise
      hash = (hash * 31 + addr.port) | 0;
    }
    return hash;
  }
}
