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
import { Address, AddressScheme, Endpoints as EndpointsPB } from '../../proto/apache/rocketmq/v2/definition_pb';

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
        const [ host, port ] = endpoint.split(':');
        if (isIPv4(host)) {
          this.scheme = AddressScheme.IPV4;
        } else if (isIPv6(host)) {
          this.scheme = AddressScheme.IPV6;
        } else {
          this.scheme = AddressScheme.DOMAIN_NAME;
        }
        this.addressesList.push({ host, port: parseInt(port) || DEFAULT_PORT });
      }
    } else {
      this.scheme = endpoints.scheme;
      this.addressesList = endpoints.addressesList;
    }
    this.facade = this.addressesList.map(addr => `${addr.host}:${addr.port}`).join(',');
  }

  getGrpcTarget() {
    return this.facade;
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
}
