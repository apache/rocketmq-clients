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

import os from 'node:os';
import path from 'node:path';
import { readFileSync } from 'node:fs';
import { UA, Language } from '../../proto/apache/rocketmq/v2/definition_pb';

const VERSION: string = JSON.parse(readFileSync(path.join(__dirname, '../../package.json'), 'utf-8')).version;

export class UserAgent {
  static readonly INSTANCE = new UserAgent(VERSION, os.platform(), os.hostname());

  readonly version: string;
  readonly platform: string;
  readonly hostname: string;

  constructor(version: string, platform: string, hostname: string) {
    this.version = version;
    this.platform = platform;
    this.hostname = hostname;
  }

  toProtobuf() {
    return new UA()
      .setLanguage(Language.NODE_JS)
      .setVersion(this.version)
      .setPlatform(this.platform)
      .setHostname(this.hostname);
  }
}
