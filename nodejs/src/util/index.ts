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

import { performance } from 'node:perf_hooks';
import { createHash, createHmac } from 'node:crypto';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import { crc32 } from '@node-rs/crc32';
import siphash24 from 'siphash24';
import { Resource } from '../../proto/apache/rocketmq/v2/definition_pb';

export const MASTER_BROKER_ID = 0;

export function getTimestamp() {
  const timestamp = performance.timeOrigin + performance.now();
  const seconds = Math.floor(timestamp / 1000);
  const nanos = Math.floor((timestamp % 1000) * 1e6);
  return { seconds, nanos, timestamp };
}

// DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'"
export function getRequestDateTime() {
  // 2023-09-13T06:30:59.399Z => 20230913T063059Z
  const now = new Date().toISOString().split('.')[0].replace(/[\-\:]/g, '');
  return `${now}Z`;
}

export function sign(accessSecret: string, dateTime: string) {
  const hmacSha1 = createHmac('sha1', accessSecret);
  hmacSha1.update(dateTime);
  return hmacSha1.digest('hex').toUpperCase();
}

export function createDuration(ms: number) {
  const nanos = ms % 1000 * 1000000;
  return new Duration()
    .setSeconds(ms / 1000)
    .setNanos(nanos);
}

export function createResource(name: string) {
  return new Resource().setName(name);
}

export function crc32CheckSum(bytes: Buffer) {
  return `${crc32(bytes)}`;
}

export function md5CheckSum(bytes: Uint8Array) {
  return createHash('md5').update(bytes).digest('hex')
    .toUpperCase();
}

export function sha1CheckSum(bytes: Uint8Array) {
  return createHash('sha1').update(bytes).digest('hex')
    .toUpperCase();
}

// k0: 0x0706050403020100L, k1: 0x0f0e0d0c0b0a0908L
const SIP_HASH_24_KEY = Buffer.from([
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
]);
/**
 * Java: Hashing.sipHash24().hashBytes(messageGroup.getBytes(StandardCharsets.UTF_8)).asLong()
 */
export function calculateStringSipHash24(value: string) {
  const hash = siphash24(Buffer.from(value), SIP_HASH_24_KEY);
  return Buffer.from(hash).readBigUInt64BE();
}
