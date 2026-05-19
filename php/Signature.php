<?php
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

namespace Apache\Rocketmq;

/**
 * Signature - Generates gRPC metadata with MQv2-HMAC-SHA1 authentication headers.
 * Mirrors Java Signature.sign(ClientConfiguration config, ClientId clientId).
 *
 * Algorithm:
 * 1. Build baseline metadata (language, protocol, version, datetime, request-id, etc.)
 * 2. Optionally add STS security token
 * 3. Compute HMAC-SHA1(accessSecret, dateTime) -> lowercase hex digest
 * 4. Build authorization header:
 *    "MQv2-HMAC-SHA1 Credential=<AK>, SignedHeaders=x-mq-date-time, Signature=<hex>"
 */
class Signature
{
    private const ALGORITHM = 'MQv2-HMAC-SHA1';
    private const CREDENTIAL = 'Credential';
    private const SIGNED_HEADERS = 'SignedHeaders';
    private const SIGNATURE = 'Signature';
    private const DATE_TIME_FORMAT = 'Ymd\THis\Z';
    private const SIGNED_HEADERS_VALUE = 'x-mq-date-time';

    /**
     * Generate signed gRPC metadata.
     *
     * @param SessionCredentials|null $credentials
     * @param string $clientId
     * @param string $language Language string (e.g., "PHP")
     * @param string $clientVersion Client version string (e.g., "5.0.0")
     * @param string $namespace Namespace string
     * @param string $protocol Protocol version (e.g., "v2")
     * @return array gRPC metadata array
     */
    public static function sign(
        ?SessionCredentials $credentials,
        string $clientId,
        string $language = 'PHP',
        string $clientVersion = '5.0.0',
        string $namespace = '',
        string $protocol = 'v2'
    ): array {
        $dateTime = gmdate(self::DATE_TIME_FORMAT);
        $requestId = self::generateUUID();

        $metadata = [
            'x-mq-client-id' => [$clientId],
            'x-mq-language' => [$language],
            'x-mq-client-version' => [$clientVersion],
            'x-mq-protocol' => [$protocol],
            'x-mq-date-time' => [$dateTime],
            'x-mq-request-id' => [$requestId],
            'x-mq-namespace' => [$namespace],
        ];

        if ($credentials !== null) {
            // Add STS security token if present
            $securityToken = $credentials->getSecurityToken();
            if (!empty($securityToken)) {
                $metadata['x-mq-session-token'] = [$securityToken];
            }

            // Compute HMAC-SHA1 signature
            $accessSecret = $credentials->getAccessSecret();
            $accessKey = $credentials->getAccessKey();
            $signature = self::hmacSha1($accessSecret, $dateTime);

            // Build authorization header value
            $authValue = self::ALGORITHM
                . ' ' . self::CREDENTIAL . '=' . $accessKey
                . ', ' . self::SIGNED_HEADERS . '=' . self::SIGNED_HEADERS_VALUE
                . ', ' . self::SIGNATURE . '=' . $signature;

            $metadata['authorization'] = [$authValue];
        }

        return $metadata;
    }

    /**
     * Compute HMAC-SHA1 and return lowercase hex digest.
     * Mirrors Java TLSHelper.sign(accessSecret, dateTime).
     */
    private static function hmacSha1(string $key, string $data): string
    {
        $raw = hash_hmac('sha1', $data, $key, true);
        return self::encodeHex($raw);
    }

    /**
     * Encode binary data as lowercase hex string.
     */
    private static function encodeHex(string $data): string
    {
        $hex = '';
        $len = strlen($data);
        for ($i = 0; $i < $len; $i++) {
            $byte = ord($data[$i]);
            $hex .= ($byte < 16 ? '0' : '') . dechex($byte);
        }
        return $hex;
    }

    /**
     * Generate a UUID v4 string for request-id.
     */
    private static function generateUUID(): string
    {
        return sprintf(
            '%08x-%04x-%04x-%04x-%012x',
            mt_rand(0, 0xffffffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) & 0x0fff | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000,
            mt_rand(0, 0xffffffffffff)
        );
    }
}
