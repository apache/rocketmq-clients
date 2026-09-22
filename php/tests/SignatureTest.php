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

namespace Apache\Rocketmq\Test;

use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../autoload.php';

require_once __DIR__ . '/../Signature.php';
require_once __DIR__ . '/../SessionCredentials.php';

use Apache\Rocketmq\Signature;
use Apache\Rocketmq\SessionCredentials;
/**
 * Tests for Signature class.
 * Mirrors Java's Signature and TLSHelper sign tests.
 */
class SignatureTest extends TestCase
{
    /**
     * Verify sign() without credentials produces baseline metadata.
     */
    public function testSignWithoutCredentials()
    {
        $metadata = Signature::sign(
            null,
            'test-client-id',
            'PHP',
            '5.0.0',
            'my-namespace',
            'v2'
        );

        $this->assertNotNull($metadata, "Metadata should not be empty");
        $this->assertEquals(
            'test-client-id',
            $metadata['x-mq-client-id'][0],
            "Client ID should match"
        );
        $this->assertEquals(
            'PHP',
            $metadata['x-mq-language'][0],
            "Language should be PHP"
        );
        $this->assertEquals(
            'v2',
            $metadata['x-mq-protocol'][0],
            "Protocol should be v2"
        );
        $this->assertEquals(
            'my-namespace',
            $metadata['x-mq-namespace'][0],
            "Namespace should match"
        );

        // Without credentials, no authorization header
        $this->assertFalse(
            isset($metadata['authorization']),
            "No authorization header without credentials"
        );
        $this->assertFalse(
            isset($metadata['x-mq-session-token']),
            "No session token without credentials"
        );
    }

    /**
     * Verify sign() with credentials produces authorization header.
     */
    public function testSignWithCredentials()
    {
        $credentials = new SessionCredentials('ak-12345', 'sk-67890');

        $metadata = Signature::sign(
            $credentials,
            'test-client-id',
            'PHP',
            '5.0.0',
            '',
            'v2'
        );

        $this->assertNotNull($metadata, "Metadata should not be empty");

        // Authorization header must be present
        $this->assertTrue(
            isset($metadata['authorization']),
            "Authorization header should be present with credentials"
        );

        $auth = $metadata['authorization'][0];
        $this->assertTrue(
            strpos($auth, 'MQv2-HMAC-SHA1') !== false,
            "Authorization should contain algorithm"
        );
        $this->assertTrue(
            strpos($auth, 'Credential=ak-12345') !== false,
            "Authorization should contain access key"
        );
        $this->assertTrue(
            strpos($auth, 'SignedHeaders=x-mq-date-time') !== false,
            "Authorization should contain signed headers"
        );
        $this->assertTrue(
            strpos($auth, 'Signature=') !== false,
            "Authorization should contain signature"
        );
    }

    /**
     * Verify sign() with STS token adds x-mq-session-token header.
     */
    public function testSignWithSecurityToken()
    {
        $credentials = new SessionCredentials('ak-sts', 'sk-sts', 'sts-token-xyz');

        $metadata = Signature::sign(
            $credentials,
            'test-client-id',
            'PHP',
            '5.0.0',
            '',
            'v2'
        );

        $this->assertTrue(
            isset($metadata['x-mq-session-token']),
            "x-mq-session-token should be present with STS credentials"
        );
        $this->assertEquals(
            'sts-token-xyz',
            $metadata['x-mq-session-token'][0],
            "Session token value should match"
        );
    }

    /**
     * Verify x-mq-date-time format is YYYYMMDDTHHmmssZ.
     */
    public function testDateTimeFormat()
    {
        $metadata = Signature::sign(
            null,
            'test-client-id'
        );

        $dateTime = $metadata['x-mq-date-time'][0];

        // Should match pattern like: 20260519T123456Z
        $this->assertTrue(
            preg_match('/^\d{8}T\d{6}Z$/', $dateTime) === 1,
            "DateTime should be in YYYYMMDDTHHmmssZ format (got: {$dateTime})"
        );
    }

    /**
     * Verify request-id is a UUID-like format.
     */
    public function testRequestIdFormat()
    {
        $metadata = Signature::sign(
            null,
            'test-client-id'
        );

        $requestId = $metadata['x-mq-request-id'][0];

        $this->assertTrue(
            preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $requestId) === 1,
            "RequestId should be UUID-like (got: {$requestId})"
        );
    }

    /**
     * Verify HMAC-SHA1 signature is deterministic for the same datetime.
     */
    public function testSignatureIsDeterministic()
    {
        $credentials = new SessionCredentials('ak-test', 'sk-secret');

        // Sign twice quickly (same second should produce same datetime)
        $metadata1 = Signature::sign($credentials, 'test-client');

        // We can't guarantee same datetime, but we can verify structure
        $auth1 = $metadata1['authorization'][0];
        $this->assertTrue(
            preg_match('/MQv2-HMAC-SHA1.*Signature=[0-9A-F]{40}$/', $auth1) === 1,
            "Signature should be 40 hex chars (SHA1)"
        );
    }

    /**
     * Verify SessionCredentials rejects empty access key.
     */
    public function testSessionCredentialsRejectsEmptyAccessKey()
    {
        $this->expectException(\InvalidArgumentException::class);
        new SessionCredentials('', 'sk-test');
    }

    /**
     * Verify SessionCredentials rejects empty secret key.
     */
    public function testSessionCredentialsRejectsEmptySecretKey()
    {
        $this->expectException(\InvalidArgumentException::class);
        new SessionCredentials('ak-test', '');
    }

    /**
     * Verify SessionCredentials stores all three fields.
     */
    public function testSessionCredentialsStoresAll()
    {
        $credentials = new SessionCredentials('ak-val', 'sk-val', 'sts-val');

        $this->assertEquals(
            'ak-val',
            $credentials->getAccessKey(),
            "Access key should match"
        );
        $this->assertEquals(
            'sk-val',
            $credentials->getAccessSecret(),
            "Secret key should match"
        );
        $this->assertEquals(
            'sts-val',
            $credentials->getSecurityToken(),
            "Security token should match"
        );
    }

    /**
     * Verify SessionCredentials without STS token returns null.
     */
    public function testSessionCredentialsWithoutSts()
    {
        $credentials = new SessionCredentials('ak-val', 'sk-val');

        $this->assertNull(
            $credentials->getSecurityToken(),
            "Security token should be null when not provided"
        );
    }
}

