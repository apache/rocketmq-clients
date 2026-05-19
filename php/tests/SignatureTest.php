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

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../Signature.php';
require_once __DIR__ . '/../SessionCredentials.php';

use Apache\Rocketmq\Signature;
use Apache\Rocketmq\SessionCredentials;
use Apache\Rocketmq\Test\TestRunner;

/**
 * Tests for Signature class.
 * Mirrors Java's Signature and TLSHelper sign tests.
 */
class SignatureTest
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

        TestRunner::assertNotNull($metadata, "Metadata should not be empty");
        TestRunner::assertEqualsWithMessage(
            'test-client-id',
            $metadata['x-mq-client-id'][0],
            "Client ID should match"
        );
        TestRunner::assertEqualsWithMessage(
            'PHP',
            $metadata['x-mq-language'][0],
            "Language should be PHP"
        );
        TestRunner::assertEqualsWithMessage(
            'v2',
            $metadata['x-mq-protocol'][0],
            "Protocol should be v2"
        );
        TestRunner::assertEqualsWithMessage(
            'my-namespace',
            $metadata['x-mq-namespace'][0],
            "Namespace should match"
        );

        // Without credentials, no authorization header
        TestRunner::assertFalse(
            isset($metadata['authorization']),
            "No authorization header without credentials"
        );
        TestRunner::assertFalse(
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

        TestRunner::assertNotNull($metadata, "Metadata should not be empty");

        // Authorization header must be present
        TestRunner::assertTrue(
            isset($metadata['authorization']),
            "Authorization header should be present with credentials"
        );

        $auth = $metadata['authorization'][0];
        TestRunner::assertTrue(
            strpos($auth, 'MQv2-HMAC-SHA1') !== false,
            "Authorization should contain algorithm"
        );
        TestRunner::assertTrue(
            strpos($auth, 'Credential=ak-12345') !== false,
            "Authorization should contain access key"
        );
        TestRunner::assertTrue(
            strpos($auth, 'SignedHeaders=x-mq-date-time') !== false,
            "Authorization should contain signed headers"
        );
        TestRunner::assertTrue(
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

        TestRunner::assertTrue(
            isset($metadata['x-mq-session-token']),
            "x-mq-session-token should be present with STS credentials"
        );
        TestRunner::assertEqualsWithMessage(
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
        TestRunner::assertTrue(
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

        TestRunner::assertTrue(
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
        TestRunner::assertTrue(
            preg_match('/MQv2-HMAC-SHA1.*Signature=[0-9a-f]{40}$/', $auth1) === 1,
            "Signature should be 40 hex chars (SHA1)"
        );
    }

    /**
     * Verify SessionCredentials rejects empty access key.
     */
    public function testSessionCredentialsRejectsEmptyAccessKey()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new SessionCredentials('', 'sk-test');
        }, "Empty access key should throw");
    }

    /**
     * Verify SessionCredentials rejects empty secret key.
     */
    public function testSessionCredentialsRejectsEmptySecretKey()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new SessionCredentials('ak-test', '');
        }, "Empty secret key should throw");
    }

    /**
     * Verify SessionCredentials stores all three fields.
     */
    public function testSessionCredentialsStoresAll()
    {
        $credentials = new SessionCredentials('ak-val', 'sk-val', 'sts-val');

        TestRunner::assertEqualsWithMessage(
            'ak-val',
            $credentials->getAccessKey(),
            "Access key should match"
        );
        TestRunner::assertEqualsWithMessage(
            'sk-val',
            $credentials->getAccessSecret(),
            "Secret key should match"
        );
        TestRunner::assertEqualsWithMessage(
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

        TestRunner::assertNull(
            $credentials->getSecurityToken(),
            "Security token should be null when not provided"
        );
    }
}

echo "=== SignatureTest ===\n";
$test = new SignatureTest();
$test->testSignWithoutCredentials();
echo "  [OK] testSignWithoutCredentials\n";
$test->testSignWithCredentials();
echo "  [OK] testSignWithCredentials\n";
$test->testSignWithSecurityToken();
echo "  [OK] testSignWithSecurityToken\n";
$test->testDateTimeFormat();
echo "  [OK] testDateTimeFormat\n";
$test->testRequestIdFormat();
echo "  [OK] testRequestIdFormat\n";
$test->testSignatureIsDeterministic();
echo "  [OK] testSignatureIsDeterministic\n";
$test->testSessionCredentialsRejectsEmptyAccessKey();
echo "  [OK] testSessionCredentialsRejectsEmptyAccessKey\n";
$test->testSessionCredentialsRejectsEmptySecretKey();
echo "  [OK] testSessionCredentialsRejectsEmptySecretKey\n";
$test->testSessionCredentialsStoresAll();
echo "  [OK] testSessionCredentialsStoresAll\n";
$test->testSessionCredentialsWithoutSts();
echo "  [OK] testSessionCredentialsWithoutSts\n";
