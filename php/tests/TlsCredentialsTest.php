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
require_once __DIR__ . '/../TlsCredentials.php';

use Apache\Rocketmq\TlsCredentials;

/**
 * Tests for TlsCredentials class.
 */
class TlsCredentialsTest
{
    public function testCreateInsecureReturnsInsecureCredentials()
    {
        $tls = TlsCredentials::createInsecure();

        TestRunner::assertTrue(
            $tls->isInsecure(),
            "createInsecure() should return insecure credentials"
        );
    }

    public function testCreateInsecureReturnsNullChannelCredentials()
    {
        $tls = TlsCredentials::createInsecure();
        $creds = $tls->toChannelCredentials();

        TestRunner::assertNull(
            $creds,
            "Insecure credentials should return null ChannelCredentials"
        );
    }

    public function testCreateDefaultReturnsSecureCredentials()
    {
        $tls = TlsCredentials::createDefault();

        TestRunner::assertFalse(
            $tls->isInsecure(),
            "createDefault() should return secure credentials"
        );
    }

    public function testCreateDefaultShouldVerifyPeer()
    {
        $tls = TlsCredentials::createDefault();

        TestRunner::assertTrue(
            $tls->shouldVerifyPeer(),
            "createDefault() should have verifyPeer = true"
        );
    }

    public function testCreateInsecureDevShouldNotVerifyPeer()
    {
        $tls = TlsCredentials::createInsecureDev();

        TestRunner::assertFalse(
            $tls->shouldVerifyPeer(),
            "createInsecureDev() should have verifyPeer = false"
        );
    }

    public function testInsecureDevIsSecure()
    {
        $tls = TlsCredentials::createInsecureDev();

        TestRunner::assertFalse(
            $tls->isInsecure(),
            "createInsecureDev() should NOT be insecure (it uses TLS but skips verification)"
        );
    }

    public function testCreateWithCaSetsCaPath()
    {
        $tls = TlsCredentials::createWithCa('/tmp/test-ca.pem');

        TestRunner::assertEquals(
            '/tmp/test-ca.pem',
            $tls->getCaCertPath(),
            "createWithCa should set the CA cert path"
        );
    }

    public function testDefaultCaPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        TestRunner::assertNull(
            $tls->getCaCertPath(),
            "createDefault() should have null CA path (use system CA)"
        );
    }

    public function testInsecureCaPathIsNull()
    {
        $tls = TlsCredentials::createInsecure();

        TestRunner::assertNull(
            $tls->getCaCertPath(),
            "Insecure credentials should have null CA path"
        );
    }

    public function testCreateMtlsSetsClientCertAndKey()
    {
        $tls = TlsCredentials::createMtls('/tmp/client.pem', '/tmp/client-key.pem');

        TestRunner::assertEquals(
            '/tmp/client.pem',
            $tls->getClientCertPath(),
            "createMtls should set client cert path"
        );

        TestRunner::assertEquals(
            '/tmp/client-key.pem',
            $tls->getClientKeyPath(),
            "createMtls should set client key path"
        );
    }

    public function testDefaultClientCertPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        TestRunner::assertNull(
            $tls->getClientCertPath(),
            "createDefault() should have null client cert path"
        );
    }

    public function testDefaultClientKeyPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        TestRunner::assertNull(
            $tls->getClientKeyPath(),
            "createDefault() should have null client key path"
        );
    }
}

echo "=== TlsCredentialsTest ===\n";
TestRunner::run(new TlsCredentialsTest());
