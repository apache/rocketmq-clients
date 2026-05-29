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

require_once __DIR__ . '/../TlsCredentials.php';

use Apache\Rocketmq\TlsCredentials;

/**
 * Tests for TlsCredentials class.
 */
class TlsCredentialsTest extends TestCase
{
    public function testCreateInsecureReturnsInsecureCredentials()
    {
        $tls = TlsCredentials::createInsecure();

        $this->assertTrue(
            $tls->isInsecure(),
            "createInsecure() should return insecure credentials"
        );
    }

    public function testCreateInsecureReturnsNullChannelCredentials()
    {
        $tls = TlsCredentials::createInsecure();
        $creds = $tls->toChannelCredentials();

        $this->assertNull(
            $creds,
            "Insecure credentials should return null ChannelCredentials"
        );
    }

    public function testCreateDefaultReturnsSecureCredentials()
    {
        $tls = TlsCredentials::createDefault();

        $this->assertFalse(
            $tls->isInsecure(),
            "createDefault() should return secure credentials"
        );
    }

    public function testCreateDefaultShouldVerifyPeer()
    {
        $tls = TlsCredentials::createDefault();

        $this->assertTrue(
            $tls->shouldVerifyPeer(),
            "createDefault() should have verifyPeer = true"
        );
    }

    public function testCreateInsecureDevShouldNotVerifyPeer()
    {
        // Expect E_USER_WARNING trigger_error
        $warningTriggered = false;
        set_error_handler(function($errno, $errstr) use (&$warningTriggered) {
            if ($errno === E_USER_WARNING && strpos($errstr, 'SECURITY WARNING') !== false) {
                $warningTriggered = true;
                return true; // Prevent default error handler
            }
            return false;
        });

        try {
            $tls = TlsCredentials::createInsecureDev();

            $this->assertFalse(
                $tls->shouldVerifyPeer(),
                "createInsecureDev() should have verifyPeer = false"
            );
            
            $this->assertTrue(
                $warningTriggered,
                "createInsecureDev() should trigger a security warning"
            );
        } finally {
            restore_error_handler();
        }
    }

    public function testInsecureDevIsSecure()
    {
        // Expect E_USER_WARNING trigger_error
        set_error_handler(function($errno, $errstr) {
            if ($errno === E_USER_WARNING && strpos($errstr, 'SECURITY WARNING') !== false) {
                return true; // Prevent default error handler
            }
            return false;
        });

        try {
            $tls = TlsCredentials::createInsecureDev();

            $this->assertFalse(
                $tls->isInsecure(),
                "createInsecureDev() should NOT be insecure (it uses TLS but skips verification)"
            );
        } finally {
            restore_error_handler();
        }
    }

    public function testCreateWithCaSetsCaPath()
    {
        $tls = TlsCredentials::createWithCa('/tmp/test-ca.pem');

        $this->assertEquals(
            '/tmp/test-ca.pem',
            $tls->getCaCertPath(),
            "createWithCa should set the CA cert path"
        );
    }

    public function testDefaultCaPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        $this->assertNull(
            $tls->getCaCertPath(),
            "createDefault() should have null CA path (use system CA)"
        );
    }

    public function testInsecureCaPathIsNull()
    {
        $tls = TlsCredentials::createInsecure();

        $this->assertNull(
            $tls->getCaCertPath(),
            "Insecure credentials should have null CA path"
        );
    }

    public function testCreateMtlsSetsClientCertAndKey()
    {
        $tls = TlsCredentials::createMtls('/tmp/client.pem', '/tmp/client-key.pem');

        $this->assertEquals(
            '/tmp/client.pem',
            $tls->getClientCertPath(),
            "createMtls should set client cert path"
        );

        $this->assertEquals(
            '/tmp/client-key.pem',
            $tls->getClientKeyPath(),
            "createMtls should set client key path"
        );
    }

    public function testDefaultClientCertPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        $this->assertNull(
            $tls->getClientCertPath(),
            "createDefault() should have null client cert path"
        );
    }

    public function testDefaultClientKeyPathIsNull()
    {
        $tls = TlsCredentials::createDefault();

        $this->assertNull(
            $tls->getClientKeyPath(),
            "createDefault() should have null client key path"
        );
    }
}
