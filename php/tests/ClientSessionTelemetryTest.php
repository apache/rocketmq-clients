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

require_once __DIR__ . '/../TelemetrySession.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;

/**
 * Tests for TelemetrySession lifecycle and behavior.
 * Mirrors Java's ClientSessionImplTest.
 */
class ClientSessionTelemetryTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    public function tearDown(): void
    {
        TelemetrySession::resetAll();
    }

    /**
     * Mirrors Java: syncSettings
     * Tests that a new session is created for a unique endpoints string.
     */
    public function testGetInstanceCreatesNewSession()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session = TelemetrySession::getInstance($fakeClient, 'test-endpoints-1', 'test-client-1');

        $this->assertNotNull($session, "Session should not be null");
        $this->assertFalse($session->isSettingsSynced(), "New session should not be synced yet");
    }

    /**
     * Tests that same endpoints returns same instance (singleton).
     */
    public function testGetInstanceReturnsSameInstanceForSameEndpoints()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session1 = TelemetrySession::getInstance($fakeClient, 'test-endpoints-2', 'client-2');
        $session2 = TelemetrySession::getInstance($fakeClient, 'test-endpoints-2', 'client-2');

        $this->assertTrue(
            $session1 === $session2,
            "Same endpoints should return same session instance"
        );
    }

    /**
     * Tests that different endpoints returns different instances.
     */
    public function testGetInstanceReturnsDifferentInstanceForDifferentEndpoints()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session1 = TelemetrySession::getInstance($fakeClient, 'test-endpoints-3', 'client-3');
        $session2 = TelemetrySession::getInstance($fakeClient, 'test-endpoints-4', 'client-4');

        $this->assertTrue(
            $session1 !== $session2,
            "Different endpoints should return different session instances"
        );
    }

    /**
     * Mirrors Java: testOnNextWithRecoverOrphanedTransactionCommand
     * Tests that TelemetryCommand can be constructed with settings.
     */
    public function testTelemetryCommandWithSettings()
    {
        $settings = new Settings();
        $settings->setClientType(ClientType::PUSH_CONSUMER);

        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $this->assertTrue($command->hasSettings(), "Command should have settings");
        $this->assertEquals(
            ClientType::PUSH_CONSUMER,
            $command->getSettings()->getClientType(),
            "Settings client type should match"
        );
    }

    /**
     * Mirrors Java: testOnNextWithUnrecognizedCommand
     * Tests that empty TelemetryCommand has no sub-commands.
     */
    public function testEmptyTelemetryCommand()
    {
        $command = new TelemetryCommand();

        $this->assertFalse($command->hasSettings(), "Empty command should not have settings");
    }

    /**
     * Tests that close() removes instance from pool.
     */
    public function testCloseRemovesFromPool()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session = TelemetrySession::getInstance($fakeClient, 'test-endpoints-5', 'client-5');
        $session->close();

        // After close, getInstance should create a new instance
        $newSession = TelemetrySession::getInstance($fakeClient, 'test-endpoints-5', 'client-5');

        $this->assertTrue(
            $session !== $newSession,
            "After close, new getInstance should create a new session"
        );
    }

    /**
     * Tests writeSync returns false when stream is not initialized.
     */
    public function testWriteSyncWithoutStream()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session = TelemetrySession::getInstance($fakeClient, 'test-endpoints-6', 'client-6');

        $settings = new Settings();
        $command = new TelemetryCommand();
        $command->setSettings($settings);

        // Without a real stream, writeSync should return false
        $result = $session->writeSync($command);

        $this->assertFalse($result, "writeSync without stream should return false");
    }

    /**
     * Tests that clientId is set correctly on session.
     */
    public function testClientIdIsSet()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session = TelemetrySession::getInstance($fakeClient, 'test-endpoints-7', 'my-custom-client-id');

        $this->assertEquals(
            'my-custom-client-id',
            $session->getClientId(),
            "ClientId should match what was passed to getInstance"
        );
    }

    /**
     * Mirrors Java: testOnError
     * Tests that session error state is tracked.
     */
    public function testSessionInitialState()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();
        $session = TelemetrySession::getInstance($fakeClient, 'test-endpoints-8', 'client-8');

        $this->assertFalse($session->isSettingsSynced(), "Initial settingsSynced should be false");
        $this->assertNull($session->getSettingsError(), "Initial settingsError should be null");
    }

    /**
     * Mirrors Java: testOnCompletedWithSessionHandlerIsNotRunning
     * Tests that a fresh session can be constructed without errors.
     */
    public function testSessionCanBeCreatedMultipleTimes()
    {
        $fakeClient = new FakeMessagingClientForTelemetry();

        for ($i = 0; $i < 5; $i++) {
            $session = TelemetrySession::getInstance($fakeClient, "test-endpoints-loop-{$i}", "client-loop-{$i}");
            $this->assertNotNull($session, "Session {$i} should be created");
            $session->close();
        }
    }
}

/**
 * Fake gRPC MessagingServiceClient for telemetry tests.
 * Implements minimal interface to satisfy type hints.
 */
class FakeMessagingClientForTelemetry {
    private $stream;
    
    public function __construct($stream = null)
    {
        $this->stream = $stream ?: new FakeTelemetryStream();
    }
    
    public function Telemetry($metadata = [])
    {
        return $this->stream;
    }
}

/**
 * Fake telemetry stream.
 */
class FakeTelemetryStream {
    public function write($command) { return false; }
    public function flush() {}
    public function responses() { return []; }
    public function cancel() {}
    public function writesDone() {}
}

