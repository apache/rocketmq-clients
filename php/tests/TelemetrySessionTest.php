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
use Apache\Rocketmq\SessionCredentials;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Code;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand;
use Apache\Rocketmq\V2\VerifyMessageCommand;
use Apache\Rocketmq\V2\PrintThreadStackTraceCommand;
use Apache\Rocketmq\V2\ReconnectEndpointsCommand;
use Apache\Rocketmq\V2\NotifyUnsubscribeLiteCommand;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;

/**
 * Tests for TelemetrySession internal dispatch and state management.
 * Uses reflection to invoke private handleResponse().
 */
class TelemetrySessionTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
        TelemetrySession::resetAll();
    }

    public function tearDown(): void
    {
        TelemetrySession::resetAll();
    }

    /**
     * Get a TelemetrySession instance for testing.
     */
    private function getSession(?string $clientId = 'test-client'): TelemetrySession
    {
        $fakeClient = new FakeMessagingClientForSession();
        return TelemetrySession::getInstance($fakeClient, 'localhost:8080', $clientId);
    }

    /**
     * Invoke the private handleResponse() method via reflection.
     */
    private function invokeHandleResponse(TelemetrySession $session, $command): void
    {
        $method = new \ReflectionMethod(TelemetrySession::class, 'handleResponse');
        $method->setAccessible(true);
        $method->invoke($session, $command);
    }

    /**
     * Get a private property value via reflection.
     */
    private function getPrivateProperty(TelemetrySession $session, string $property)
    {
        $ref = new \ReflectionProperty(TelemetrySession::class, $property);
        $ref->setAccessible(true);
        return $ref->getValue($session);
    }

    /**
     * Set a private property value via reflection.
     */
    private function setPrivateProperty(TelemetrySession $session, string $property, $value): void
    {
        $ref = new \ReflectionProperty(TelemetrySession::class, $property);
        $ref->setAccessible(true);
        $ref->setValue($session, $value);
    }

    // ========================
    // Settings command dispatch
    // ========================

    /**
     * Test that SETTINGS command sets settingsSynced flag.
     */
    public function testHandleResponseWithSettings()
    {
        $session = $this->getSession('settings-client');

        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $this->invokeHandleResponse($session, $command);

        $this->assertTrue($session->isSettingsSynced(), "Settings command should mark session as synced");
        $this->assertNotNull($session->getServerSettings(), "Server settings should be stored");
    }

    /**
     * Test that SETTINGS command triggers onSettingsChange callback.
     */
    public function testOnSettingsChangeCallbackInvoked()
    {
        $session = $this->getSession('callback-settings');
        $received = null;

        $session->setOnSettingsChange(function ($settings) use (&$received) {
            $received = $settings;
        });

        $settings = new Settings();
        $settings->setClientType(ClientType::PUSH_CONSUMER);
        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($received, "onSettingsChange callback should be invoked");
        $this->assertEquals(ClientType::PUSH_CONSUMER, $received->getClientType());
    }

    /**
     * Test that SETTINGS callback exception is handled gracefully.
     */
    public function testOnSettingsChangeCallbackExceptionHandled()
    {
        $session = $this->getSession('callback-exception');

        $session->setOnSettingsChange(function ($settings) {
            throw new \RuntimeException("Callback error");
        });

        $settings = new Settings();
        $command = new TelemetryCommand();
        $command->setSettings($settings);

        // Should not throw
        $this->invokeHandleResponse($session, $command);

        $this->assertTrue($session->isSettingsSynced(), "Session should still be synced after callback exception");
    }

    // ========================
    // STATUS command dispatch
    // ========================

    /**
     * Test that STATUS command is processed without error.
     */
    public function testHandleResponseWithStatus()
    {
        $session = $this->getSession('status-client');

        $status = new Status();
        $status->setCode(Code::OK);
        $command = new TelemetryCommand();
        $command->setStatus($status);

        // Should not throw
        $this->invokeHandleResponse($session, $command);

        $this->assertFalse($session->isSettingsSynced(), "STATUS command should not set settingsSynced");
    }

    // ========================
    // RecoverOrphanedTransaction command
    // ========================

    /**
     * Test RecoverOrphanedTransactionCommand dispatch.
     */
    public function testOnRecoverOrphanedTransactionCallback()
    {
        $session = $this->getSession('orphan-tx');
        $receivedCmd = null;

        $session->setOnRecoverOrphanedTransaction(function ($cmd) use (&$receivedCmd) {
            $receivedCmd = $cmd;
        });

        $recoverCmd = new RecoverOrphanedTransactionCommand();
        $recoverCmd->setTransactionId('tx-orphan-001');
        $command = new TelemetryCommand();
        $command->setRecoverOrphanedTransactionCommand($recoverCmd);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($receivedCmd, "RecoverOrphanedTransaction callback should be invoked");
        $this->assertEquals('tx-orphan-001', $receivedCmd->getTransactionId());
    }

    /**
     * Test RecoverOrphanedTransaction callback exception is handled gracefully.
     */
    public function testOnRecoverOrphanedTransactionCallbackException()
    {
        $session = $this->getSession('orphan-tx-ex');

        $session->setOnRecoverOrphanedTransaction(function ($cmd) {
            throw new \RuntimeException("Recovery failed");
        });

        $recoverCmd = new RecoverOrphanedTransactionCommand();
        $recoverCmd->setTransactionId('tx-orphan-002');
        $command = new TelemetryCommand();
        $command->setRecoverOrphanedTransactionCommand($recoverCmd);

        // Should not throw
        $this->invokeHandleResponse($session, $command);
        $this->assertTrue(true, "Exception in callback should be handled gracefully");
    }

    // ========================
    // VerifyMessage command
    // ========================

    /**
     * Test VerifyMessageCommand dispatch with response write-back.
     */
    public function testOnVerifyMessageCallback()
    {
        $session = $this->getSession('verify-msg');
        $receivedCmd = null;

        $session->setOnVerifyMessage(function ($cmd) use (&$receivedCmd) {
            $receivedCmd = $cmd;
            // Return a TelemetryCommand as response
            return new TelemetryCommand();
        });

        $verifyCmd = new VerifyMessageCommand();
        $verifyCmd->setNonce('nonce-001');
        $command = new TelemetryCommand();
        $command->setVerifyMessageCommand($verifyCmd);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($receivedCmd, "VerifyMessage callback should be invoked");
        $this->assertEquals('nonce-001', $receivedCmd->getNonce());
    }

    /**
     * Test VerifyMessage callback exception is handled gracefully.
     */
    public function testOnVerifyMessageCallbackException()
    {
        $session = $this->getSession('verify-ex');

        $session->setOnVerifyMessage(function ($cmd) {
            throw new \RuntimeException("Verify failed");
        });

        $verifyCmd = new VerifyMessageCommand();
        $verifyCmd->setNonce('nonce-002');
        $command = new TelemetryCommand();
        $command->setVerifyMessageCommand($verifyCmd);

        // Should not throw
        $this->invokeHandleResponse($session, $command);
        $this->assertTrue(true, "Exception in VerifyMessage callback should be handled gracefully");
    }

    // ========================
    // PrintThreadStackTrace command
    // ========================

    /**
     * Test PrintThreadStackTraceCommand dispatch.
     */
    public function testOnPrintThreadStackTraceCallback()
    {
        $session = $this->getSession('print-stack');
        $receivedCmd = null;

        $session->setOnPrintThreadStackTrace(function ($cmd) use (&$receivedCmd) {
            $receivedCmd = $cmd;
            return new TelemetryCommand();
        });

        $printCmd = new PrintThreadStackTraceCommand();
        $printCmd->setNonce('stack-nonce-001');
        $command = new TelemetryCommand();
        $command->setPrintThreadStackTraceCommand($printCmd);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($receivedCmd, "PrintThreadStackTrace callback should be invoked");
        $this->assertEquals('stack-nonce-001', $receivedCmd->getNonce());
    }

    // ========================
    // ReconnectEndpoints command
    // ========================

    /**
     * Test ReconnectEndpointsCommand dispatch.
     */
    public function testOnReconnectEndpointsCallback()
    {
        $session = $this->getSession('reconnect');
        $receivedCmd = null;

        $session->setOnReconnectEndpoints(function ($cmd) use (&$receivedCmd) {
            $receivedCmd = $cmd;
        });

        $reconnectCmd = new ReconnectEndpointsCommand();
        $reconnectCmd->setNonce('reconnect-nonce-001');
        $command = new TelemetryCommand();
        $command->setReconnectEndpointsCommand($reconnectCmd);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($receivedCmd, "ReconnectEndpoints callback should be invoked");
        $this->assertEquals('reconnect-nonce-001', $receivedCmd->getNonce());
    }

    // ========================
    // NotifyUnsubscribeLite command
    // ========================

    /**
     * Test NotifyUnsubscribeLiteCommand dispatch.
     */
    public function testOnNotifyUnsubscribeLiteCallback()
    {
        $session = $this->getSession('unsubscribe');
        $receivedCmd = null;

        $session->setOnNotifyUnsubscribeLite(function ($cmd) use (&$receivedCmd) {
            $receivedCmd = $cmd;
        });

        $notifyCmd = new NotifyUnsubscribeLiteCommand();
        $notifyCmd->setLiteTopic('lite-topic-001');
        $command = new TelemetryCommand();
        $command->setNotifyUnsubscribeLiteCommand($notifyCmd);

        $this->invokeHandleResponse($session, $command);

        $this->assertNotNull($receivedCmd, "NotifyUnsubscribeLite callback should be invoked");
        $this->assertEquals('lite-topic-001', $receivedCmd->getLiteTopic());
    }

    // ========================
    // Unrecognized command
    // ========================

    /**
     * Test unrecognized command does not throw.
     */
    public function testHandleResponseWithEmptyCommand()
    {
        $session = $this->getSession('empty-cmd');
        $command = new TelemetryCommand();

        // Should not throw
        $this->invokeHandleResponse($session, $command);
        $this->assertTrue(true, "Empty command should be handled gracefully");
    }

    // ========================
    // Singleton pattern
    // ========================

    /**
     * Test singleton returns same instance for same key.
     */
    public function testSingletonSameKey()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $s1 = TelemetrySession::getInstance($fakeClient, 'ep-1', 'client-a');
        $s2 = TelemetrySession::getInstance($fakeClient, 'ep-1', 'client-a');

        $this->assertSame($s1, $s2, "Same key should return same instance");
    }

    /**
     * Test singleton returns different instance for different endpoints.
     */
    public function testSingletonDifferentEndpoints()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $s1 = TelemetrySession::getInstance($fakeClient, 'ep-2', 'client-b');
        $s2 = TelemetrySession::getInstance($fakeClient, 'ep-3', 'client-b');

        $this->assertNotSame($s1, $s2, "Different endpoints should return different instances");
    }

    /**
     * Test singleton returns different instance for different clientIds.
     */
    public function testSingletonDifferentClientIds()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $s1 = TelemetrySession::getInstance($fakeClient, 'ep-4', 'client-x');
        $s2 = TelemetrySession::getInstance($fakeClient, 'ep-4', 'client-y');

        $this->assertNotSame($s1, $s2, "Different clientIds should return different instances");
    }

    /**
     * Test singleton returns different instance for different namespaces.
     */
    public function testSingletonDifferentNamespaces()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $s1 = TelemetrySession::getInstance($fakeClient, 'ep-5', 'client-z', null, 'ns-1');
        $s2 = TelemetrySession::getInstance($fakeClient, 'ep-5', 'client-z', null, 'ns-2');

        $this->assertNotSame($s1, $s2, "Different namespaces should return different instances");
    }

    /**
     * Test singleton returns different instance for different credentials.
     */
    public function testSingletonDifferentCredentials()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $cred1 = new SessionCredentials('ak1', 'sk1');
        $cred2 = new SessionCredentials('ak2', 'sk2');
        $s1 = TelemetrySession::getInstance($fakeClient, 'ep-6', 'client-c', $cred1);
        $s2 = TelemetrySession::getInstance($fakeClient, 'ep-6', 'client-c', $cred2);

        $this->assertNotSame($s1, $s2, "Different credentials should return different instances");
    }

    // ========================
    // MAX_INSTANCES eviction
    // ========================

    /**
     * Test that exceeding MAX_INSTANCES evicts the oldest session.
     */
    public function testMaxInstancesEviction()
    {
        $fakeClient = new FakeMessagingClientForSession();

        // Create 10 sessions (MAX_INSTANCES = 10)
        $sessions = [];
        for ($i = 0; $i < 10; $i++) {
            $sessions[] = TelemetrySession::getInstance($fakeClient, "evict-ep-{$i}", "evict-client-{$i}");
        }

        // 11th session should evict the oldest
        $newSession = TelemetrySession::getInstance($fakeClient, 'evict-ep-new', 'evict-client-new');

        $this->assertNotNull($newSession, "New session should be created after eviction");
    }

    // ========================
    // Stale session eviction
    // ========================

    /**
     * Test that a closed session is evicted and replaced with a new one.
     */
    public function testStaleSessionEvicted()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $session1 = TelemetrySession::getInstance($fakeClient, 'stale-ep', 'stale-client');
        $session1->close();

        $session2 = TelemetrySession::getInstance($fakeClient, 'stale-ep', 'stale-client');

        $this->assertNotSame($session1, $session2, "After close, new getInstance should create fresh instance");
    }

    // ========================
    // ClientId
    // ========================

    /**
     * Test getClientId returns the configured client ID.
     */
    public function testGetClientId()
    {
        $session = $this->getSession('my-client-id');
        $this->assertEquals('my-client-id', $session->getClientId());
    }

    /**
     * Test getClientId when no clientId is provided throws (uninitialized property).
     */
    public function testGetClientIdDefault()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $session = TelemetrySession::getInstance($fakeClient, 'ep-noclient', null);
        $this->expectException(\Error::class);
        $session->getClientId();
    }

    // ========================
    // Initial state
    // ========================

    /**
     * Test initial session state.
     */
    public function testInitialState()
    {
        $session = $this->getSession('init-state');

        $this->assertFalse($session->isSettingsSynced(), "Initial settingsSynced should be false");
        $this->assertNull($session->getSettingsError(), "Initial settingsError should be null");
        $this->assertNull($session->getServerSettings(), "Initial serverSettings should be null");
    }

    // ========================
    // writeSync without stream
    // ========================

    /**
     * Test writeSync returns false when stream is not initialized.
     */
    public function testWriteSyncWithoutStream()
    {
        $session = $this->getSession('no-stream');

        $command = new TelemetryCommand();
        $result = $session->writeSync($command);

        $this->assertFalse($result, "writeSync without stream should return false");
    }

    // ========================
    // resetAll
    // ========================

    /**
     * Test resetAll clears all instances.
     */
    public function testResetAll()
    {
        $fakeClient = new FakeMessagingClientForSession();
        $s1 = TelemetrySession::getInstance($fakeClient, 'reset-ep-1', 'reset-1');
        $s2 = TelemetrySession::getInstance($fakeClient, 'reset-ep-2', 'reset-2');

        TelemetrySession::resetAll();

        // After reset, new getInstance should create fresh instances
        $s3 = TelemetrySession::getInstance($fakeClient, 'reset-ep-1', 'reset-1');
        $this->assertNotSame($s1, $s3, "After resetAll, new instance should be created");
    }

    // ========================
    // Multiple callbacks
    // ========================

    /**
     * Test registering all callbacks simultaneously.
     */
    public function testMultipleCallbacksRegistered()
    {
        $session = $this->getSession('multi-cb');
        $calls = [];

        $session->setOnSettingsChange(function () use (&$calls) { $calls[] = 'settings'; });
        $session->setOnRecoverOrphanedTransaction(function () use (&$calls) { $calls[] = 'orphan'; });
        $session->setOnVerifyMessage(function () use (&$calls) { $calls[] = 'verify'; });
        $session->setOnPrintThreadStackTrace(function () use (&$calls) { $calls[] = 'stack'; });
        $session->setOnReconnectEndpoints(function () use (&$calls) { $calls[] = 'reconnect'; });
        $session->setOnNotifyUnsubscribeLite(function () use (&$calls) { $calls[] = 'unsubscribe'; });

        // Fire settings
        $cmd = new TelemetryCommand();
        $cmd->setSettings(new Settings());
        $this->invokeHandleResponse($session, $cmd);

        // Fire orphan tx
        $cmd2 = new TelemetryCommand();
        $cmd2->setRecoverOrphanedTransactionCommand(new RecoverOrphanedTransactionCommand());
        $this->invokeHandleResponse($session, $cmd2);

        // Fire verify
        $cmd3 = new TelemetryCommand();
        $cmd3->setVerifyMessageCommand(new VerifyMessageCommand());
        $this->invokeHandleResponse($session, $cmd3);

        $this->assertContains('settings', $calls, "Settings callback should fire");
        $this->assertContains('orphan', $calls, "Orphan tx callback should fire");
        $this->assertContains('verify', $calls, "Verify callback should fire");
    }
}

/**
 * Fake gRPC client for session tests.
 */
class FakeMessagingClientForSession
{
    public function Telemetry($metadata = [])
    {
        return new FakeStreamForSession();
    }
}

/**
 * Fake telemetry stream for session tests.
 */
class FakeStreamForSession
{
    public function write($command) { return true; }
    public function flush() {}
    public function read() { return null; }
    public function cancel() {}
    public function writesDone() {}
    public function getStatus() { return null; }
}
