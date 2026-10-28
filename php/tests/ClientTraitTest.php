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
require_once __DIR__ . '/../ClientTrait.php';
require_once __DIR__ . '/../autoload.php';

use Apache\Rocketmq\MessageViewInterface;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

/**
 * Fake message view for ClientTrait extraction tests.
 */
class FakeMessageViewForTrait implements MessageViewInterface {
    private $systemProperties;
    private $topic;

    public function __construct(?SystemProperties $sysProps = null, ?string $topicName = null)
    {
        $this->systemProperties = $sysProps;
        if ($topicName !== null) {
            $resource = new Resource();
            $resource->setName($topicName);
            $this->topic = $resource;
        }
    }

    public function getSystemProperties(): ?object
    {
        return $this->systemProperties;
    }

    public function getMessageId(): string
    {
        return $this->systemProperties?->getMessageId() ?? '';
    }

    public function getTopic(): string
    {
        return $this->topic?->getName() ?? '';
    }

    public function getDeliveryAttempt(): int { return 1; }
    public function incrementDeliveryAttempt(): void {}
    public function isCorrupted(): bool { return false; }
    public function getEndpoints(): ?object { return null; }
}

/**
 * Concrete class that uses ClientTrait to test extract methods.
 */
class ClientTraitExtractor {
    use \Apache\Rocketmq\ClientTrait;

    protected function getCredentials(): ?\Apache\Rocketmq\SessionCredentials
    {
        return null;
    }

    protected function getClientIdValue(): string
    {
        return 'test-client-id';
    }

    protected function getNamespaceValue(): string
    {
        return '';
    }

    public function testExtractReceiptHandle($messageView): ?string
    {
        return $this->extractReceiptHandle($messageView);
    }

    public function testExtractMessageId($messageView): ?string
    {
        return $this->extractMessageId($messageView);
    }

    public function testExtractTopic($messageView): ?string
    {
        return $this->extractTopic($messageView);
    }
}

/**
 * Tests for ClientTrait extraction methods.
 * Mirrors Java's extractReceiptHandle, extractMessageId, extractTopic tests.
 */
class ClientTraitTest extends TestCase
{
    private $extractor;

    public function setUp(): void
    {
        $this->extractor = new ClientTraitExtractor();
    }

    public function testExtractReceiptHandleFromMessageView()
    {
        $sysProps = new SystemProperties();
        $sysProps->setReceiptHandle('test-receipt-handle-123');

        $messageView = new FakeMessageViewForTrait($sysProps);
        $handle = $this->extractor->testExtractReceiptHandle($messageView);

        $this->assertEquals(
            'test-receipt-handle-123',
            $handle,
            "Should extract receipt handle from message view"
        );
    }

    public function testExtractReceiptHandleReturnsNullForNullSysProps()
    {
        // No sysProps - getSystemProperties returns null
        $messageView = new FakeMessageViewForTrait();
        $handle = $this->extractor->testExtractReceiptHandle($messageView);

        $this->assertNull($handle, "Should return null when no system properties");
    }

    public function testExtractReceiptHandleReturnsEmptyForUnsetHandle()
    {
        // sysProps exists but no receipt handle set - protobuf returns ''
        $sysProps = new SystemProperties();
        $messageView = new FakeMessageViewForTrait($sysProps);
        $handle = $this->extractor->testExtractReceiptHandle($messageView);

        $this->assertEquals('', $handle, "Should return empty string when sysProps has no receipt handle");
    }

    public function testExtractMessageIdFromMessageView()
    {
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-id-abc-123');

        $messageView = new FakeMessageViewForTrait($sysProps);
        $messageId = $this->extractor->testExtractMessageId($messageView);

        $this->assertEquals(
            'msg-id-abc-123',
            $messageId,
            "Should extract message ID from message view"
        );
    }

    public function testExtractMessageIdReturnsEmptyForMissing()
    {
        // sysProps exists but no messageId set - protobuf returns ''
        $sysProps = new SystemProperties();
        $messageView = new FakeMessageViewForTrait($sysProps);
        $messageId = $this->extractor->testExtractMessageId($messageView);

        $this->assertEquals('', $messageId, "Should return empty string when sysProps has no message ID");
    }

    public function testExtractTopicFromMessageView()
    {
        $messageView = new FakeMessageViewForTrait(null, 'test-topic-name');
        $topic = $this->extractor->testExtractTopic($messageView);

        $this->assertEquals(
            'test-topic-name',
            $topic,
            "Should extract topic name from message view"
        );
    }

    public function testExtractTopicReturnsNullForMissingTopic()
    {
        $messageView = new FakeMessageViewForTrait();
        $topic = $this->extractor->testExtractTopic($messageView);

        $this->assertNull($topic, "Should return null when no topic");
    }
}

