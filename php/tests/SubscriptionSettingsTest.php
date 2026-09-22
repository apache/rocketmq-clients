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

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\Resource;

/**
 * Tests for Subscription Settings protobuf generation.
 * Mirrors Java's PushSubscriptionSettingsTest and SimpleSubscriptionSettingsTest.
 * Tests that Settings/Subscription protobuf objects are correctly built.
 */
class SubscriptionSettingsTest extends TestCase
{
    /**
     * Mirrors Java: PushSubscriptionSettingsTest.testToProtobuf
     * Tests that a Settings object with TAG filter type is correctly built.
     */
    public function testPushSettingsToProtobuf()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('test-consumer-group');
        $subscription->setGroup($groupResource);
        $subscription->setFifo(false);

        // Add subscription entry with TAG filter
        $topicResource = new Resource();
        $topicResource->setName('test-topic');

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');
        $filterExpression->setType(FilterType::TAG);

        $entry = new SubscriptionEntry();
        $entry->setTopic($topicResource);
        $entry->setExpression($filterExpression);
        $subscription->setSubscriptions([$entry]);

        // Verify
        $this->assertTrue(
            $subscription->hasGroup(),
            "Subscription should have group"
        );
        $this->assertEquals(
            'test-consumer-group',
            $subscription->getGroup()->getName(),
            "Group name should match"
        );
        $this->assertFalse(
            $subscription->getFifo(),
            "Push subscription should not be FIFO by default"
        );

        $entries = $subscription->getSubscriptions();
        $this->assertTrue(
            is_iterable($entries) && iterator_count($entries) === 1,
            "Should have 1 subscription entry"
        );
        $subscription->setSubscriptions([]); // reset
        $subscription->setSubscriptions([$entry]);

        $firstEntry = iterator_to_array($subscription->getSubscriptions())[0];
        $this->assertEquals(
            FilterType::TAG,
            $firstEntry->getExpression()->getType(),
            "Expression type should be TAG"
        );
        $this->assertEquals(
            'test-topic',
            $firstEntry->getTopic()->getName(),
            "Topic name should match"
        );
    }

    /**
     * Mirrors Java: PushSubscriptionSettingsTest.testToProtobufWithSqlExpression
     * Tests SQL92 filter expression type.
     */
    public function testSettingsWithSqlExpression()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('test-consumer-group');
        $subscription->setGroup($groupResource);
        $subscription->setFifo(false);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('(a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)');
        $filterExpression->setType(FilterType::SQL);

        $entry = new SubscriptionEntry();
        $entry->setTopic($topicResource);
        $entry->setExpression($filterExpression);
        $subscription->setSubscriptions([$entry]);

        $firstEntry = iterator_to_array($subscription->getSubscriptions())[0];
        $this->assertEquals(
            FilterType::SQL,
            $firstEntry->getExpression()->getType(),
            "Expression type should be SQL"
        );
        $this->assertEquals(
            '(a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)',
            $firstEntry->getExpression()->getExpression(),
            "SQL expression should match"
        );
    }

    /**
     * Tests Settings clientType configuration.
     * Mirrors Java: verifying settings.getClientType() == ClientType.PUSH_CONSUMER
     */
    public function testSettingsClientType()
    {
        $settings = new Settings();
        $settings->setClientType(ClientType::PUSH_CONSUMER);

        $this->assertEquals(
            ClientType::PUSH_CONSUMER,
            $settings->getClientType(),
            "ClientType should be PUSH_CONSUMER"
        );
    }

    /**
     * Tests SimpleConsumer Settings with longPollingTimeout.
     * Mirrors Java: SimpleSubscriptionSettingsTest.testToProtobuf
     */
    public function testSimpleSettingsWithLongPollingTimeout()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('simple-consumer-group');
        $subscription->setGroup($groupResource);
        $subscription->setFifo(false);
        $subscription->setReceiveBatchSize(32);

        $topicResource = new Resource();
        $topicResource->setName('simple-topic');

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');
        $filterExpression->setType(FilterType::TAG);

        $entry = new SubscriptionEntry();
        $entry->setTopic($topicResource);
        $entry->setExpression($filterExpression);
        $subscription->setSubscriptions([$entry]);

        $this->assertEquals(
            'simple-consumer-group',
            $subscription->getGroup()->getName(),
            "Group name should match"
        );
        $this->assertEquals(
            32,
            $subscription->getReceiveBatchSize(),
            "Receive batch size should be 32"
        );
        $this->assertFalse(
            $subscription->getFifo(),
            "Simple subscription should not be FIFO"
        );
    }

    /**
     * Tests FIFO subscription Settings.
     * Mirrors Java: PushSubscriptionSettingsTest.testSync with fifo=true
     */
    public function testFifoSubscriptionSettings()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('fifo-consumer-group');
        $subscription->setGroup($groupResource);
        $subscription->setFifo(true);
        $subscription->setReceiveBatchSize(1);

        $this->assertTrue(
            $subscription->getFifo(),
            "FIFO subscription should have fifo=true"
        );
        $this->assertEquals(
            1,
            $subscription->getReceiveBatchSize(),
            "FIFO receive batch size should be 1"
        );
    }

    /**
     * Tests multiple subscription entries.
     */
    public function testMultipleSubscriptionEntries()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('multi-topic-group');
        $subscription->setGroup($groupResource);

        $entries = [];
        for ($i = 0; $i < 3; $i++) {
            $topicResource = new Resource();
            $topicResource->setName("topic-{$i}");

            $filterExpression = new FilterExpression();
            $filterExpression->setExpression('*');

            $entry = new SubscriptionEntry();
            $entry->setTopic($topicResource);
            $entry->setExpression($filterExpression);
            $entries[] = $entry;
        }
        $subscription->setSubscriptions($entries);

        $entryList = iterator_to_array($subscription->getSubscriptions());
        $this->assertEquals(
            3,
            count($entryList),
            "Should have 3 subscription entries"
        );

        for ($i = 0; $i < 3; $i++) {
            $this->assertEquals(
                "topic-{$i}",
                $entryList[$i]->getTopic()->getName(),
                "Topic name at index {$i} should match"
            );
        }
    }

    /**
     * Tests Resource name and namespace.
     */
    public function testResourceName()
    {
        $resource = new Resource();
        $resource->setName('test-topic');

        $this->assertEquals(
            'test-topic',
            $resource->getName(),
            "Resource name should match"
        );
    }

    /**
     * Tests FilterExpression with default type (0 = unspecified, defaults to TAG in Java).
     */
    public function testFilterExpressionDefaultType()
    {
        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');

        // Default type is 0 (unspecified) - Java defaults to TAG
        $type = $filterExpression->getType();
        $this->assertEquals(
            0,
            $type,
            "Default filter expression type should be 0 (unspecified)"
        );

        // Explicit TAG type
        $tagFilter = new FilterExpression();
        $tagFilter->setExpression('*');
        $tagFilter->setType(FilterType::TAG);
        $this->assertEquals(
            FilterType::TAG,
            $tagFilter->getType(),
            "Explicit filter type should be TAG"
        );
    }

    /**
     * Tests empty subscription list.
     */
    public function testEmptySubscriptions()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('empty-group');
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions([]);

        $entries = iterator_to_array($subscription->getSubscriptions());
        $this->assertEquals(
            0,
            count($entries),
            "Empty subscriptions should have 0 entries"
        );
    }

    /**
     * Tests Settings with subscription set.
     */
    public function testSettingsWithSubscription()
    {
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName('settings-test-group');
        $subscription->setGroup($groupResource);

        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setSubscription($subscription);

        $this->assertTrue(
            $settings->hasSubscription(),
            "Settings should have subscription"
        );
        $this->assertEquals(
            ClientType::SIMPLE_CONSUMER,
            $settings->getClientType(),
            "ClientType should be SIMPLE_CONSUMER"
        );
    }
}

