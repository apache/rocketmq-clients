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

use Apache\Rocketmq\Message;
use Apache\Rocketmq\MessageBuilder;
use Apache\Rocketmq\ProducerMessageConverter;
use Apache\Rocketmq\V2\MessageType as V2MessageType;
use PHPUnit\Framework\TestCase;

/**
 * ProducerMessageConverterTest - Test message conversion and type detection
 */
class ProducerMessageConverterTest extends TestCase
{
    private ProducerMessageConverter $converter;

    protected function setUp(): void
    {
        $this->converter = new ProducerMessageConverter();
    }

    /**
     * Test convert normal message to protobuf
     */
    public function testToProtobufMessageNormal()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Hello World')
            ->build();

        $mockQueue = new class {
            public function getId() { return 0; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);

        $this->assertNotNull($protoMsg);
        $this->assertEquals('test-topic', $protoMsg->getTopic()->getName());
        $this->assertEquals('Hello World', $protoMsg->getBody());
        $this->assertTrue($protoMsg->hasSystemProperties());
        $this->assertNotEmpty($protoMsg->getSystemProperties()->getMessageId());
    }

    /**
     * Test convert message with user properties
     */
    public function testToProtobufMessageWithUserProperties()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test Body')
            ->addProperty('key1', 'value1')
            ->addProperty('key2', 'value2')
            ->build();

        $mockQueue = new class {
            public function getId() { return 1; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);

        $userProps = $protoMsg->getUserProperties();
        $this->assertArrayHasKey('key1', $userProps);
        $this->assertEquals('value1', $userProps['key1']);
        $this->assertArrayHasKey('key2', $userProps);
        $this->assertEquals('value2', $userProps['key2']);
    }

    /**
     * Test convert FIFO message with message group
     */
    public function testToProtobufMessageFifo()
    {
        $message = (new MessageBuilder())
            ->setTopic('fifo-topic')
            ->setBody('FIFO Message')
            ->setMessageGroup('order-group-1')
            ->build();

        $mockQueue = new class {
            public function getId() { return 2; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);

        $this->assertEquals('order-group-1', $protoMsg->getSystemProperties()->getMessageGroup());
    }

    /**
     * Test detect NORMAL message type
     */
    public function testDetectMessageTypeNormal()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Normal Message')
            ->build();

        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::NORMAL, $type);
    }

    /**
     * Test detect FIFO message type
     */
    public function testDetectMessageTypeFifo()
    {
        $message = (new MessageBuilder())
            ->setTopic('fifo-topic')
            ->setBody('FIFO Message')
            ->setMessageGroup('group-1')
            ->build();

        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::FIFO, $type);
    }

    /**
     * Test detect DELAY message type
     */
    public function testDetectMessageTypeDelay()
    {
        $deliveryTime = (time() + 3600) * 1000; // milliseconds
        
        $message = (new MessageBuilder())
            ->setTopic('delay-topic')
            ->setBody('Delay Message')
            ->setDeliveryTimestamp($deliveryTime)
            ->build();

        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::DELAY, $type);
    }

    /**
     * Test detect TRANSACTION message type
     */
    public function testDetectMessageTypeTransaction()
    {
        $message = (new MessageBuilder())
            ->setTopic('transaction-topic')
            ->setBody('Transaction Message')
            ->build();

        $type = $this->converter->detectMessageType($message, true);
        $this->assertEquals(V2MessageType::TRANSACTION, $type);
    }

    /**
     * Test detect LITE message type
     */
    public function testDetectMessageTypeLite()
    {
        $message = (new MessageBuilder())
            ->setTopic('parent-topic')
            ->setBody('Lite Message')
            ->setLiteTopic('lite-subtopic')
            ->build();

        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::LITE, $type);
    }

    /**
     * Test detect PRIORITY message type
     */
    public function testDetectMessageTypePriority()
    {
        $message = (new MessageBuilder())
            ->setTopic('priority-topic')
            ->setBody('Priority Message')
            ->setPriority(5)
            ->build();

        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::PRIORITY, $type);
    }

    /**
     * Test message type priority: FIFO > others
     */
    public function testMessageTypePriorityFifoWins()
    {
        $message = (new MessageBuilder())
            ->setTopic('mixed-topic')
            ->setBody('Mixed Message')
            ->setMessageGroup('group-1')
            ->build();

        // FIFO should take priority over DELAY
        $type = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::FIFO, $type);
    }

    /**
     * Test convert message with tag
     */
    public function testToProtobufMessageWithTag()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Message with Tag')
            ->setTag('TagA')
            ->build();

        $mockQueue = new class {
            public function getId() { return 0; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);
        $this->assertEquals('TagA', $protoMsg->getSystemProperties()->getTag());
    }

    /**
     * Test convert message with keys
     */
    public function testToProtobufMessageWithKeys()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Message with Keys')
            ->setKeys(['key1', 'key2', 'key3'])
            ->build();

        $mockQueue = new class {
            public function getId() { return 0; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);
        $keys = $protoMsg->getSystemProperties()->getKeys();
        $this->assertCount(3, $keys);
    }

    /**
     * Test convert message with empty body
     */
    public function testToProtobufMessageEmptyBody()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('')
            ->build();

        $mockQueue = new class {
            public function getId() { return 0; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);
        $this->assertEquals('', $protoMsg->getBody());
    }

    /**
     * Test convert message with binary data
     */
    public function testToProtobufMessageBinaryData()
    {
        $binaryData = random_bytes(1024);
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody($binaryData)
            ->build();

        $mockQueue = new class {
            public function getId() { return 0; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);
        $this->assertEquals($binaryData, $protoMsg->getBody());
    }

    /**
     * Test transaction flag affects message type
     */
    public function testTransactionFlagChangesType()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Message')
            ->build();

        // Without transaction flag -> NORMAL
        $type1 = $this->converter->detectMessageType($message, false);
        $this->assertEquals(V2MessageType::NORMAL, $type1);

        // With transaction flag -> TRANSACTION
        $type2 = $this->converter->detectMessageType($message, true);
        $this->assertEquals(V2MessageType::TRANSACTION, $type2);
    }

    /**
     * Test message with queue ID
     */
    public function testToProtobufMessageWithQueueId()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->build();

        $mockQueue = new class {
            public function getId() { return 42; }
        };

        $protoMsg = $this->converter->toProtobufMessage($message, $mockQueue);
        $this->assertEquals(42, $protoMsg->getSystemProperties()->getQueueId());
    }

    /**
     * Test converter instance creation
     */
    public function testConverterInstanceCreation()
    {
        $converter = new ProducerMessageConverter();
        $this->assertInstanceOf(ProducerMessageConverter::class, $converter);
    }
}
