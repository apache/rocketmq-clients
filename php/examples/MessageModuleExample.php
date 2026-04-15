<?php
/**
 * Message Module Usage Example
 * 
 * This example demonstrates how to use the Message module for creating and handling messages.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\Message\MessageBuilder;
use Apache\Rocketmq\Message\MessageViewImpl;
use Apache\Rocketmq\Message\MessageIdCodec;
use Apache\Rocketmq\Message\MessageType;
use Apache\Rocketmq\Message\GeneralMessageImpl;
use Apache\Rocketmq\Message\PublishingMessage;

echo "=== Message Module Usage Example ===\n\n";

// 1. Create Message ID
echo "1. Creating Message ID...\n";
$codec = MessageIdCodec::getInstance();
$messageId = $codec->nextMessageId();
echo "   Message ID: {$messageId}\n";
echo "   Version: {$messageId->getVersion()}\n\n";

// 2. Build a message using MessageBuilder
echo "2. Building a message...\n";
// Note: You would use your actual MessageBuilder implementation
// $builder = new MessageBuilderImpl();
// $message = $builder->setTopic('test-topic')
//     ->setBody('Hello, RocketMQ!')
//     ->setTag('order')
//     ->setKeys('order-123')
//     ->setMessageGroup('group-1')
//     ->build();
echo "   Message builder pattern for creating messages\n";
echo "   - setTopic(): Set message topic\n";
echo "   - setBody(): Set message body\n";
echo "   - setTag(): Set message tag\n";
echo "   - setKeys(): Set message keys\n";
echo "   - setMessageGroup(): Set for FIFO messages\n";
echo "   - setDeliveryTimestamp(): Set for delay messages\n";
echo "   - setPriority(): Set for priority messages\n\n";

// 3. Message Types
echo "3. Message Types:\n";
echo "   - NORMAL: " . MessageType::getName(MessageType::NORMAL) . "\n";
echo "   - FIFO: " . MessageType::getName(MessageType::FIFO) . "\n";
echo "   - DELAY: " . MessageType::getName(MessageType::DELAY) . "\n";
echo "   - PRIORITY: " . MessageType::getName(MessageType::PRIORITY) . "\n";
echo "   - TRANSACTION: " . MessageType::getName(MessageType::TRANSACTION) . "\n";
echo "   - LITE: " . MessageType::getName(MessageType::LITE) . "\n\n";

// 4. PublishingMessage (for sending)
echo "4. PublishingMessage features:\n";
echo "   - Validates message before sending\n";
echo "   - Determines message type automatically\n";
echo "   - Checks body size limit (4MB)\n";
echo "   - Validates FIFO message group\n";
echo "   - Validates delay timestamp\n\n";

// 5. MessageViewImpl (for receiving)
echo "5. MessageViewImpl features:\n";
echo "   - Complete message view from Protobuf\n";
echo "   - Body digest verification (CRC32/MD5/SHA1)\n";
echo "   - Body decompression (GZIP/IDENTITY)\n";
echo "   - Receipt handle management\n";
echo "   - Delivery attempt tracking\n\n";

// 6. GeneralMessage
echo "6. GeneralMessage usage:\n";
echo "   - Combines Message and MessageView interfaces\n";
echo "   - Used by message interceptors\n";
echo "   - Handles both sending and receiving\n\n";

// 7. Message ID Codec
echo "7. Message ID format (V1):\n";
echo "   - Total: 34 characters (17 bytes in hex)\n";
echo "   - Version: 2 chars (01)\n";
echo "   - MAC address: 12 chars\n";
echo "   - Process ID: 4 chars\n";
echo "   - Timestamp: 8 chars (since 2021-01-01)\n";
echo "   - Sequence: 8 chars\n\n";

echo "=== Example Complete ===\n";
