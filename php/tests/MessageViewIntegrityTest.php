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
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\DigestType;
use Apache\Rocketmq\V2\Resource;

/**
 * Tests for MessageView body integrity verification and GZIP decompression.
 * Mirrors Java's MessageViewImplTest.
 */
class MessageViewIntegrityTest
{
    /**
     * Mirrors Java: testFromProtobufWithCrc32
     * MessageView with correct CRC32 digest should NOT be corrupted.
     */
    public function testCorrectCrc32Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';
        $crc32 = sprintf('%u', crc32($body));

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::CRC32, $crc32);
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage($body, $view->getBody(),
            "Body should match original");
        TestRunner::assertFalse($view->isCorrupted(),
            "Message should NOT be corrupted with correct CRC32");
    }

    /**
     * Mirrors Java: testFromProtobufWithWrongCrc32
     * MessageView with incorrect CRC32 should be marked as corrupted.
     */
    public function testWrongCrc32Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::CRC32, '9EF61F96');
        $view = new MessageView($message);

        TestRunner::assertTrue($view->isCorrupted(),
            "Message should be corrupted with wrong CRC32");
    }

    /**
     * Mirrors Java: testFromProtobufWithMd5
     * MessageView with correct MD5 digest should NOT be corrupted.
     */
    public function testCorrectMd5Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';
        $md5 = strtoupper(md5($body));

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::MD5, $md5);
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage($body, $view->getBody(),
            "Body should match original");
        TestRunner::assertFalse($view->isCorrupted(),
            "Message should NOT be corrupted with correct MD5");
    }

    /**
     * Mirrors Java: testFromProtobufWithWrongMd5
     * MessageView with incorrect MD5 should be corrupted.
     */
    public function testWrongMd5Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::MD5, '3858F62230AC3C915F300C664312C63G');
        $view = new MessageView($message);

        TestRunner::assertTrue($view->isCorrupted(),
            "Message should be corrupted with wrong MD5");
    }

    /**
     * Mirrors Java: testFromProtobufWithSha1
     * MessageView with correct SHA1 digest should NOT be corrupted.
     */
    public function testCorrectSha1Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';
        $sha1 = strtoupper(sha1($body));

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::SHA1, $sha1);
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage($body, $view->getBody(),
            "Body should match original");
        TestRunner::assertFalse($view->isCorrupted(),
            "Message should NOT be corrupted with correct SHA1");
    }

    /**
     * Mirrors Java: testFromProtobufWithWrongSha1
     * MessageView with incorrect SHA1 should be corrupted.
     */
    public function testWrongSha1Digest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'foobar';

        $message = $this->buildMessage($body, Encoding::IDENTITY, DigestType::SHA1, '8843D7F92416211DE9EBB963FF4CE28125932879');
        $view = new MessageView($message);

        TestRunner::assertTrue($view->isCorrupted(),
            "Message should be corrupted with wrong SHA1");
    }

    /**
     * Tests GZIP compressed body with correct CRC32.
     */
    public function testGzipBodyWithCorrectCrc32()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'hello world';
        $compressed = gzencode($body);
        $crc32 = sprintf('%u', crc32($body)); // CRC is computed on decompressed body

        $message = $this->buildMessage($compressed, Encoding::GZIP, DigestType::CRC32, $crc32);
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage($body, $view->getBody(),
            "Body should be decompressed correctly");
        TestRunner::assertFalse($view->isCorrupted(),
            "GZIP message should NOT be corrupted with correct CRC32");
    }

    /**
     * Tests GZIP compressed body with wrong CRC32.
     */
    public function testGzipBodyWithWrongCrc32()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'hello world';
        $compressed = gzencode($body);

        $message = $this->buildMessage($compressed, Encoding::GZIP, DigestType::CRC32, 'WRONG_CRC32');
        $view = new MessageView($message);

        TestRunner::assertTrue($view->isCorrupted(),
            "GZIP message should be corrupted with wrong CRC32");
    }

    /**
     * Tests empty body should not be corrupted.
     */
    public function testEmptyBody()
    {
        \Apache\Rocketmq\Logger::close();

        $message = $this->buildMessage('', Encoding::IDENTITY, null, '');
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage('', $view->getBody(),
            "Body should be empty");
        TestRunner::assertFalse($view->isCorrupted(),
            "Empty body message should NOT be corrupted");
    }

    /**
     * Tests message with no digest should not be corrupted.
     */
    public function testNoDigest()
    {
        \Apache\Rocketmq\Logger::close();

        $body = 'test data';
        $message = $this->buildMessage($body, Encoding::IDENTITY, null, null);
        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage($body, $view->getBody(),
            "Body should match original");
        TestRunner::assertFalse($view->isCorrupted(),
            "Message without digest should NOT be corrupted");
    }

    /**
     * Helper to build a protobuf Message with body and digest.
     */
    private function buildMessage($body, $encoding, $digestType, $digestValue)
    {
        $message = new Message();
        $message->setBody($body);

        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);

        $sysProps = new SystemProperties();
        $sysProps->setMessageId('test-msg-id');
        $sysProps->setBodyEncoding($encoding);

        if ($digestType !== null && $digestValue !== null) {
            $digest = new \Apache\Rocketmq\V2\Digest();
            $digest->setType($digestType);
            $digest->setChecksum($digestValue);
            $sysProps->setBodyDigest($digest);
        }

        $message->setSystemProperties($sysProps);
        return $message;
    }
}

echo "=== MessageViewIntegrityTest ===\n";
$test = new MessageViewIntegrityTest();
$test->testCorrectCrc32Digest();
echo "  [OK] testCorrectCrc32Digest\n";
$test->testWrongCrc32Digest();
echo "  [OK] testWrongCrc32Digest\n";
$test->testCorrectMd5Digest();
echo "  [OK] testCorrectMd5Digest\n";
$test->testWrongMd5Digest();
echo "  [OK] testWrongMd5Digest\n";
$test->testCorrectSha1Digest();
echo "  [OK] testCorrectSha1Digest\n";
$test->testWrongSha1Digest();
echo "  [OK] testWrongSha1Digest\n";
$test->testGzipBodyWithCorrectCrc32();
echo "  [OK] testGzipBodyWithCorrectCrc32\n";
$test->testGzipBodyWithWrongCrc32();
echo "  [OK] testGzipBodyWithWrongCrc32\n";
$test->testEmptyBody();
echo "  [OK] testEmptyBody\n";
$test->testNoDigest();
echo "  [OK] testNoDigest\n";
