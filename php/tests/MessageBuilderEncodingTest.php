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
require_once __DIR__ . '/../MessageBuilder.php';
require_once __DIR__ . '/../Utilities.php';

use Apache\Rocketmq\MessageBuilder;
use Apache\Rocketmq\Utilities;

/**
 * Tests for MessageBuilder::setEncoding compression integration.
 */
class MessageBuilderEncodingTest
{
    private $topic = 'test_topic';
    private $body = 'Hello, RocketMQ!';

    public function testBuildWithoutEncodingReturnsPlainBody()
    {
        $message = (new MessageBuilder())
            ->setTopic($this->topic)
            ->setBody($this->body)
            ->build();

        TestRunner::assertEquals(
            $this->body,
            $message->getBody(),
            "Message body should be unchanged when no encoding is set"
        );
    }

    public function testBuildWithGzipEncodingCompressesBody()
    {
        $message = (new MessageBuilder())
            ->setTopic($this->topic)
            ->setBody($this->body)
            ->setEncoding(Utilities::ENCODING_GZIP_STR)
            ->build();

        $encodedBody = $message->getBody();
        TestRunner::assertTrue(
            $encodedBody !== $this->body,
            "GZIP-encoded body should differ from original"
        );

        $decompressed = Utilities::decompressBytes($encodedBody, Utilities::ENCODING_GZIP);
        TestRunner::assertEquals(
            $this->body,
            $decompressed,
            "GZIP-encoded body should decompress back to original"
        );
    }

    public function testBuildWithZlibEncodingCompressesBody()
    {
        $message = (new MessageBuilder())
            ->setTopic($this->topic)
            ->setBody($this->body)
            ->setEncoding(Utilities::ENCODING_ZLIB_STR)
            ->build();

        $encodedBody = $message->getBody();
        TestRunner::assertTrue(
            $encodedBody !== $this->body,
            "ZLIB-encoded body should differ from original"
        );

        $decompressed = Utilities::decompressBytes($encodedBody, Utilities::ENCODING_ZLIB);
        TestRunner::assertEquals(
            $this->body,
            $decompressed,
            "ZLIB-encoded body should decompress back to original"
        );
    }

    public function testBuildWithIdentityEncodingReturnsPlainBody()
    {
        $message = (new MessageBuilder())
            ->setTopic($this->topic)
            ->setBody($this->body)
            ->setEncoding(Utilities::ENCODING_IDENTITY_STR)
            ->build();

        TestRunner::assertEquals(
            $this->body,
            $message->getBody(),
            "IDENTITY encoding should leave body unchanged"
        );
    }

    public function testSetEncodingReturnsFluentBuilder()
    {
        $builder = new MessageBuilder();
        $result = $builder->setTopic($this->topic)
            ->setBody($this->body)
            ->setEncoding(Utilities::ENCODING_GZIP_STR);

        TestRunner::assertTrue(
            $result instanceof MessageBuilder,
            "setEncoding should return the builder for chaining"
        );
    }
}

echo "=== MessageBuilderEncodingTest ===\n";
TestRunner::run(new MessageBuilderEncodingTest());
