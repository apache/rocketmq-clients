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
require_once __DIR__ . '/../grpc/Apache/Rocketmq/V2/Encoding.php';
require_once __DIR__ . '/../grpc/Apache/Rocketmq/V2/DigestType.php';

use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\DigestType;

/**
 * Tests for Encoding and DigestType protobuf enum values.
 * Mirrors Java's EncodingTest.
 * PHP gRPC uses integer constants directly, so we verify the values
 * and name/value round-trip behavior.
 */
class EncodingTest
{
    /**
     * Mirrors Java: testToProtobuf - Verify PHP constants match protobuf values.
     * In PHP, the enum values ARE the protobuf values (no conversion needed).
     */
    public function testEncodingValues()
    {
        TestRunner::assertEqualsWithMessage(0, Encoding::ENCODING_UNSPECIFIED,
            "ENCODING_UNSPECIFIED should be 0");
        TestRunner::assertEqualsWithMessage(1, Encoding::IDENTITY,
            "IDENTITY should be 1");
        TestRunner::assertEqualsWithMessage(2, Encoding::GZIP,
            "GZIP should be 2");
    }

    /**
     * Mirrors Java: testFromProtobuf - Verify name() method works correctly.
     */
    public function testEncodingNameLookup()
    {
        TestRunner::assertEqualsWithMessage('ENCODING_UNSPECIFIED',
            Encoding::name(Encoding::ENCODING_UNSPECIFIED),
            "name(0) should return ENCODING_UNSPECIFIED");
        TestRunner::assertEqualsWithMessage('IDENTITY',
            Encoding::name(Encoding::IDENTITY),
            "name(1) should return IDENTITY");
        TestRunner::assertEqualsWithMessage('GZIP',
            Encoding::name(Encoding::GZIP),
            "name(2) should return GZIP");
    }

    /**
     * Mirrors Java: testFromProtobufWithUnspecified - Verify value() method
     * works correctly, and invalid values throw.
     */
    public function testEncodingValueLookup()
    {
        TestRunner::assertEqualsWithMessage(1,
            Encoding::value('IDENTITY'),
            "value('IDENTITY') should return 1");
        TestRunner::assertEqualsWithMessage(2,
            Encoding::value('GZIP'),
            "value('GZIP') should return 2");

        TestRunner::assertThrows(\UnexpectedValueException::class, function() {
            Encoding::name(999);
        }, "name() with invalid value should throw");
    }

    /**
     * Tests DigestType enum values match protobuf specification.
     */
    public function testDigestTypeValues()
    {
        TestRunner::assertEqualsWithMessage(0, DigestType::DIGEST_TYPE_UNSPECIFIED,
            "DIGEST_TYPE_UNSPECIFIED should be 0");
        TestRunner::assertEqualsWithMessage(1, DigestType::CRC32,
            "CRC32 should be 1");
        TestRunner::assertEqualsWithMessage(2, DigestType::MD5,
            "MD5 should be 2");
        TestRunner::assertEqualsWithMessage(3, DigestType::SHA1,
            "SHA1 should be 3");
    }

    /**
     * Tests DigestType name() method works correctly.
     */
    public function testDigestTypeNameLookup()
    {
        TestRunner::assertEqualsWithMessage('CRC32',
            DigestType::name(DigestType::CRC32),
            "name(CRC32) should return CRC32");
        TestRunner::assertEqualsWithMessage('MD5',
            DigestType::name(DigestType::MD5),
            "name(MD5) should return MD5");
        TestRunner::assertEqualsWithMessage('SHA1',
            DigestType::name(DigestType::SHA1),
            "name(SHA1) should return SHA1");
    }
}

echo "=== EncodingTest ===\n";
$test = new EncodingTest();
$test->testEncodingValues();
echo "  [OK] testEncodingValues\n";
$test->testEncodingNameLookup();
echo "  [OK] testEncodingNameLookup\n";
$test->testEncodingValueLookup();
echo "  [OK] testEncodingValueLookup\n";
$test->testDigestTypeValues();
echo "  [OK] testDigestTypeValues\n";
$test->testDigestTypeNameLookup();
echo "  [OK] testDigestTypeNameLookup\n";
