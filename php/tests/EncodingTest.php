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

require_once __DIR__ . '/../grpc/Apache/Rocketmq/V2/Encoding.php';
require_once __DIR__ . '/../grpc/Apache/Rocketmq/V2/DigestType.php';

use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\DigestType;

/**
 * Tests for Encoding and DigestType protobuf enum values.
 */
class EncodingTest extends TestCase
{
    public function testEncodingValues()
    {
        $this->assertEquals(0, Encoding::ENCODING_UNSPECIFIED, "ENCODING_UNSPECIFIED should be 0");
        $this->assertEquals(1, Encoding::IDENTITY, "IDENTITY should be 1");
        $this->assertEquals(2, Encoding::GZIP, "GZIP should be 2");
    }

    public function testEncodingNameLookup()
    {
        $this->assertEquals('ENCODING_UNSPECIFIED', Encoding::name(Encoding::ENCODING_UNSPECIFIED), "name(0) should return ENCODING_UNSPECIFIED");
        $this->assertEquals('IDENTITY', Encoding::name(Encoding::IDENTITY), "name(1) should return IDENTITY");
        $this->assertEquals('GZIP', Encoding::name(Encoding::GZIP), "name(2) should return GZIP");
    }

    public function testEncodingValueLookup()
    {
        $this->assertEquals(1, Encoding::value('IDENTITY'), "value('IDENTITY') should return 1");
        $this->assertEquals(2, Encoding::value('GZIP'), "value('GZIP') should return 2");

        $this->expectException(\UnexpectedValueException::class);
        Encoding::name(999);
    }

    public function testDigestTypeValues()
    {
        $this->assertEquals(0, DigestType::DIGEST_TYPE_UNSPECIFIED, "DIGEST_TYPE_UNSPECIFIED should be 0");
        $this->assertEquals(1, DigestType::CRC32, "CRC32 should be 1");
        $this->assertEquals(2, DigestType::MD5, "MD5 should be 2");
        $this->assertEquals(3, DigestType::SHA1, "SHA1 should be 3");
    }

    public function testDigestTypeNameLookup()
    {
        $this->assertEquals('CRC32', DigestType::name(DigestType::CRC32), "name(CRC32) should return CRC32");
        $this->assertEquals('MD5', DigestType::name(DigestType::MD5), "name(MD5) should return MD5");
        $this->assertEquals('SHA1', DigestType::name(DigestType::SHA1), "name(SHA1) should return SHA1");
    }
}
