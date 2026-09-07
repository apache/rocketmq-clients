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

use Apache\Rocketmq\Utilities;

/**
 * Tests for Utilities class (compression, decompression, checksums).
 */
class UtilitiesTest extends TestCase
{
    private $body = 'foobar';

    public function testCompressDecompressGzipRoundTrip()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $decompressed = Utilities::decompressBytes($compressed, Utilities::ENCODING_GZIP);

        $this->assertEquals(
            $this->body,
            $decompressed,
            "GZIP compress/decompress round-trip should restore original"
        );
    }

    public function testCompressDecompressZlibRoundTrip()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $decompressed = Utilities::decompressBytes($compressed, Utilities::ENCODING_ZLIB);

        $this->assertEquals(
            $this->body,
            $decompressed,
            "ZLIB compress/decompress round-trip should restore original"
        );
    }

    public function testAutoDetectGzipEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $decompressed = Utilities::decompressBytes($compressed); // null encoding = auto-detect

        $this->assertEquals(
            $this->body,
            $decompressed,
            "Auto-detect should recognize GZIP magic bytes"
        );
    }

    public function testAutoDetectZlibEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $decompressed = Utilities::decompressBytes($compressed); // null encoding = auto-detect

        $this->assertEquals(
            $this->body,
            $decompressed,
            "Auto-detect should recognize ZLIB magic bytes"
        );
    }

    public function testIdentityEncodingReturnsOriginal()
    {
        $result = Utilities::decompressBytes($this->body, Utilities::ENCODING_IDENTITY);

        $this->assertEquals(
            $this->body,
            $result,
            "IDENTITY encoding should return data unchanged"
        );
    }

    public function testCompressEmptyString()
    {
        $compressed = Utilities::compressBytes('', Utilities::ENCODING_GZIP_STR);
        $decompressed = Utilities::decompressBytes($compressed, Utilities::ENCODING_GZIP);

        $this->assertEquals(
            '',
            $decompressed,
            "GZIP round-trip with empty string should return empty"
        );
    }

    public function testUnsupportedEncodingThrows()
    {
        $caught = false;
        try {
            Utilities::compressBytes($this->body, 'BROTLI');
        } catch (\InvalidArgumentException $e) {
            $caught = true;
        }

        $this->assertTrue($caught, "Unsupported encoding should throw InvalidArgumentException");
    }

    public function testCrc32CheckSum()
    {
        $result = Utilities::crc32CheckSum($this->body);

        $this->assertEquals(
            '9EF61F95',
            $result,
            "CRC32 of 'foobar' should be 9EF61F95"
        );
    }

    public function testMd5CheckSum()
    {
        $result = Utilities::md5CheckSum($this->body);

        $this->assertEquals(
            '3858F62230AC3C915F300C664312C63F',
            $result,
            "MD5 of 'foobar' should be 3858F62230AC3C915F300C664312C63F"
        );
    }

    public function testSha1CheckSum()
    {
        $result = Utilities::sha1CheckSum($this->body);

        $this->assertEquals(
            '8843D7F92416211DE9EBB963FF4CE28125932878',
            $result,
            "SHA1 of 'foobar' should be 8843D7F92416211DE9EBB963FF4CE28125932878"
        );
    }

    public function testEncodeHexString()
    {
        $binary = "\x00\x0f\xff\xab\xcd";
        $result = Utilities::encodeHexString($binary);

        $this->assertEquals(
            '000FFFABCD',
            $result,
            "encodeHexString should produce uppercase hex"
        );
    }

    public function testDifferentInputsDifferentChecksums()
    {
        $crc1 = Utilities::crc32CheckSum('hello');
        $crc2 = Utilities::crc32CheckSum('world');

        $this->assertTrue(
            $crc1 !== $crc2,
            "Different inputs should produce different CRC32 checksums"
        );
    }

    public function testEmptyStringChecksums()
    {
        $crc32 = Utilities::crc32CheckSum('');
        $md5 = Utilities::md5CheckSum('');
        $sha1 = Utilities::sha1CheckSum('');

        $this->assertTrue(
            $crc32 !== '' && $md5 !== '' && $sha1 !== '',
            "Empty string should still produce non-empty checksums"
        );
    }

    public function testDecompressCorruptDataThrows()
    {
        $caught = false;
        try {
            @Utilities::decompressBytes('this-is-not-valid-compressed-data', Utilities::ENCODING_GZIP);
        } catch (\RuntimeException $e) {
            $caught = true;
        }

        $this->assertTrue($caught, "Decompressing corrupt data should throw RuntimeException");
    }
}
