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
require_once __DIR__ . '/../Utilities.php';

use Apache\Rocketmq\Utilities;

/**
 * Tests for utility functions (compression, checksums) via Utilities class.
 */
class UtilitiesTest
{
    private $body = 'foobar';

    public function testCompressDecompressZlib()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $original = Utilities::decompressBytes($compressed, Utilities::ENCODING_ZLIB);

        TestRunner::assertEquals(
            $this->body,
            $original,
            "ZLIB compress/decompress round-trip should restore original"
        );
    }

    public function testCompressDecompressGzip()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $original = Utilities::decompressBytes($compressed, Utilities::ENCODING_GZIP);

        TestRunner::assertEquals(
            $this->body,
            $original,
            "GZIP compress/decompress round-trip should restore original"
        );
    }

    public function testCompressDecompressDeflate()
    {
        $bytes = $this->body;
        $compressed = gzdeflate($bytes, 5);
        $original = gzinflate($compressed);

        TestRunner::assertEquals(
            $this->body,
            $original,
            "DEFLATE compress/decompress round-trip should restore original"
        );
    }

    public function testDecompressCorruptData()
    {
        TestRunner::assertFalse(
            @gzuncompress('this-is-not-valid-compressed-data'),
            "Decompressing corrupt data should return false"
        );
    }

    public function testCrc32CheckSum()
    {
        $result = Utilities::crc32CheckSum($this->body);

        TestRunner::assertEquals(
            '9EF61F95',
            $result,
            "CRC32 of 'foobar' should be 9EF61F95"
        );
    }

    public function testMd5CheckSum()
    {
        $result = Utilities::md5CheckSum($this->body);

        TestRunner::assertEquals(
            '3858F62230AC3C915F300C664312C63F',
            $result,
            "MD5 of 'foobar' should be 3858F62230AC3C915F300C664312C63F"
        );
    }

    public function testSha1CheckSum()
    {
        $result = Utilities::sha1CheckSum($this->body);

        TestRunner::assertEquals(
            '8843D7F92416211DE9EBB963FF4CE28125932878',
            $result,
            "SHA1 of 'foobar' should be 8843D7F92416211DE9EBB963FF4CE28125932878"
        );
    }

    public function testChecksumCaseInsensitiveComparison()
    {
        $md5Upper = Utilities::md5CheckSum($this->body);
        $md5Lower = strtolower(Utilities::md5CheckSum($this->body));

        TestRunner::assertTrue(
            strcasecmp($md5Upper, $md5Lower) === 0,
            "MD5 hex should be case-insensitive equivalent"
        );
    }

    public function testDifferentInputsDifferentChecksums()
    {
        $crc1 = Utilities::crc32CheckSum('hello');
        $crc2 = Utilities::crc32CheckSum('world');

        TestRunner::assertTrue(
            $crc1 !== $crc2,
            "Different inputs should produce different CRC32 checksums"
        );
    }

    public function testStackTrace()
    {
        $stackTrace = debug_backtrace();
        TestRunner::assertTrue(
            is_array($stackTrace) && count($stackTrace) > 0,
            "Stack trace should be a non-empty array"
        );
    }

    public function testPhpDescription()
    {
        $description = PHP_VERSION;
        TestRunner::assertTrue(
            strlen($description) > 0,
            "PHP version description should be non-empty"
        );
    }

    public function testMaxIntValue()
    {
        TestRunner::assertTrue(
            PHP_INT_MAX > 1000000000,
            "PHP_INT_MAX should support at least 1GB values"
        );
    }

    public function testEmptyStringChecksums()
    {
        $crc32 = Utilities::crc32CheckSum('');
        $md5 = Utilities::md5CheckSum('');
        $sha1 = Utilities::sha1CheckSum('');

        TestRunner::assertTrue(
            $crc32 !== '' && $md5 !== '' && $sha1 !== '',
            "Empty string should still produce non-empty checksums"
        );
    }

    public function testAutoDetectGzipEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $decompressed = Utilities::decompressBytes($compressed);

        TestRunner::assertEquals(
            $this->body,
            $decompressed,
            "Auto-detect should recognize GZIP magic bytes"
        );
    }

    public function testAutoDetectZlibEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $decompressed = Utilities::decompressBytes($compressed);

        TestRunner::assertEquals(
            $this->body,
            $decompressed,
            "Auto-detect should recognize ZLIB magic bytes"
        );
    }

    public function testIdentityEncodingReturnsOriginal()
    {
        $result = Utilities::decompressBytes($this->body, Utilities::ENCODING_IDENTITY);

        TestRunner::assertEquals(
            $this->body,
            $result,
            "IDENTITY encoding should return data unchanged"
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

        TestRunner::assertTrue($caught, "Unsupported encoding should throw InvalidArgumentException");
    }

    public function testEncodeHexString()
    {
        $binary = "\x00\x0f\xff\xab\xcd";
        $result = Utilities::encodeHexString($binary);

        TestRunner::assertEquals(
            '000FFFABCD',
            $result,
            "encodeHexString should produce uppercase hex"
        );
    }
}

echo "=== UtilitiesTest ===\n";
TestRunner::run(new UtilitiesTest());
