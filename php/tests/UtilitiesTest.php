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

require_once __DIR__ . '/../Utilities.php';

use Apache\Rocketmq\Utilities;

/**
 * Tests for utility functions (compression, checksums) via Utilities class.
 */
class UtilitiesTest extends TestCase
{
    private $body = 'foobar';

    public function testCompressDecompressZlib()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $original = Utilities::decompressBytes($compressed, Utilities::ENCODING_ZLIB);

        $this->assertEquals(
            $this->body,
            $original,
            "ZLIB compress/decompress round-trip should restore original"
        );
    }

    public function testCompressDecompressGzip()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $original = Utilities::decompressBytes($compressed, Utilities::ENCODING_GZIP);

        $this->assertEquals(
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

        $this->assertEquals(
            $this->body,
            $original,
            "DEFLATE compress/decompress round-trip should restore original"
        );
    }

    public function testDecompressCorruptData()
    {
        $this->assertFalse(
            @gzuncompress('this-is-not-valid-compressed-data'),
            "Decompressing corrupt data should return false"
        );
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

    public function testChecksumCaseInsensitiveComparison()
    {
        $md5Upper = Utilities::md5CheckSum($this->body);
        $md5Lower = strtolower(Utilities::md5CheckSum($this->body));

        $this->assertTrue(
            strcasecmp($md5Upper, $md5Lower) === 0,
            "MD5 hex should be case-insensitive equivalent"
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

    public function testStackTrace()
    {
        $stackTrace = debug_backtrace();
        $this->assertTrue(
            is_array($stackTrace) && count($stackTrace) > 0,
            "Stack trace should be a non-empty array"
        );
    }

    public function testPhpDescription()
    {
        $description = PHP_VERSION;
        $this->assertTrue(
            strlen($description) > 0,
            "PHP version description should be non-empty"
        );
    }

    public function testMaxIntValue()
    {
        $this->assertTrue(
            PHP_INT_MAX > 1000000000,
            "PHP_INT_MAX should support at least 1GB values"
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

    public function testAutoDetectGzipEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_GZIP_STR);
        $decompressed = Utilities::decompressBytes($compressed);

        $this->assertEquals(
            $this->body,
            $decompressed,
            "Auto-detect should recognize GZIP magic bytes"
        );
    }

    public function testAutoDetectZlibEncoding()
    {
        $compressed = Utilities::compressBytes($this->body, Utilities::ENCODING_ZLIB_STR);
        $decompressed = Utilities::decompressBytes($compressed);

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
}
