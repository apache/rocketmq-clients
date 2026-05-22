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

/**
 * Tests for utility functions (compression, checksums).
 */
class UtilitiesTest
{
    private $body = 'foobar';

    public function testCompressDecompressZlib()
    {
        $bytes = $this->body;
        $compressed = gzcompress($bytes, 5);
        $original = gzuncompress($compressed);

        TestRunner::assertEquals(
            $this->body,
            $original,
            "ZLIB compress/decompress round-trip should restore original"
        );
    }

    public function testCompressDecompressGzip()
    {
        $bytes = $this->body;
        $compressed = gzencode($bytes);
        $original = gzdecode($compressed);

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
        $crc32 = sprintf('%u', crc32($this->body));
        $hex = strtoupper(dechex($crc32));

        TestRunner::assertEquals(
            '9EF61F95',
            $hex,
            "CRC32 of 'foobar' should be 9EF61F95"
        );
    }

    public function testMd5CheckSum()
    {
        $md5 = strtoupper(md5($this->body));

        TestRunner::assertEquals(
            '3858F62230AC3C915F300C664312C63F',
            $md5,
            "MD5 of 'foobar' should be 3858F62230AC3C915F300C664312C63F"
        );
    }

    public function testSha1CheckSum()
    {
        $sha1 = strtoupper(sha1($this->body));

        TestRunner::assertEquals(
            '8843D7F92416211DE9EBB963FF4CE28125932878',
            $sha1,
            "SHA1 of 'foobar' should be 8843D7F92416211DE9EBB963FF4CE28125932878"
        );
    }

    public function testChecksumCaseInsensitiveComparison()
    {
        $md5Upper = strtoupper(md5($this->body));
        $md5Lower = strtolower(md5($this->body));

        TestRunner::assertTrue(
            strcasecmp($md5Upper, $md5Lower) === 0,
            "MD5 hex should be case-insensitive equivalent"
        );
    }

    public function testDifferentInputsDifferentChecksums()
    {
        $crc1 = sprintf('%u', crc32('hello'));
        $crc2 = sprintf('%u', crc32('world'));

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
        // PHP_INT_MAX should be large enough for message size calculations
        TestRunner::assertTrue(
            PHP_INT_MAX > 1000000000,
            "PHP_INT_MAX should support at least 1GB values"
        );
    }

    public function testEmptyStringChecksums()
    {
        $crc32 = sprintf('%u', crc32(''));
        $md5 = strtoupper(md5(''));
        $sha1 = strtoupper(sha1(''));

        TestRunner::assertTrue(
            $crc32 !== '' && $md5 !== '' && $sha1 !== '',
            "Empty string should still produce non-empty checksums"
        );
    }
}

echo "=== UtilitiesTest ===\n";
TestRunner::run(new UtilitiesTest());
