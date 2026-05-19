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
require_once __DIR__ . '/../Logger.php';

/**
 * Tests for utility functions (compression, checksums).
 * Mirrors Java's UtilitiesTest.
 */
class UtilitiesTest
{
    private $body = 'foobar';

    /**
     * Mirrors Java: testCompressAndUncompressByteArray
     * Tests ZLIB compress/decompress round-trip.
     */
    public function testCompressDecompressZlib()
    {
        $bytes = $this->body;
        $compressed = gzcompress($bytes, 5);
        $original = gzuncompress($compressed);

        TestRunner::assertEqualsWithMessage(
            $this->body,
            $original,
            "ZLIB compress/decompress round-trip should restore original"
        );
    }

    /**
     * Tests GZIP compress/decompress round-trip.
     */
    public function testCompressDecompressGzip()
    {
        $bytes = $this->body;
        $compressed = gzencode($bytes);
        $original = gzdecode($compressed);

        TestRunner::assertEqualsWithMessage(
            $this->body,
            $original,
            "GZIP compress/decompress round-trip should restore original"
        );
    }

    /**
     * Tests DEFLATE compress/decompress round-trip.
     */
    public function testCompressDecompressDeflate()
    {
        $bytes = $this->body;
        $compressed = gzdeflate($bytes, 5);
        $original = gzinflate($compressed);

        TestRunner::assertEqualsWithMessage(
            $this->body,
            $original,
            "DEFLATE compress/decompress round-trip should restore original"
        );
    }

    /**
     * Tests that corrupt data fails decompression.
     */
    public function testDecompressCorruptData()
    {
        TestRunner::assertFalse(
            @gzuncompress('this-is-not-valid-compressed-data'),
            "Decompressing corrupt data should return false"
        );
    }

    /**
     * Mirrors Java: testCrc32CheckSum
     * CRC32 of "foobar" should be 9EF61F95 (unsigned, uppercase hex).
     */
    public function testCrc32CheckSum()
    {
        $crc32 = sprintf('%u', crc32($this->body));
        $hex = strtoupper(dechex($crc32));

        TestRunner::assertEqualsWithMessage(
            '9EF61F95',
            $hex,
            "CRC32 of 'foobar' should be 9EF61F95"
        );
    }

    /**
     * Mirrors Java: testMd5CheckSum
     * MD5 of "foobar" should be 3858F62230AC3C915F300C664312C63F.
     */
    public function testMd5CheckSum()
    {
        $md5 = strtoupper(md5($this->body));

        TestRunner::assertEqualsWithMessage(
            '3858F62230AC3C915F300C664312C63F',
            $md5,
            "MD5 of 'foobar' should be 3858F62230AC3C915F300C664312C63F"
        );
    }

    /**
     * Mirrors Java: testSha1CheckSum
     * SHA1 of "foobar" should be 8843D7F92416211DE9EBB963FF4CE28125932878.
     */
    public function testSha1CheckSum()
    {
        $sha1 = strtoupper(sha1($this->body));

        TestRunner::assertEqualsWithMessage(
            '8843D7F92416211DE9EBB963FF4CE28125932878',
            $sha1,
            "SHA1 of 'foobar' should be 8843D7F92416211DE9EBB963FF4CE28125932878"
        );
    }

    /**
     * Tests that checksums are case-insensitive when comparing.
     */
    public function testChecksumCaseInsensitiveComparison()
    {
        $md5Upper = strtoupper(md5($this->body));
        $md5Lower = strtolower(md5($this->body));

        TestRunner::assertTrue(
            strcasecmp($md5Upper, $md5Lower) === 0,
            "MD5 hex should be case-insensitive equivalent"
        );
    }

    /**
     * Tests that different inputs produce different checksums.
     */
    public function testDifferentInputsDifferentChecksums()
    {
        $crc1 = sprintf('%u', crc32('hello'));
        $crc2 = sprintf('%u', crc32('world'));

        TestRunner::assertTrue(
            $crc1 !== $crc2,
            "Different inputs should produce different CRC32 checksums"
        );
    }

    /**
     * Mirrors Java: testStackTrace - PHP equivalent using debug_backtrace.
     */
    public function testStackTrace()
    {
        $stackTrace = debug_backtrace();
        TestRunner::assertTrue(
            is_array($stackTrace) && count($stackTrace) > 0,
            "Stack trace should be a non-empty array"
        );
    }

    /**
     * Mirrors Java: testGetJavaDescription - PHP equivalent using PHP_VERSION.
     */
    public function testPhpDescription()
    {
        $description = PHP_VERSION;
        TestRunner::assertTrue(
            strlen($description) > 0,
            "PHP version description should be non-empty"
        );
    }

    /**
     * Tests PHP_INT_MAX for understanding platform limits.
     */
    public function testMaxIntValue()
    {
        // PHP_INT_MAX should be large enough for message size calculations
        TestRunner::assertTrue(
            PHP_INT_MAX > 1000000000,
            "PHP_INT_MAX should support at least 1GB values"
        );
    }

    /**
     * Tests empty string checksums.
     */
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
$test = new UtilitiesTest();
$test->testCompressDecompressZlib();
echo "  [OK] testCompressDecompressZlib\n";
$test->testCompressDecompressGzip();
echo "  [OK] testCompressDecompressGzip\n";
$test->testCompressDecompressDeflate();
echo "  [OK] testCompressDecompressDeflate\n";
$test->testDecompressCorruptData();
echo "  [OK] testDecompressCorruptData\n";
$test->testCrc32CheckSum();
echo "  [OK] testCrc32CheckSum\n";
$test->testMd5CheckSum();
echo "  [OK] testMd5CheckSum\n";
$test->testSha1CheckSum();
echo "  [OK] testSha1CheckSum\n";
$test->testChecksumCaseInsensitiveComparison();
echo "  [OK] testChecksumCaseInsensitiveComparison\n";
$test->testDifferentInputsDifferentChecksums();
echo "  [OK] testDifferentInputsDifferentChecksums\n";
$test->testStackTrace();
echo "  [OK] testStackTrace\n";
$test->testPhpDescription();
echo "  [OK] testPhpDescription\n";
$test->testMaxIntValue();
echo "  [OK] testMaxIntValue\n";
$test->testEmptyStringChecksums();
echo "  [OK] testEmptyStringChecksums\n";
