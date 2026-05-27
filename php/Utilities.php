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

namespace Apache\Rocketmq;

/**
 * Utilities - Compression, decompression, and checksum utilities.
 *
 * Mirrors Java's Utilities.java: supports GZIP and ZLIB via PHP native functions,
 * ZSTD and LZ4 via optional PHP extensions with graceful fallback.
 */
class Utilities
{
    /**
     * Encoding constants matching V2\Encoding protobuf enum values.
     */
    const ENCODING_UNSPECIFIED = 0;
    const ENCODING_IDENTITY = 1;
    const ENCODING_GZIP = 2;
    const ENCODING_ZLIB = 3;

    /**
     * Encoding string names for message builder API.
     */
    const ENCODING_GZIP_STR = 'GZIP';
    const ENCODING_ZLIB_STR = 'ZLIB';
    const ENCODING_ZSTD_STR = 'ZSTD';
    const ENCODING_LZ4_STR = 'LZ4';
    const ENCODING_IDENTITY_STR = 'IDENTITY';

    /**
     * Magic bytes for auto-detecting compression format.
     */
    const MAGIC_GZIP = "\x1f\x8b";
    const MAGIC_ZLIB_DEFLATE = "\x78";
    const MAGIC_ZSTD = "\x28\xb5\x2f\xfd";
    const MAGIC_LZ4 = "\x04\x22\x4d\x18";

    /**
     * Compress data with the specified encoding.
     *
     * @param string $data Raw data to compress
     * @param string $encoding One of ENCODING_GZIP_STR, ENCODING_ZLIB_STR, ENCODING_ZSTD_STR, ENCODING_LZ4_STR
     * @return string Compressed data
     * @throws \RuntimeException if the required extension is not installed
     */
    public static function compressBytes(string $data, string $encoding): string
    {
        switch ($encoding) {
            case self::ENCODING_GZIP_STR:
            case self::ENCODING_GZIP:
                $result = gzencode($data, 5);
                break;
            case self::ENCODING_ZLIB_STR:
            case self::ENCODING_ZLIB:
                $result = gzcompress($data, 5);
                break;
            case self::ENCODING_ZSTD_STR:
                if (!function_exists('zstd_compress')) {
                    throw new \RuntimeException("ZSTD compression requires the zstd PHP extension");
                }
                $result = zstd_compress($data, 5);
                break;
            case self::ENCODING_LZ4_STR:
                if (!function_exists('lz4_compress')) {
                    throw new \RuntimeException("LZ4 compression requires the lz4 PHP extension");
                }
                $result = lz4_compress($data);
                break;
            default:
                throw new \InvalidArgumentException("Unsupported encoding: {$encoding}");
        }

        if ($result === false) {
            throw new \RuntimeException("Compression failed for encoding: {$encoding}");
        }

        return $result;
    }

    /**
     * Decompress data. If encoding is IDENTITY or UNSPECIFIED, auto-detects via magic bytes.
     *
     * @param string $data Compressed data
     * @param string|int|null $encoding Encoding constant or name. null = auto-detect.
     * @return string Decompressed data
     * @throws \RuntimeException if decompression fails or encoding is unsupported
     */
    public static function decompressBytes(string $data, $encoding = null): string
    {
        if ($encoding === null || $encoding === self::ENCODING_UNSPECIFIED || $encoding === self::ENCODING_IDENTITY_STR || $encoding === self::ENCODING_IDENTITY) {
            $encoding = self::detectEncoding($data);
            if ($encoding === self::ENCODING_IDENTITY) {
                return $data;
            }
        }

        switch ($encoding) {
            case self::ENCODING_GZIP:
            case self::ENCODING_GZIP_STR:
                $result = gzdecode($data);
                break;
            case self::ENCODING_ZLIB:
            case self::ENCODING_ZLIB_STR:
                $result = gzuncompress($data);
                break;
            case self::ENCODING_ZSTD:
            case self::ENCODING_ZSTD_STR:
                if (!function_exists('zstd_uncompress')) {
                    throw new \RuntimeException("ZSTD decompression requires the zstd PHP extension");
                }
                $result = zstd_uncompress($data);
                break;
            case self::ENCODING_LZ4:
            case self::ENCODING_LZ4_STR:
                if (!function_exists('lz4_uncompress')) {
                    throw new \RuntimeException("LZ4 decompression requires the lz4 PHP extension");
                }
                $result = lz4_uncompress($data);
                break;
            default:
                throw new \InvalidArgumentException("Unsupported encoding: {$encoding}");
        }

        if ($result === false) {
            throw new \RuntimeException("Decompression failed for encoding: {$encoding}");
        }

        return $result;
    }

    /**
     * Auto-detect compression encoding from magic bytes.
     *
     * @param string $data Compressed data
     * @return int Encoding constant (IDENTITY if no compression detected)
     */
    private static function detectEncoding(string $data): int
    {
        if (strlen($data) < 2) {
            return self::ENCODING_IDENTITY;
        }

        $prefix2 = substr($data, 0, 2);
        $prefix4 = substr($data, 0, 4);

        if ($prefix2 === self::MAGIC_GZIP) {
            return self::ENCODING_GZIP;
        }
        if ($prefix4 === self::MAGIC_ZSTD) {
            return self::ENCODING_ZSTD;
        }
        if ($prefix4 === self::MAGIC_LZ4) {
            return self::ENCODING_LZ4;
        }
        if ($prefix2[0] === self::MAGIC_ZLIB_DEFLATE) {
            return self::ENCODING_ZLIB;
        }

        return self::ENCODING_IDENTITY;
    }

    /**
     * Compute CRC32 checksum and return as zero-padded uppercase hex string.
     *
     * @param string $data
     * @return string
     */
    public static function crc32CheckSum(string $data): string
    {
        return strtoupper(sprintf('%08X', crc32($data)));
    }

    /**
     * Compute MD5 checksum and return as uppercase hex string.
     *
     * @param string $data
     * @return string
     */
    public static function md5CheckSum(string $data): string
    {
        return strtoupper(md5($data));
    }

    /**
     * Compute SHA1 checksum and return as uppercase hex string.
     *
     * @param string $data
     * @return string
     */
    public static function sha1CheckSum(string $data): string
    {
        return strtoupper(sha1($data));
    }

    /**
     * Encode binary data as uppercase hexadecimal string.
     *
     * @param string $bytes Binary data
     * @return string Uppercase hex string
     */
    public static function encodeHexString(string $bytes): string
    {
        return strtoupper(bin2hex($bytes));
    }
}
