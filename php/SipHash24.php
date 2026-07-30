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
 * SipHash-2-4 implementation compatible with Guava's Hashing.sipHash24().
 *
 * Uses 64-bit arithmetic with platform-aware handling:
 * - 64-bit PHP: Native integer operations
 * - 32-bit PHP: Split into high/low 32-bit parts
 *
 * The default key matches Guava's Hashing.sipHash24(): the fixed key bytes
 * 00 01 .. 0f, i.e. k0 = 0x0706050403020100 and k1 = 0x0f0e0d0c0b0a0908
 * (SipHash key words are little-endian). Input words are also read in
 * little-endian order, per the SipHash specification, so hashes are
 * byte-for-byte identical with the Java and Node.js clients.
 *
 * Note: On 32-bit platforms, hash values may exceed PHP_INT_MAX and be
 * represented as floats. Use with caution in array keys or strict comparisons.
 */
class SipHash24
{
    /** @var int */
    private int $k0;
    /** @var int */
    private int $k1;
    
    // 64-bit mask constant (avoid float conversion)
    private const MASK_64 = -1; // All bits set to 1 in two's complement

    // Guava's fixed sipHash24() key: bytes 00..0f read as two little-endian longs
    public const GUAVA_K0 = 0x0706050403020100;
    public const GUAVA_K1 = 0x0f0e0d0c0b0a0908;

    /**
     * Constructor - initializes SipHash-2-4 with given key.
     *
     * Defaults to Guava's Hashing.sipHash24() fixed key (bytes 00..0f).
     *
     * @param int $k0 Key part 0 (will be masked to 64 bits)
     * @param int $k1 Key part 1 (will be masked to 64 bits)
     */
    public function __construct(int $k0 = self::GUAVA_K0, int $k1 = self::GUAVA_K1)
    {
        $this->k0 = (int)$k0 & self::MASK_64;
        $this->k1 = (int)$k1 & self::MASK_64;
    }

    /**
     * Convenience static method for quick hashing with default key.
     *
     * @param string $data Input data
     * @return int|float 64-bit hash value (may be float on 32-bit PHP if value exceeds PHP_INT_MAX)
     */
    public static function hash(string $data): int
    {
        $instance = new self();
        return $instance->hashBytes($data);
    }

    /**
     * Compute SipHash-2-4 of the given bytes.
     *
     * @param string $data Input data
     * @return int|float 64-bit hash value (may be float on 32-bit PHP if value exceeds PHP_INT_MAX)
     */
    public function hashBytes(string $data): int
    {
        $length = strlen($data);

        // Initialize state with the standard SipHash constants
        // ("somepseudorandomlygeneratedbytes" split into four 64-bit words)
        $v0 = (int)($this->k0 ^ 0x736f6d6570736575);
        $v1 = (int)($this->k1 ^ 0x646f72616e646f6d);
        $v2 = (int)($this->k0 ^ 0x6c7967656e657261);
        $v3 = (int)($this->k1 ^ 0x7465646279746573);

        // Process full 8-byte blocks
        $blocks = intdiv($length, 8);
        for ($i = 0; $i < $blocks; $i++) {
            $m = $this->readLong($data, $i * 8);
            $v3 = self::xor64($v3, $m);
            list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
            list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
            $v0 = self::xor64($v0, $m);
        }

        // Build last block: length in the top byte, remaining input bytes
        // packed little-endian (byte i of the tail goes to bits i*8..i*8+7)
        $b = (int)(($length & 0xFF) << 56);
        $offset = $blocks * 8;
        $left = $length - $offset;
        for ($i = 0; $i < $left; $i++) {
            $b |= (ord($data[$offset + $i]) & 0xFF) << ($i * 8);
        }

        $v3 = self::xor64($v3, $b);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        $v0 = self::xor64($v0, $b);

        $v2 = (int)(self::xor64($v2, 0xFF));

        // Finalization: 4 rounds
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);

        return self::and64(self::xor64(self::xor64($v0, $v1), self::xor64($v2, $v3)), self::MASK_64);
    }

    /**
     * Read a little-endian 64-bit integer from $data at $offset.
     *
     * @param string $data Input byte string
     * @param int $offset Byte offset to read from
     * @return int|float 64-bit value (may be float on 32-bit PHP)
     */
    private function readLong(string $data, int $offset): int
    {
        if (PHP_INT_SIZE >= 8) {
            // 64-bit PHP: direct calculation, byte i is bits i*8..i*8+7 (little-endian)
            $result = 0;
            for ($i = 0; $i < 8; $i++) {
                $result |= (ord($data[$offset + $i]) & 0xFF) << ($i * 8);
            }
            return (int)$result & self::MASK_64;
        } else {
            // 32-bit PHP: split into high and low 32-bit parts
            $low = 0;
            $high = 0;
            
            // Low 32 bits (bytes 0-3, little-endian)
            for ($i = 0; $i < 4; $i++) {
                $low |= (ord($data[$offset + $i]) & 0xFF) << ($i * 8);
            }
            
            // High 32 bits (bytes 4-7, little-endian)
            for ($i = 4; $i < 8; $i++) {
                $high |= (ord($data[$offset + $i]) & 0xFF) << (($i - 4) * 8);
            }
            
            return ($high << 32) | ($low & 0xFFFFFFFF);
        }
    }

    /**
     * One SipRound mixing operation.
     *
     * @param int|float $v0 State word 0
     * @param int|float $v1 State word 1
     * @param int|float $v2 State word 2
     * @param int|float $v3 State word 3
     * @return array Array of four state words [$v0, $v1, $v2, $v3]
     */
    private function sipRound(int $v0, int $v1, int $v2, int $v3): array
    {
        $v0 = self::add64($v0, $v1);
        $v1 = self::rotl64($v1, 13);
        $v1 = self::xor64($v1, $v0);
        $v0 = self::rotl64($v0, 32);

        $v2 = self::add64($v2, $v3);
        $v3 = self::rotl64($v3, 16);
        $v3 = self::xor64($v3, $v2);

        $v0 = self::add64($v0, $v3);
        $v3 = self::rotl64($v3, 21);
        $v3 = self::xor64($v3, $v0);

        $v2 = self::add64($v2, $v1);
        $v1 = self::rotl64($v1, 17);
        $v1 = self::xor64($v1, $v2);
        $v2 = self::rotl64($v2, 32);

        return [$v0, $v1, $v2, $v3];
    }

    /**
     * 64-bit addition, works on both 64-bit and 32-bit PHP.
     *
     * @param int|float $a First 64-bit operand
     * @param int|float $b Second 64-bit operand
     * @return int|float 64-bit sum masked to 64 bits
     */
    private static function add64(int $a, int $b): int
    {
        if (PHP_INT_SIZE >= 8) {
            // Split into 32-bit halves: native + would overflow to float on
            // 64-bit PHP, silently losing low-order bits
            $low = ($a & 0xFFFFFFFF) + ($b & 0xFFFFFFFF);
            $high = (($a >> 32) & 0xFFFFFFFF) + (($b >> 32) & 0xFFFFFFFF) + (($low >> 32) & 0x1);
            return (($high & 0xFFFFFFFF) << 32) | ($low & 0xFFFFFFFF);
        }

        // Split into high and low 32-bit parts
        $ah = (int)(($a >> 32) & 0xFFFFFFFF);
        $al = (int)($a & 0xFFFFFFFF);
        $bh = (int)(($b >> 32) & 0xFFFFFFFF);
        $bl = (int)($b & 0xFFFFFFFF);

        // Add low parts first
        $sumL = (int)($al + $bl);
        
        // Check for carry (use comparison to avoid float conversion)
        $carry = 0;
        if ($sumL < 0 || ($al > 0 && $bl > 0 && $sumL < $al)) {
            $carry = 1;
            $sumL = (int)($sumL & 0xFFFFFFFF);
        }
        
        // Add high parts with carry
        $sumH = (int)(($ah + $bh + $carry) & 0xFFFFFFFF);

        return ($sumH << 32) | $sumL;
    }

    /**
     * 64-bit XOR.
     *
     * @param int|float $a First 64-bit operand
     * @param int|float $b Second 64-bit operand
     * @return int|float 64-bit XOR result masked to 64 bits
     */
    private static function xor64(int $a, int $b): int
    {
        if (PHP_INT_SIZE >= 8) {
            return ((int)$a ^ (int)$b) & self::MASK_64;
        }

        $aHi = (int)(($a >> 32) & 0xFFFFFFFF);
        $alower = (int)($a & 0xFFFFFFFF);
        $bHi = (int)(($b >> 32) & 0xFFFFFFFF);
        $blower = (int)($b & 0xFFFFFFFF);

        return (($aHi ^ $bHi) << 32) | ($alower ^ $blower);
    }

    /**
     * 64-bit AND mask.
     *
     * @param int|float $a 64-bit value to mask
     * @param int $mask 64-bit mask value
     * @return int|float 64-bit masked result
     */
    private static function and64(int $a, int $mask): int
    {
        if (PHP_INT_SIZE >= 8) {
            return (int)($a & $mask);
        }

        $aHi = (int)(($a >> 32) & 0xFFFFFFFF);
        $alower = (int)($a & 0xFFFFFFFF);
        $mHi = (int)(($mask >> 32) & 0xFFFFFFFF);
        $mlower = (int)($mask & 0xFFFFFFFF);

        return (($aHi & $mHi) << 32) | ($alower & $mlower);
    }

    /**
     * 64-bit left rotate.
     *
     * @param int|float $a 64-bit value to rotate
     * @param int $n Number of bits to rotate left
     * @return int|float 64-bit rotated result masked to 64 bits
     */
    private static function rotl64(int $a, int $n): int
    {
        if (PHP_INT_SIZE >= 8) {
            $n &= 63;
            if ($n === 0) {
                return (int)$a;
            }
            // Mask after the right shift: PHP's >> is arithmetic and would
            // sign-extend negative values into the rotated-in bits
            return (((int)$a << $n) | (((int)$a >> (64 - $n)) & ((1 << $n) - 1))) & self::MASK_64;
        }

        $n &= 63;
        if ($n === 0) {
            return $a;
        }

        $aHi = (int)(($a >> 32) & 0xFFFFFFFF);
        $alower = (int)($a & 0xFFFFFFFF);

        if ($n >= 32) {
            // Rotating by 32 swaps the halves; reduce to a rotation below 32
            $tmp = $aHi;
            $aHi = $alower;
            $alower = $tmp;
            $n -= 32;
            if ($n === 0) {
                return ($aHi << 32) | $alower;
            }
        }

        $newHi = (int)((($aHi << $n) | (($alower >> (32 - $n)) & ((1 << $n) - 1))) & 0xFFFFFFFF);
        $newlower = (int)((($alower << $n) | (($aHi >> (32 - $n)) & ((1 << $n) - 1))) & 0xFFFFFFFF);

        return ($newHi << 32) | $newlower;
    }
}
