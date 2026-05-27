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
 * Uses 64-bit arithmetic via GMP or int fallback.
 * Default key (0, 0) matches Guava's Hashing.sipHash24() behavior.
 */
class SipHash24
{
    /** @var int */
    private $k0;
    /** @var int */
    private $k1;

    /**
     * @param int $k0 Lower 64 bits of the key
     * @param int $k1 Upper 64 bits of the key
     */
    public function __construct($k0 = 0, $k1 = 0)
    {
        $this->k0 = $k0 & 0xFFFFFFFFFFFFFFFF;
        $this->k1 = $k1 & 0xFFFFFFFFFFFFFFFF;
    }

    /**
     * Convenience static method for quick hashing with default key.
     *
     * @param string $data Input data
     * @return int 64-bit hash value
     */
    public static function hash($data)
    {
        $instance = new self();
        return $instance->hashBytes($data);
    }

    /**
     * Compute SipHash-2-4 of the given bytes.
     *
     * @param string $data Input data
     * @return int 64-bit hash value
     */
    public function hashBytes($data)
    {
        $length = strlen($data);

        // Initialize state
        $v0 = $this->k0 ^ 0x736f6d6570736575;
        $v1 = $this->k1 ^ 0x646f72616e646f6d;
        $v2 = $this->k0 ^ 0x6c6f6e67746f6163;
        $v3 = $this->k1 ^ 0x7465646279746573;

        // Process full 8-byte blocks
        $blocks = intdiv($length, 8);
        for ($i = 0; $i < $blocks; $i++) {
            $m = $this->readLong($data, $i * 8);
            $v3 = self::xor64($v3, $m);
            list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
            list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
            $v0 = self::xor64($v0, $m);
        }

        // Build last block
        $b = (($length & 0xFF) << 56);
        $offset = $blocks * 8;
        for ($j = 7; $j >= 1; $j--) {
            if ($length - $offset > 7 - $j) {
                $b |= (ord($data[$offset + (7 - $j)]) & 0xFF) << ($j * 8);
            }
        }

        $v3 = self::xor64($v3, $b);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        $v0 = self::xor64($v0, $b);

        $v2 = self::xor64($v2, 0xFF);

        // Finalization: 4 rounds
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);
        list($v0, $v1, $v2, $v3) = $this->sipRound($v0, $v1, $v2, $v3);

        return self::and64(self::xor64(self::xor64($v0, $v1), self::xor64($v2, $v3)), 0xFFFFFFFFFFFFFFFF);
    }

    /**
     * Read a little-endian 64-bit integer from $data at $offset.
     */
    private function readLong($data, $offset)
    {
        $result = 0;
        for ($i = 7; $i >= 0; $i--) {
            $result |= (ord($data[$offset + (7 - $i)]) & 0xFF) << ($i * 8);
        }
        return $result & 0xFFFFFFFFFFFFFFFF;
    }

    /**
     * One SipRound.
     */
    private function sipRound($v0, $v1, $v2, $v3)
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
     */
    private static function add64($a, $b)
    {
        if (PHP_INT_SIZE >= 8) {
            return ($a + $b) & 0xFFFFFFFFFFFFFFFF;
        }

        $ah = ($a >> 32) & 0xFFFFFFFF;
        $al = $a & 0xFFFFFFFF;
        $bh = ($b >> 32) & 0xFFFFFFFF;
        $bl = $b & 0xFFFFFFFF;

        $sumL = $al + $bl;
        $carry = ($sumL > 0xFFFFFFFF) ? 1 : 0;
        $sumL = $sumL & 0xFFFFFFFF;
        $sumH = ($ah + $bh + $carry) & 0xFFFFFFFF;

        return ($sumH << 32) | $sumL;
    }

    /**
     * 64-bit XOR.
     */
    private static function xor64($a, $b)
    {
        if (PHP_INT_SIZE >= 8) {
            return ($a ^ $b) & 0xFFFFFFFFFFFFFFFF;
        }

        $aHi = ($a >> 32) & 0xFFFFFFFF;
        $aLo = $a & 0xFFFFFFFF;
        $bHi = ($b >> 32) & 0xFFFFFFFF;
        $bLo = $b & 0xFFFFFFFF;

        return (($aHi ^ $bHi) << 32) | ($aLo ^ $bLo);
    }

    /**
     * 64-bit AND mask.
     */
    private static function and64($a, $mask)
    {
        if (PHP_INT_SIZE >= 8) {
            return $a & $mask;
        }

        $aHi = ($a >> 32) & 0xFFFFFFFF;
        $aLo = $a & 0xFFFFFFFF;
        $mHi = ($mask >> 32) & 0xFFFFFFFF;
        $mLo = $mask & 0xFFFFFFFF;

        return (($aHi & $mHi) << 32) | ($aLo & $mLo);
    }

    /**
     * 64-bit left rotate.
     */
    private static function rotl64($a, $n)
    {
        if (PHP_INT_SIZE >= 8) {
            return (($a << $n) | ($a >> (64 - $n))) & 0xFFFFFFFFFFFFFFFF;
        }

        $aHi = ($a >> 32) & 0xFFFFFFFF;
        $aLo = $a & 0xFFFFFFFF;

        if ($n >= 32) {
            $shift = $n - 32;
            return (($aLo << $shift) | ($aHi >> (32 - $shift))) & 0xFFFFFFFF
                | ((($aHi << $shift) | ($aLo >> (32 - $shift))) & 0xFFFFFFFF) << 32;
        }

        if ($n === 0) {
            return $a;
        }

        $newHi = (($aHi << $n) | ($aLo >> (32 - $n))) & 0xFFFFFFFF;
        $newLo = (($aLo << $n) | ($aHi >> (32 - $n))) & 0xFFFFFFFF;

        return ($newHi << 32) | $newLo;
    }
}
