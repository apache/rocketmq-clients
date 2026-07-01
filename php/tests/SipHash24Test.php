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

use Apache\Rocketmq\SipHash24;
use PHPUnit\Framework\TestCase;

/**
 * SipHash24Test - Unit tests for SipHash-2-4 implementation
 * 
 * Tests cover:
 * 1. Basic hashing functionality
 * 2. Deterministic output (same input → same hash)
 * 3. Different inputs produce different hashes
 * 4. Empty string handling
 * 5. Large data handling
 * 6. Custom key support
 * 7. Binary data handling
 */
class SipHash24Test extends TestCase
{
    /**
     * Test basic hashing with default key (0, 0)
     */
    public function testHashWithDefaultKey()
    {
        $hash1 = SipHash24::hash("test");
        $hash2 = SipHash24::hash("test");
        
        // Same input should produce same hash (deterministic)
        $this->assertEquals($hash1, $hash2);
        $this->assertIsInt($hash1);
    }

    /**
     * Test that different inputs produce different hashes
     */
    public function testDifferentInputsProduceDifferentHashes()
    {
        $hash1 = SipHash24::hash("test1");
        $hash2 = SipHash24::hash("test2");
        $hash3 = SipHash24::hash("different");
        
        $this->assertNotEquals($hash1, $hash2);
        $this->assertNotEquals($hash2, $hash3);
        $this->assertNotEquals($hash1, $hash3);
    }

    /**
     * Test empty string hashing
     */
    public function testHashEmptyString()
    {
        $hash = SipHash24::hash("");
        $this->assertIsInt($hash);
        
        // Empty string should always produce the same hash
        $hash2 = SipHash24::hash("");
        $this->assertEquals($hash, $hash2);
    }

    /**
     * Test hashing with custom keys
     */
    public function testHashWithCustomKeys()
    {
        $data = "test data";
        
        $hash1 = (new SipHash24(0, 0))->hashBytes($data);
        $hash2 = (new SipHash24(1, 0))->hashBytes($data);
        $hash3 = (new SipHash24(0, 1))->hashBytes($data);
        $hash4 = (new SipHash24(123456, 789012))->hashBytes($data);
        
        // Different keys should produce different hashes for same data
        $this->assertNotEquals($hash1, $hash2);
        $this->assertNotEquals($hash1, $hash3);
        $this->assertNotEquals($hash2, $hash3);
        $this->assertNotEquals($hash1, $hash4);
    }

    /**
     * Test that same key produces consistent results
     */
    public function testConsistentHashingWithSameKey()
    {
        $data = "consistent test";
        $sipHash = new SipHash24(0x12345678, 0x9ABCDEF0);
        
        $hash1 = $sipHash->hashBytes($data);
        $hash2 = $sipHash->hashBytes($data);
        $hash3 = $sipHash->hashBytes($data);
        
        $this->assertEquals($hash1, $hash2);
        $this->assertEquals($hash2, $hash3);
    }

    /**
     * Test hashing single character
     */
    public function testHashSingleCharacter()
    {
        $hashA = SipHash24::hash("a");
        $hashB = SipHash24::hash("b");
        
        $this->assertNotEquals($hashA, $hashB);
        $this->assertIsInt($hashA);
        $this->assertIsInt($hashB);
    }

    /**
     * Test hashing long strings
     */
    public function testHashLongString()
    {
        $longString = str_repeat("abcdefghijklmnopqrstuvwxyz", 100); // 2600 chars
        $hash = SipHash24::hash($longString);
        
        $this->assertIsInt($hash);
        
        // Should be deterministic
        $hash2 = SipHash24::hash($longString);
        $this->assertEquals($hash, $hash2);
    }

    /**
     * Test hashing binary data
     */
    public function testHashBinaryData()
    {
        $binaryData = pack("C*", range(0, 255)); // All byte values
        $hash = SipHash24::hash($binaryData);
        
        $this->assertIsInt($hash);
        
        // Same binary data should produce same hash
        $hash2 = SipHash24::hash($binaryData);
        $this->assertEquals($hash, $hash2);
    }

    /**
     * Test hashing null bytes
     */
    public function testHashNullBytes()
    {
        $nullBytes = "\0\0\0\0";
        $hash = SipHash24::hash($nullBytes);
        
        $this->assertIsInt($hash);
        
        // Different from empty string
        $emptyHash = SipHash24::hash("");
        $this->assertNotEquals($hash, $emptyHash);
    }

    /**
     * Test hashing special characters
     */
    public function testHashSpecialCharacters()
    {
        $special = "!@#$%^&*()_+-=[]{}|;':\",./<>?";
        $hash = SipHash24::hash($special);
        
        $this->assertIsInt($hash);
        
        // Should be different from regular text
        $normalHash = SipHash24::hash("normal text");
        $this->assertNotEquals($hash, $normalHash);
    }

    /**
     * Test hashing Unicode/UTF-8 strings
     */
    public function testHashUnicodeString()
    {
        $unicode = "你好世界🌍Hello世界";
        $hash = SipHash24::hash($unicode);
        
        $this->assertIsInt($hash);
        
        // Should be deterministic
        $hash2 = SipHash24::hash($unicode);
        $this->assertEquals($hash, $hash2);
    }

    /**
     * Test that hash values are within 64-bit range
     */
    public function testHashValueRange()
    {
        $testStrings = [
            "short",
            "medium length string",
            "very long string " . str_repeat("x", 1000),
            "",
            "a",
        ];
        
        foreach ($testStrings as $str) {
            $hash = SipHash24::hash($str);
            
            // On 64-bit PHP, should be a valid integer
            if (PHP_INT_SIZE >= 8) {
                $this->assertIsInt($hash);
            } else {
                // On 32-bit PHP, might be float due to large numbers
                $this->assertTrue(is_int($hash) || is_float($hash));
            }
        }
    }

    /**
     * Test multiple instances with same key produce same results
     */
    public function testMultipleInstancesConsistency()
    {
        $data = "test consistency";
        $key0 = 12345;
        $key1 = 67890;
        
        $instance1 = new SipHash24($key0, $key1);
        $instance2 = new SipHash24($key0, $key1);
        
        $hash1 = $instance1->hashBytes($data);
        $hash2 = $instance2->hashBytes($data);
        
        $this->assertEquals($hash1, $hash2);
    }

    /**
     * Test case sensitivity
     */
    public function testCaseSensitivity()
    {
        $hashLower = SipHash24::hash("test");
        $hashUpper = SipHash24::hash("TEST");
        $hashMixed = SipHash24::hash("TeSt");
        
        $this->assertNotEquals($hashLower, $hashUpper);
        $this->assertNotEquals($hashLower, $hashMixed);
        $this->assertNotEquals($hashUpper, $hashMixed);
    }

    /**
     * Test hashing data with exactly 8 bytes (one block)
     */
    public function testHashExactlyOneBlock()
    {
        $data = "12345678"; // Exactly 8 bytes
        $this->assertEquals(8, strlen($data));
        
        $hash = SipHash24::hash($data);
        $this->assertIsInt($hash);
        
        // Should be deterministic
        $hash2 = SipHash24::hash($data);
        $this->assertEquals($hash, $hash2);
    }

    /**
     * Test hashing data with multiple of 8 bytes
     */
    public function testHashMultipleBlocks()
    {
        $data = "1234567812345678"; // 16 bytes = 2 blocks
        $this->assertEquals(16, strlen($data));
        
        $hash = SipHash24::hash($data);
        $this->assertIsInt($hash);
        
        // Different from single block
        $singleBlockHash = SipHash24::hash("12345678");
        $this->assertNotEquals($hash, $singleBlockHash);
    }

    /**
     * Test hashing data with partial block (not multiple of 8)
     */
    public function testHashPartialBlock()
    {
        $data = "12345"; // 5 bytes (partial block)
        $hash = SipHash24::hash($data);
        
        $this->assertIsInt($hash);
        
        // Should be different from other lengths
        $hash6 = SipHash24::hash("123456");
        $hash7 = SipHash24::hash("1234567");
        $hash8 = SipHash24::hash("12345678");
        
        $this->assertNotEquals($hash, $hash6);
        $this->assertNotEquals($hash, $hash7);
        $this->assertNotEquals($hash, $hash8);
    }

    /**
     * Test constructor key masking
     */
    public function testConstructorKeyMasking()
    {
        // Keys should be masked to 64 bits
        $largeKey = PHP_INT_MAX;
        $instance = new SipHash24($largeKey, $largeKey);
        
        $hash = $instance->hashBytes("test");
        $this->assertIsInt($hash);
    }

    /**
     * Test static hash method vs instance method consistency
     */
    public function testStaticVsInstanceMethod()
    {
        $data = "consistency test";
        
        $staticHash = SipHash24::hash($data);
        $instanceHash = (new SipHash24(0, 0))->hashBytes($data);
        
        $this->assertEquals($staticHash, $instanceHash);
    }

    /**
     * Test performance with many iterations
     */
    public function testPerformanceWithManyIterations()
    {
        $start = microtime(true);
        
        // Reduce iterations for 32-bit PHP (slower due to manual 64-bit arithmetic)
        $iterations = (PHP_INT_SIZE >= 8) ? 1000 : 100;
        
        for ($i = 0; $i < $iterations; $i++) {
            SipHash24::hash("test message {$i}");
        }
        
        $elapsed = microtime(true) - $start;
        
        // Should complete in reasonable time (< 5 seconds for 1000 on 64-bit, < 1s for 100 on 32-bit)
        $maxTime = (PHP_INT_SIZE >= 8) ? 5.0 : 1.0;
        $this->assertLessThan($maxTime, $elapsed, "{$iterations} hashes took too long: {$elapsed}s");
    }

    /**
     * Test that hash distribution is reasonable (basic avalanche test)
     */
    public function testHashDistribution()
    {
        $hashes = [];
        for ($i = 0; $i < 100; $i++) {
            $hash = SipHash24::hash("input_{$i}");
            $hashes[] = $hash;
        }
        
        // All hashes should be unique (high probability with good hash function)
        $uniqueHashes = array_unique($hashes);
        $this->assertCount(100, $uniqueHashes, "Hash function should produce unique outputs for different inputs");
    }
}
