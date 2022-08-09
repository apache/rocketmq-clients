/*
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

package org.apache.rocketmq.client.java.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import org.junit.Test;

public class UtilitiesTest {
    private final String body = "foobar";

    @Test
    public void testCompressAndUncompressByteArray() throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        final byte[] compressedBytes = Utilities.compressBytesGzip(bytes, 5);
        final byte[] originalBytes = Utilities.uncompressBytesGzip(compressedBytes);
        assertEquals(new String(originalBytes, StandardCharsets.UTF_8), body);
    }

    @Test
    public void testCrc32CheckSum() {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        assertEquals("9EF61F95", Utilities.crc32CheckSum(bytes));
    }

    @Test
    public void testMd5CheckSum() throws NoSuchAlgorithmException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        assertEquals("3858F62230AC3C915F300C664312C63F", Utilities.md5CheckSum(bytes));
    }

    @Test
    public void testSha1CheckSum() throws NoSuchAlgorithmException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        assertEquals("8843D7F92416211DE9EBB963FF4CE28125932878", Utilities.sha1CheckSum(bytes));
    }

    @Test
    public void testStackTrace() {
        final String stackTrace = Utilities.stackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length() > 0);
    }
}