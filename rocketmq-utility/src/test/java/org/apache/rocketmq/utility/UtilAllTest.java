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

package org.apache.rocketmq.utility;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import org.testng.annotations.Test;

public class UtilAllTest {
    private final String body = "foobar";

    @Test
    public void testPositiveMod() {
        assertTrue(0 <= UtilAll.positiveMod(Integer.MIN_VALUE, 3));
        assertTrue(0 <= UtilAll.positiveMod(Integer.MAX_VALUE, 3));
        assertTrue(0 <= UtilAll.positiveMod(3, 3));
    }

    @Test
    public void testCompressAndUncompressByteArray() throws IOException {
        final byte[] bytes = body.getBytes(UtilAll.DEFAULT_CHARSET);
        final byte[] compressedBytes = UtilAll.compressBytesGzip(bytes, 5);
        final byte[] originalBytes = UtilAll.uncompressBytesGzip(compressedBytes);
        assertEquals(new String(originalBytes, UtilAll.DEFAULT_CHARSET), body);
    }

    @Test
    public void testMacAddress() {
        final byte[] macAddressBytes = UtilAll.macAddress();
        assertNotNull(macAddressBytes);

        final byte[] cachedMacAddressBytes = UtilAll.macAddress();
        assertNotNull(cachedMacAddressBytes);
    }

    @Test
    public void testHostName() {
        final String hostname = UtilAll.hostName();
        assertNotNull(hostname);
        assertTrue(hostname.length() > 0);

        final String cachedHostName = UtilAll.hostName();
        assertNotNull(cachedHostName);
        assertTrue(cachedHostName.length() > 0);
    }

    @Test
    public void testProcessId() {
        final int processId = UtilAll.processId();
        assertTrue(processId > 0);

        final int cachedProcessId = UtilAll.processId();
        assertTrue(cachedProcessId > 0);
    }

    @Test
    public void testCrc32CheckSum() throws UnsupportedEncodingException {
        final byte[] bytes = body.getBytes(UtilAll.DEFAULT_CHARSET);
        assertEquals("9EF61F95", UtilAll.crc32CheckSum(bytes));
    }

    @Test
    public void testMd5CheckSum() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        final byte[] bytes = body.getBytes(UtilAll.DEFAULT_CHARSET);
        assertEquals("3858F62230AC3C915F300C664312C63F", UtilAll.md5CheckSum(bytes));
    }

    @Test
    public void testSha1CheckSum() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        final byte[] bytes = body.getBytes(UtilAll.DEFAULT_CHARSET);
        assertEquals("8843D7F92416211DE9EBB963FF4CE28125932878", UtilAll.sha1CheckSum(bytes));
    }

    @Test
    public void testStackTrace() {
        final String stackTrace = UtilAll.stackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length() > 0);
    }
}
