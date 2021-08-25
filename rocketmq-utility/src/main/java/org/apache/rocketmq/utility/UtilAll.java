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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomUtils;

public class UtilAll {
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final Locale LOCALE = new Locale("zh", "CN");

    private static final int PROCESS_ID_NOT_SET = -2;
    private static final int PROCESS_ID_NOT_FOUND = -1;
    private static int PROCESS_ID = PROCESS_ID_NOT_SET;

    private static final String HOST_NAME_NOT_FOUND = "HOST_NAME_NOT_FOUND";
    private static String HOST_NAME = null;

    private static final byte[] MAC_ADDRESS_NOT_FOUND = RandomUtils.nextBytes(6);
    private static byte[] MAC_ADDRESS = null;

    private UtilAll() {
    }

    public static int positiveMod(int a, int b) {
        return (a % b + b) % b;
    }

    public static byte[] macAddress() {
        if (null != MAC_ADDRESS) {
            return MAC_ADDRESS.clone();
        }
        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                final byte[] mac = networkInterface.getHardwareAddress();
                if (null == mac) {
                    continue;
                }
                MAC_ADDRESS = mac;
                return MAC_ADDRESS.clone();
            }
            MAC_ADDRESS = MAC_ADDRESS_NOT_FOUND;
            return MAC_ADDRESS.clone();
        } catch (Throwable ignore) {
            MAC_ADDRESS = MAC_ADDRESS_NOT_FOUND;
            return MAC_ADDRESS.clone();
        }
    }

    public static int processId() {
        if (PROCESS_ID != PROCESS_ID_NOT_SET) {
            return PROCESS_ID;
        }
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        // format: "pid@hostname"
        String name = runtime.getName();
        try {
            PROCESS_ID = Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Throwable ignore) {
            PROCESS_ID = PROCESS_ID_NOT_FOUND;
        }
        return PROCESS_ID;
    }

    public static String hostName() {
        if (null != HOST_NAME) {
            return HOST_NAME;
        }
        try {
            HOST_NAME = InetAddress.getLocalHost().getHostName();
            return HOST_NAME;
        } catch (Throwable ignore) {
            HOST_NAME = HOST_NAME_NOT_FOUND;
            return HOST_NAME;
        }
    }

    public static byte[] compressBytesGzip(final byte[] src, final int level) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater defeater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream =
                new DeflaterOutputStream(byteArrayOutputStream, defeater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            defeater.end();
        }
    }

    public static byte[] uncompressBytesGzip(final byte[] src) throws IOException {
        byte[] uncompressData = new byte[src.length];

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            int length;
            while ((length = inflaterInputStream.read(uncompressData, 0, uncompressData.length)) > 0) {
                byteArrayOutputStream.write(uncompressData, 0, length);
            }
            byteArrayOutputStream.flush();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            try {
                inflaterInputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
        }
    }

    public static String crc32CheckSum(byte[] array) {
        CRC32 crc32 = new CRC32();
        // Do not use crc32.update(array) directly for the compatibility, which has been marked as since Java1.9.
        crc32.update(array, 0, array.length);
        return Long.toHexString(crc32.getValue()).toUpperCase(LOCALE);
    }

    public static String md5CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(array);
        return Hex.encodeHexString(digest.digest(), false);
    }

    public static String sha1CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(array);
        return Hex.encodeHexString(digest.digest(), false);
    }

    public static String stackTrace() {
        return stackTrace(Thread.getAllStackTraces());
    }

    public static String stackTrace(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(),
                                                thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s%s%n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable e) {
            result.append(e);
        }
        return result.toString();
    }
}
