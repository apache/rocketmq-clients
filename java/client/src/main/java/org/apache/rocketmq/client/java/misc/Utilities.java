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

import apache.rocketmq.v2.ReceiveMessageRequest;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.lang3.StringUtils;

public class Utilities {
    public static final int MASTER_BROKER_ID = 0;

    public static final Locale LOCALE = new Locale("zh", "CN");

    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";

    private static final Random RANDOM = new SecureRandom();
    private static final int PROCESS_ID_NOT_SET = -2;
    private static final int PROCESS_ID_NOT_FOUND = -1;
    private static int processId = PROCESS_ID_NOT_SET;

    private static final String HOST_NAME_NOT_FOUND = "HOST_NAME_NOT_FOUND";

    private static final ThreadLocal<String> PROTOCOL_VERSION_THREAD_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<String> HOST_NAME_THREAD_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> MAC_ADDRESS_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_LOWER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
        'e', 'f'};

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
        'E', 'F'};

    private Utilities() {
    }

    public static byte[] macAddress() {
        byte[] macAddress = MAC_ADDRESS_THREAD_LOCAL.get();
        if (null != macAddress) {
            return macAddress.clone();
        }
        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                final byte[] mac = networkInterface.getHardwareAddress();
                if (null == mac) {
                    continue;
                }
                macAddress = mac;
                MAC_ADDRESS_THREAD_LOCAL.set(macAddress);
                return macAddress.clone();
            }
        } catch (Throwable ignore) {
            // Ignore on purpose.
        }
        byte[] randomBytes = new byte[6];
        RANDOM.nextBytes(randomBytes);
        macAddress = randomBytes;
        MAC_ADDRESS_THREAD_LOCAL.set(macAddress);
        return macAddress.clone();
    }

    public static String getProtocolVersion() {
        String protocolVersion = PROTOCOL_VERSION_THREAD_LOCAL.get();
        if (null != protocolVersion) {
            return protocolVersion;
        }
        protocolVersion = ReceiveMessageRequest.class.getName().split("\\.")[2];
        PROTOCOL_VERSION_THREAD_LOCAL.set(protocolVersion);
        return protocolVersion;
    }

    public static int processId() {
        if (processId != PROCESS_ID_NOT_SET) {
            return processId;
        }
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        // Format: "pid@hostname"
        String name = runtime.getName();
        try {
            processId = Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Throwable ignore) {
            processId = PROCESS_ID_NOT_FOUND;
        }
        return processId;
    }

    public static String hostName() {
        String hostName = HOST_NAME_THREAD_LOCAL.get();
        if (null != hostName) {
            return hostName;
        }
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            HOST_NAME_THREAD_LOCAL.set(hostName);
            return hostName;
        } catch (Throwable ignore) {
            hostName = HOST_NAME_NOT_FOUND;
            HOST_NAME_THREAD_LOCAL.set(hostName);
            return hostName;
        }
    }

    public static byte[] compressBytesGZIP(final byte[] src) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        GZIPOutputStream outputStream = new GZIPOutputStream(byteArrayOutputStream);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
        }
    }

    public static byte[] compressBytesZSTD(final byte[] src, final int level) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        ZstdOutputStream outputStream = new ZstdOutputStream(byteArrayOutputStream, level);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
        }
    }

    public static byte[] compressBytesZLIB(final byte[] src, final int level) throws IOException {
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

    public static byte[] compressBytesLZ4(byte[] src) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        LZ4FrameOutputStream outputStream = new LZ4FrameOutputStream(byteArrayOutputStream);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();
            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
        }
    }

    public static byte[] decompressBytes(final byte[] src) throws IOException {
        byte[] uncompressData = new byte[src.length];

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        FilterInputStream filterInputStream = getStreamByMagicCode(src, byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            int length;
            while ((length = filterInputStream.read(uncompressData, 0, uncompressData.length)) > 0) {
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
                filterInputStream.close();
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

    private static FilterInputStream getStreamByMagicCode(byte[] src, InputStream inputStream) throws IOException {
        // Automatically select the appropriate decompression algorithm according to magic code
        // GZIP magic code: 0x1F 0x8B
        // ZLIB magic code: 0x78
        // LZ4 magic code: 0x04 0x22 0x4D 0x18
        // ZSTD magic code: 0x28 0xB5 0x2F 0xFD
        FilterInputStream filterInputStream;
        if ((src[0] & 0xFF) == 0x1F && (src[1] & 0xFF) == 0x8B) {
            filterInputStream = new GZIPInputStream(inputStream);
        } else if ((src[0] & 0xFF) == 0x78) {
            filterInputStream = new InflaterInputStream(inputStream);
        } else if ((src[0] & 0xFF) == 0x04 && (src[1] & 0xFF) == 0x22 && (src[2] & 0xFF) == 0x4D
            && (src[3] & 0xFF) == 0x18) {
            filterInputStream = new LZ4FrameInputStream(inputStream);
        } else if (((src[0] & 0xFF) == 0x28 && (src[1] & 0xFF) == 0xB5 && (src[2] & 0xFF) == 0x2F
            && (src[3] & 0xFF) == 0xFD)) {
            filterInputStream = new ZstdInputStream(inputStream);
        } else {
            throw new IOException("Unknown compression format");
        }
        return filterInputStream;
    }

    public static String encodeHexString(ByteBuffer byteBuffer, boolean toLowerCase) {
        return new String(encodeHex(byteBuffer, toLowerCase));
    }

    public static char[] encodeHex(ByteBuffer byteBuffer, boolean toLowerCase) {
        return encodeHex(byteBuffer, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    public static String encodeHexString(final byte[] data, final boolean toLowerCase) {
        return new String(encodeHex(data, toLowerCase));
    }

    public static char[] encodeHex(final byte[] data, final boolean toLowerCase) {
        return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    protected static char[] encodeHex(final ByteBuffer data, final char[] toDigits) {
        return encodeHex(data.array(), toDigits);
    }

    protected static char[] encodeHex(final byte[] data, final char[] toDigits) {
        final int l = data.length;
        final char[] out = new char[l << 1];
        // Two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
            out[j++] = toDigits[0x0F & data[i]];
        }
        return out;
    }

    public static String crc32CheckSum(byte[] array) {
        CRC32 crc32 = new CRC32();
        // Do not use crc32.update(array) directly for the compatibility, which has been marked as 'since Java1.9'.
        crc32.update(array, 0, array.length);
        return Long.toHexString(crc32.getValue()).toUpperCase(LOCALE);
    }

    public static String md5CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(array);
        return encodeHexString(digest.digest(), false);
    }

    public static String sha1CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(array);
        return encodeHexString(digest.digest(), false);
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

    public static String getOsDescription() {
        final String osName = Utilities.getOsName();
        if (null == osName) {
            return StringUtils.EMPTY;
        }
        String version = Utilities.getOsVersion();
        return null != version ? osName + StringUtils.SPACE + version : osName;
    }

    public static String getOsName() {
        try {
            return System.getProperty(OS_NAME);
        } catch (Throwable t) {
            // ignore on purpose.
            return null;
        }
    }

    public static String getOsVersion() {
        try {
            return System.getProperty(OS_VERSION);
        } catch (Throwable t) {
            // ignore on purpose.
            return null;
        }
    }

    public static String getJavaDescription() {
        return System.getProperty("java.vm.vendor")
            + " " + System.getProperty("java.vm.name")
            + " " + System.getProperty("java.vm.version");
    }
}
