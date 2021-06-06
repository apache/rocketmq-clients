package org.apache.rocketmq.utility;

import com.sun.jna.platform.win32.Kernel32;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.SystemUtils;

public class UtilAll {

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private static byte[] IPV4_ADDRESS = null;

    private static final int PROCESS_ID_NOT_SET = -1;
    private static final int PROCESS_ID_NOT_FOUND = -2;
    private static int PROCESS_ID = PROCESS_ID_NOT_SET;

    private UtilAll() {
    }

    public static int processId() {
        if (PROCESS_ID != PROCESS_ID_NOT_SET) {
            return PROCESS_ID;
        }
        if (SystemUtils.IS_OS_WINDOWS) {
            PROCESS_ID = Kernel32.INSTANCE.GetCurrentProcessId();
            return PROCESS_ID;
        }
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC) {
            PROCESS_ID = CLibrary.INSTANCE.getpid();
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

    public static String bytes2string(byte[] src) {
        char[] hexChars = new char[src.length * 2];
        for (int j = 0; j < src.length; j++) {
            int v = src[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] getIpv4AddressBytes() {
        if (null != IPV4_ADDRESS) {
            return IPV4_ADDRESS.clone();
        }
        try {
            final Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            InetAddress address;

            byte[] internalIp = null;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = networkInterfaces.nextElement();
                final Enumeration<InetAddress> inetAddresses = netInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    address = inetAddresses.nextElement();
                    if (!(address instanceof Inet4Address)) {
                        continue;
                    }
                    byte[] ipBytes = address.getAddress();
                    if (!ipClassCheck(ipBytes)) {
                        continue;
                    }
                    if (!address.isSiteLocalAddress()) {
                        IPV4_ADDRESS = ipBytes;
                        return ipBytes;
                    }

                    if (internalIp == null) {
                        internalIp = ipBytes;
                    }
                }
            }
            if (null != internalIp) {
                IPV4_ADDRESS = internalIp;
                return internalIp.clone();
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Throwable t) {
            throw new RuntimeException("Can not get local ip", t);
        }
    }

    public static String getIpv4Address() {
        final byte[] src = getIpv4AddressBytes();
        return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff) + "." + (src[3] & 0xff);
    }

    /**
     * Check ip is a valid address which belong to class A/B/C or not.
     * <p>Class A: 1.0.0.1-126.255.255.254.
     * <p>Class B: 128.1.0.1-191.255.255.254.
     * <p>Class C: 192.0.1.1-223.255.255.254.
     *
     * @param ipBytes source ip bytes.
     * @return check result.
     */
    public static boolean ipClassCheck(byte[] ipBytes) {
        if (ipBytes.length != 4) {
            throw new RuntimeException("Illegal ipv4 bytes");
        }
        if (ipBytes[0] >= (byte) 1 && ipBytes[0] <= (byte) 126) {
            if (ipBytes[1] == (byte) 255 && ipBytes[2] == (byte) 255 && ipBytes[3] == (byte) 255) {
                return false;
            }
            return ipBytes[1] != (byte) 0 || ipBytes[2] != (byte) 0 || ipBytes[3] != (byte) 0;
        }
        if (ipBytes[0] <= (byte) 191) {
            if (ipBytes[2] == (byte) 255 && ipBytes[3] == (byte) 255) {
                return false;
            }
            return ipBytes[2] != (byte) 0 || ipBytes[3] != (byte) 0;
        }
        if (ipBytes[0] <= (byte) 223) {
            if (ipBytes[3] == (byte) 255) {
                return false;
            }
            return ipBytes[3] != (byte) 0;
        }
        return false;
    }

    public static int getThreadParallelCount(ThreadPoolExecutor executor) {
        return executor.getMaximumPoolSize() + executor.getQueue().remainingCapacity();
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

    public static String getCrc32CheckSum(byte[] array) {
        CRC32 crc32 = new CRC32();
        // Do not use crc32.update(array) directly for the compatibility, which has been marked as since Java1.9.
        crc32.update(array, 0, array.length);
        return Long.toHexString(crc32.getValue());
    }

    public static String getMd5CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(array);
        return Hex.encodeHexString(digest.digest());
    }

    public static String getSha1CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(array);
        return Hex.encodeHexString(digest.digest());
    }


}
