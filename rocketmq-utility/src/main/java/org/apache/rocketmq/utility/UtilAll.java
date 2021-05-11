package org.apache.rocketmq.utility;

import com.sun.jna.platform.win32.Kernel32;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class UtilAll {

    private static final String OS_NAME = System.getProperty("os.name");
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private UtilAll() {
    }

    public static int processId() {
        // For windows.
        if (OS_NAME.toLowerCase().contains("windows")) {
            return Kernel32.INSTANCE.GetCurrentProcessId();
        }
        // For unix.
        return CLibrary.INSTANCE.getpid();
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

    public static byte[] getIP() {
        try {
            final Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            InetAddress address;

            byte[] internalIP = null;
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
                        return ipBytes;
                    }

                    if (internalIP == null) {
                        internalIP = ipBytes;
                    }
                }
            }
            if (null != internalIP) {
                return internalIP;
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Throwable t) {
            throw new RuntimeException("Can not get local ip", t);
        }
    }

    /**
     * Check ip is a valid address which belong to class A/B/C or not.
     *
     * <p>Class A: 1.0.0.1-126.255.255.254. Class B: 128.1.0.1-191.255.255.254. Class C:
     * 192.0.1.1-223.255.255.254.
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

    public static String shiftTargetPort(String target, int offset) {
        final String[] split = target.split(":");
        final String port = String.valueOf(Integer.parseInt(split[1]) + offset);
        return split[0] + ":" + port;
    }

    public static int getThreadParallelCount(ThreadPoolExecutor executor) {
        return executor.getMaximumPoolSize() + executor.getQueue().remainingCapacity();
    }

    public static byte[] compressByteArray(final byte[] src, final int level) throws IOException {
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

    public static byte[] uncompressByteArray(final byte[] src) throws IOException {
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

    public static SocketAddress host2SocketAddress(String host) {
        return null;
    }


}
