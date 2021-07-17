package org.apache.rocketmq.client.message;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class MessageIdUtils {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final String FIX_PREFIX;

    private static long startTime;
    private static long nextStartTime;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(10);

        buffer.position(2);
        buffer.putInt(UtilAll.processId());
        buffer.position(0);

        try {
            buffer.put(UtilAll.getIpv4AddressBytes());
        } catch (Throwable t) {
            buffer.put(createFakeIP());
        }
        buffer.position(6);
        buffer.putInt(MessageIdUtils.class.getClassLoader().hashCode());
        FIX_PREFIX = Hex.encodeHexString(buffer.array(), false);
        setStartTime(System.currentTimeMillis());
    }

    private MessageIdUtils() {
    }

    public static String createUniqId() {
        return FIX_PREFIX + Hex.encodeHexString(createUniqIdBuffer(), false);
    }

    private static byte[] createUniqIdBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 2);
        long current = System.currentTimeMillis();
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        buffer.position(0);
        buffer.putInt((int) (System.currentTimeMillis() - startTime));
        buffer.putShort((short) COUNTER.getAndIncrement());
        return buffer.array();
    }

    private static synchronized void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }
}
