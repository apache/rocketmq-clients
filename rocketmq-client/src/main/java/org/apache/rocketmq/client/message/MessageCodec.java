package org.apache.rocketmq.client.message;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.constant.SystemProperty;

public class MessageCodec {
    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;

    public static final int DEFAULT_MESSAGE_COMPRESSION_LEVEL = 5;
    public static final int MESSAGE_COMPRESSION_LEVEL =
            Integer.parseInt(
                    System.getProperty(
                            SystemProperty.MESSAGE_COMPRESSION_LEVEL,
                            Integer.toString(DEFAULT_MESSAGE_COMPRESSION_LEVEL)));

    private MessageCodec() {
    }

    public static String messageProperties2String(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        if (properties != null) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String name = entry.getKey();
                final String value = entry.getValue();

                sb.append(name);
                sb.append(NAME_VALUE_SEPARATOR);
                sb.append(value);
                sb.append(PROPERTY_SEPARATOR);
            }
        }
        return sb.toString();
    }

    public static byte[] encodeMessage(Message message) throws UnsupportedEncodingException {
        throw new UnsupportedOperationException();
    }

    public static byte[] encodeMessages(List<Message> messages) throws UnsupportedEncodingException {
        // TO DO refactor, accumulate in one buffer, avoid copies
        List<byte[]> encodedMessages = new ArrayList<byte[]>(messages.size());
        int allSize = 0;
        for (Message message : messages) {
            byte[] tmp = encodeMessage(message);
            encodedMessages.add(tmp);
            allSize += tmp.length;
        }
        byte[] allBytes = new byte[allSize];
        int pos = 0;
        for (byte[] bytes : encodedMessages) {
            System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
            pos += bytes.length;
        }
        return allBytes;
    }
}
