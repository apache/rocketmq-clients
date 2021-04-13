package org.apache.rocketmq.client.message;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.misc.MixAll;

public class MessageCodec {
  public static final char NAME_VALUE_SEPARATOR = 1;
  public static final char PROPERTY_SEPARATOR = 2;

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
    // only need flag, body, properties
    byte[] body = message.getBody();
    int bodyLen = body.length;
    String properties = messageProperties2String(message.getProperties());
    byte[] propertiesBytes = properties.getBytes(MixAll.DEFAULT_CHARSET);
    // note properties length must not more than Short.MAX
    short propertiesLength = (short) propertiesBytes.length;
    int sysFlag = message.getFlag();
    int storeSize =
        4 // 1 TOTALSIZE
            + 4 // 2 MAGICCOD
            + 4 // 3 BODYCRC
            + 4 // 4 FLAG
            + 4
            + bodyLen // 4 BODY
            + 2
            + propertiesLength;
    ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
    // 1 TOTALSIZE
    byteBuffer.putInt(storeSize);

    // 2 MAGICCODE
    byteBuffer.putInt(0);

    // 3 BODYCRC
    byteBuffer.putInt(0);

    // 4 FLAG
    int flag = message.getFlag();
    byteBuffer.putInt(flag);

    // 5 BODY
    byteBuffer.putInt(bodyLen);
    byteBuffer.put(body);

    // 6 properties
    byteBuffer.putShort(propertiesLength);
    byteBuffer.put(propertiesBytes);

    return byteBuffer.array();
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
