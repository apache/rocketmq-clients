package org.apache.rocketmq.client.message;

import java.nio.ByteBuffer;
import org.apache.rocketmq.client.constant.MagicCode;

public enum MessageVersion {
  MESSAGE_VERSION_V1(MagicCode.MESSAGE_MAGIC_CODE.getCode()) {
    @Override
    public int getTopicLengthSize() {
      return 1;
    }

    @Override
    public int getTopicLength(ByteBuffer buffer) {
      return buffer.get();
    }

    @Override
    public int getTopicLength(ByteBuffer buffer, int index) {
      return buffer.get(index);
    }

    @Override
    public void putTopicLength(ByteBuffer buffer, int topicLength) {
      buffer.put((byte) topicLength);
    }
  },
  MESSAGE_VERSION_V2(MagicCode.MESSAGE_MAGIC_CODE_V2.getCode()) {
    @Override
    public int getTopicLengthSize() {
      return 2;
    }

    @Override
    public int getTopicLength(ByteBuffer buffer) {
      return buffer.getShort();
    }

    @Override
    public int getTopicLength(ByteBuffer buffer, int index) {
      return buffer.getShort(index);
    }

    @Override
    public void putTopicLength(ByteBuffer buffer, int topicLength) {
      buffer.putShort((short) topicLength);
    }
  };

  private final int magicCode;

  MessageVersion(int magicCode) {
    this.magicCode = magicCode;
  }

  public static MessageVersion valueOfMagicCode(int magicCode) {
    for (MessageVersion version : MessageVersion.values()) {
      if (version.getMagicCode() == magicCode) {
        return version;
      }
    }

    throw new IllegalArgumentException("Invalid magicCode " + magicCode);
  }

  public int getMagicCode() {
    return magicCode;
  }

  public abstract int getTopicLengthSize();

  public abstract int getTopicLength(ByteBuffer buffer);

  public abstract int getTopicLength(ByteBuffer buffer, int index);

  public abstract void putTopicLength(ByteBuffer buffer, int topicLength);
}
