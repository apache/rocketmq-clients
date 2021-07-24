package org.apache.rocketmq.client.misc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.Message;

public class Validators {
    public static final String TOPIC_REGEX = "^[%|a-zA-Z0-9._-]+$";
    public static final Pattern TOPIC_PATTERN = Pattern.compile(TOPIC_REGEX);
    public static final int TOPIC_MAX_LENGTH = 255;

    public static final int MESSAGE_BODY_MAX_SIZE = 1024 * 1024 * 4;

    public static final String CONSUMER_GROUP_REGEX = TOPIC_REGEX;
    public static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile(CONSUMER_GROUP_REGEX);
    public static final int CONSUMER_GROUP_MAX_LENGTH = TOPIC_MAX_LENGTH;

    private Validators() {
    }

    private static boolean regexNotMatched(String origin, Pattern pattern) {
        Matcher matcher = pattern.matcher(origin);
        return !matcher.matches();
    }

    public static void topicCheck(String topic) throws ClientException {
        if (!StringUtils.isNoneBlank(topic)) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "Topic is blank.");
        }
        if (regexNotMatched(topic, TOPIC_PATTERN)) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, String.format("Topic[%s] is illegal.", topic));
        }
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT,
                                        "Topic's length exceeds the threshold, masSize=" + TOPIC_MAX_LENGTH + " bytes");
        }
    }

    public static void consumerGroupCheck(String consumerGroup) throws ClientException {
        if (!StringUtils.isNoneBlank(consumerGroup)) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "ConsumerGroup is blank.");
        }
        if (regexNotMatched(consumerGroup, CONSUMER_GROUP_PATTERN)) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, String.format("ConsumerGroup[%s] is illegal.",
                                                                              consumerGroup));
        }
        if (consumerGroup.length() > CONSUMER_GROUP_MAX_LENGTH) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "ConsumerGroup' length exceeds the threshold, maxSize"
                                                                + CONSUMER_GROUP_MAX_LENGTH + " bytes");
        }
    }

    public static void check(Message message) throws ClientException {
        if (null == message) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "Message is null.");
        }

        topicCheck(message.getTopic());

        final byte[] body = message.getBody();
        if (null == body) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "Message body is null.");
        }
        if (0 == body.length) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "Message body's length is zero.");
        }
        if (body.length > MESSAGE_BODY_MAX_SIZE) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "Message body's length exceeds the threshold, "
                                                                + "maxSize=" + MESSAGE_BODY_MAX_SIZE + " bytes.");
        }
    }
}
