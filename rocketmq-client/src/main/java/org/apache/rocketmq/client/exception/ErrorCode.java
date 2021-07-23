package org.apache.rocketmq.client.exception;

public enum ErrorCode {
    CLIENT_NOT_STARTED,
    /**
     *
     */
    FETCH_TOPIC_ROUTE_FAILURE,
    /**
     * If topic was not found or partition is empty.
     */
    TOPIC_NOT_FOUND,
    /**
     *
     */
    NO_AVAILABLE_NAME_SERVER,
    NO_PERMISSION,

    FETCH_NAME_SERVER_FAILURE,
    SIGNATURE_FAILURE,

    NO_LISTENER_REGISTERED,
    NOT_SUPPORTED_OPERATION,
    NO_ASSIGNMENT,
    ILLEGAL_FORMAT,
    OTHER;
}
