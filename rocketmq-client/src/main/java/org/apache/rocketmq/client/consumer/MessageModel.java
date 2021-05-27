package org.apache.rocketmq.client.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum MessageModel {
    /**
     * Broadcasting
     */
    BROADCASTING("BROADCASTING"),
    /**
     * Clustering.
     */
    CLUSTERING("CLUSTERING");

    private final String model;
}
