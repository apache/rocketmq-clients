package org.apache.rocketmq.client.consumer.filter;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ExpressionType {
    SQL92("SQL92"),
    TAG("TAG");

    private final String type;
}
