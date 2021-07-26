package org.apache.rocketmq.client.route;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Topic {
    final String arn;
    final String name;
}
