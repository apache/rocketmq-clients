package org.apache.rocketmq.client.route;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Topic {
    final String arn;
    final String name;
}
