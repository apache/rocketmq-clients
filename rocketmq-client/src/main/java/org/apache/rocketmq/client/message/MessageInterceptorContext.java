/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.message;

import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MessageInterceptorContext {
    @Builder.Default
    private final MessageHookPoint.PointStatus status = MessageHookPoint.PointStatus.UNSET;
    @Builder.Default
    private final int messageBatchSize = 1;
    @Builder.Default
    private final int messageIndex = 0;
    @Builder.Default
    private final int attempt = 1;
    @Builder.Default
    private final long duration = 0;
    @Builder.Default
    private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    @Builder.Default
    private final Throwable throwable = null;
}
