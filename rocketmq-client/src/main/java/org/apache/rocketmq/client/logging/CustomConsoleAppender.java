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

package org.apache.rocketmq.client.logging;

import org.apache.rocketmq.shaded.ch.qos.logback.core.ConsoleAppender;

/**
 * Custom the default console appender in logback to archive the goal of conditional on/off.
 *
 * <p> Actual logback provides the similar feature, but it introduces janino. See
 * <a href="http://logback.qos.ch/manual/configuration.html#conditional">Logback Conditional</a>
 * for more details.
 */
public class CustomConsoleAppender<E> extends ConsoleAppender<E> {
    public static final String ENABLE_CONSOLE_APPENDER_KEY = "mq.consoleAppender.enabled";
    private final boolean enabled;

    public CustomConsoleAppender() {
        this.enabled = Boolean.parseBoolean(System.getenv(ENABLE_CONSOLE_APPENDER_KEY)) ||
                       Boolean.parseBoolean(System.getProperty(ENABLE_CONSOLE_APPENDER_KEY));
    }

    @Override
    protected void append(E eventObject) {
        if (enabled) {
            super.append(eventObject);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }
}
