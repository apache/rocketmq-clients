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

package org.apache.rocketmq.client.java.logging;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class ProcessIdConverter extends ClassicConverter {
    private static final long PROCESS_ID_NOT_SET = -2L;
    private static final long PROCESS_ID_NOT_FOUND = -1L;
    private static long PROCESS_ID = -2L;

    public ProcessIdConverter() {
    }

    public String convert(ILoggingEvent iLoggingEvent) {
        return String.valueOf(this.processId());
    }

    private long processId() {
        if (PROCESS_ID_NOT_SET == PROCESS_ID) {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName();

            try {
                PROCESS_ID = Integer.parseInt(name.substring(0, name.indexOf(64)));
            } catch (Throwable var4) {
                PROCESS_ID = PROCESS_ID_NOT_FOUND;
            }

        }
        return PROCESS_ID;
    }
}
