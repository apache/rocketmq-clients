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

public class ProcessIdConverter extends ClassicConverter {
    private static final long PROCESS_ID_NOT_FOUND = -1L;
    private static final String PROCESS_ID = String.valueOf(getProcessId());

    private static long getProcessId() {
        try {
            String processName = ManagementFactory.getRuntimeMXBean().getName();
            return Long.parseLong(processName.split("@")[0]);
        } catch (Throwable t) {
            return PROCESS_ID_NOT_FOUND;
        }
    }

    @Override
    public String convert(ILoggingEvent event) {
        return PROCESS_ID;
    }
}
