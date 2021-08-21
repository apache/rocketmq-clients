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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class CustomConsoleAppenderTest {

    @Test
    public void testDisabled() {
        CustomConsoleAppender<String> appender = new CustomConsoleAppender<String>();
        assertFalse(appender.isEnabled());
    }

    @Test
    public void testEnabled() {
        System.setProperty(CustomConsoleAppender.ENABLE_CONSOLE_APPENDER_KEY, "true");
        CustomConsoleAppender<String> appender = new CustomConsoleAppender<String>();
        assertTrue(appender.isEnabled());
    }
}