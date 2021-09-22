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

package org.apache.rocketmq.client.trace;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.common.AttributeKey;

public class ResourceAttributes {
    /**
     * Logical name of the service.
     *
     * <p>Note: MUST be the same for all instances of horizontally scaled services. If the value was
     * not specified, SDKs MUST fall back to `unknown_service:` concatenated with
     * [`process.executable.name`](process.md#process), e.g. `unknown_service:bash`. If
     * `process.executable.name` is not available, the value MUST be set to `unknown_service`.
     */
    public static final AttributeKey<String> SERVICE_NAME = stringKey("service.name");

    /**
     * Name of the host. On Unix systems, it may contain what the hostname command returns, or the
     * fully qualified hostname, or another name specified by the user.
     */
    public static final AttributeKey<String> HOST_NAME = stringKey("host.name");

    /**
     * The operating system type.
     */
    public static final AttributeKey<String> OS_TYPE = stringKey("os.type");

    /**
     * Human-readable (not intended to be parsed) OS version information, like e.g. reported by `ver`
     * or `lsb_release -a` commands.
     */
    public static final AttributeKey<String> OS_DESCRIPTION = stringKey("os.description");

    /**
     * Process identifier (PID).
     */
    public static final AttributeKey<Long> PROCESS_PID = longKey("process.pid");

    /**
     * The full command used to launch the process as a single string representing the full command.
     * On Windows, can be set to the result of `GetCommandLineW`. Do not set this if you have to
     * assemble it just for monitoring; use `process.command_args` instead.
     */
    public static final AttributeKey<String> PROCESS_COMMAND_LINE = stringKey("process.command_line");

    /**
     * The full path to the process executable. On Linux based systems, can be set to the target of
     * `proc/[pid]/exe`. On Windows, can be set to the result of `GetProcessImageFileNameW`.
     */
    public static final AttributeKey<String> PROCESS_EXECUTABLE_PATH = stringKey("process.executable.path");

    /**
     * The name of the runtime of this process. For compiled native binaries, this SHOULD be the name
     * of the compiler.
     */
    public static final AttributeKey<String> PROCESS_RUNTIME_NAME = stringKey("process.runtime.name");

    /**
     * The version of the runtime of this process, as returned by the runtime without modification.
     */
    public static final AttributeKey<String> PROCESS_RUNTIME_VERSION = stringKey("process.runtime.version");

    /**
     * An additional description about the runtime of the process, for example a specific vendor
     * customization of the runtime environment.
     */
    public static final AttributeKey<String> PROCESS_RUNTIME_DESCRIPTION = stringKey("process.runtime.description");

    public static final class OsTypeValues {

        /**
         * Microsoft Windows.
         */
        public static final String WINDOWS = "windows";

        /**
         * Linux.
         */
        public static final String LINUX = "linux";

        /**
         * Apple Darwin.
         */
        public static final String DARWIN = "darwin";

        /**
         * FreeBSD.
         */
        public static final String FREEBSD = "freebsd";

        /**
         * NetBSD.
         */
        public static final String NETBSD = "netbsd";

        /**
         * OpenBSD.
         */
        public static final String OPENBSD = "openbsd";

        /**
         * DragonFly BSD.
         */
        public static final String DRAGONFLYBSD = "dragonflybsd";

        /**
         * HP-UX (Hewlett Packard Unix).
         */
        public static final String HPUX = "hpux";

        /**
         * AIX (Advanced Interactive eXecutive).
         */
        public static final String AIX = "aix";

        /**
         * Oracle Solaris.
         */
        public static final String SOLARIS = "solaris";

        /**
         * IBM z/OS.
         */
        public static final String Z_OS = "z_os";

        private OsTypeValues() {
        }
    }

    private ResourceAttributes() {
    }
}
