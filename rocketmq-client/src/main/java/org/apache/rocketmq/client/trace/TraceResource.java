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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.resources.Resource;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.rocketmq.utility.UtilAll;

/**
 * Factory of a {@link Resource} which provides information about the related info.
 */
public class TraceResource {
    private static final String SERVICE_NAME = "rocketmq-client";
    private static final Resource INSTANCE = buildResource();

    private TraceResource() {
    }

    /**
     * Returns a factory for a {@link Resource} which provides information about the current operating
     * system.
     */
    public static Resource get() {
        return INSTANCE;
    }

    @VisibleForTesting
    static Resource buildResource() {
        AttributesBuilder attributesBuilder = Attributes.builder();
        final Attributes defaultAttributes = Resource.getDefault().getAttributes();
        attributesBuilder.putAll(defaultAttributes);

        attributesBuilder.put(ResourceAttributes.SERVICE_NAME, SERVICE_NAME);
        attributesBuilder.put(ResourceAttributes.HOST_NAME, UtilAll.hostName());

        final Attributes osAttributes = buildOsAttributes();
        final Attributes processAttributes = buildProcessAttributes();
        attributesBuilder.putAll(osAttributes);
        attributesBuilder.putAll(processAttributes);
        return Resource.create(attributesBuilder.build());
    }

    static Attributes buildOsAttributes() {
        AttributesBuilder attributesBuilder = Attributes.builder();
        final String osName = UtilAll.getOsName();
        if (null == osName) {
            return attributesBuilder.build();
        }
        String osSimplifiedName = getOsSimplifiedName(osName);
        if (null != osSimplifiedName) {
            attributesBuilder.put(ResourceAttributes.OS_TYPE, osSimplifiedName);
        }

        String version = UtilAll.getOsVersion();
        String osDescription = null != version ? osName + ' ' + version : osName;
        attributesBuilder.put(ResourceAttributes.OS_DESCRIPTION, osDescription);
        return attributesBuilder.build();
    }

    static Attributes buildProcessAttributes() {
        AttributesBuilder attributesBuilder = Attributes.builder();
        try {
            final long processId = UtilAll.processId();
            String name = System.getProperty("java.runtime.name");
            String version = System.getProperty("java.runtime.version");
            String description = System.getProperty("java.vm.vendor")
                                 + " " + System.getProperty("java.vm.name")
                                 + " " + System.getProperty("java.vm.version");
            attributesBuilder.put(ResourceAttributes.PROCESS_PID, processId);
            attributesBuilder.put(ResourceAttributes.PROCESS_RUNTIME_NAME, name);
            attributesBuilder.put(ResourceAttributes.PROCESS_RUNTIME_VERSION, version);
            attributesBuilder.put(ResourceAttributes.PROCESS_RUNTIME_DESCRIPTION, description);
            String javaHome = UtilAll.getJavaHome();
            final String osName = UtilAll.getOsName();
            if (javaHome != null) {
                StringBuilder executablePath = new StringBuilder(javaHome);
                executablePath
                        .append(File.pathSeparatorChar)
                        .append("bin")
                        .append(File.pathSeparatorChar)
                        .append("java");
                if (null != osName && osName.toLowerCase(UtilAll.LOCALE).startsWith("windows")) {
                    executablePath.append(".exe");
                }
                attributesBuilder.put(ResourceAttributes.PROCESS_EXECUTABLE_PATH, executablePath.toString());
                RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
                StringBuilder commandLine = new StringBuilder(executablePath);
                for (String arg : runtime.getInputArguments()) {
                    commandLine.append(' ').append(arg);
                }
                attributesBuilder.put(ResourceAttributes.PROCESS_COMMAND_LINE, commandLine.toString());
                attributesBuilder.build();
            }
            return attributesBuilder.build();
        } catch (SecurityException ignore) {
            // ignore on purpose.
            return attributesBuilder.build();
        }
    }

    private static String getOsSimplifiedName(String os) {
        os = os.toLowerCase(UtilAll.LOCALE);
        if (os.startsWith("windows")) {
            return ResourceAttributes.OsTypeValues.WINDOWS;
        }
        if (os.startsWith("linux")) {
            return ResourceAttributes.OsTypeValues.LINUX;
        }
        if (os.startsWith("mac")) {
            return ResourceAttributes.OsTypeValues.DARWIN;
        }
        if (os.startsWith("freebsd")) {
            return ResourceAttributes.OsTypeValues.FREEBSD;
        }
        if (os.startsWith("netbsd")) {
            return ResourceAttributes.OsTypeValues.NETBSD;
        }
        if (os.startsWith("openbsd")) {
            return ResourceAttributes.OsTypeValues.OPENBSD;
        }
        if (os.startsWith("dragonflybsd")) {
            return ResourceAttributes.OsTypeValues.DRAGONFLYBSD;
        }
        if (os.startsWith("hp-ux")) {
            return ResourceAttributes.OsTypeValues.HPUX;
        }
        if (os.startsWith("aix")) {
            return ResourceAttributes.OsTypeValues.AIX;
        }
        if (os.startsWith("solaris")) {
            return ResourceAttributes.OsTypeValues.SOLARIS;
        }
        if (os.startsWith("z/os")) {
            return ResourceAttributes.OsTypeValues.Z_OS;
        }
        return null;
    }
}
