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

package org.apache.rocketmq.client.apis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumerBuilder;

/**
 * Builder to set {@link ClientConfiguration}.
 */
public class ClientConfigurationBuilder {
    private static final int MAX_CLIENT_PROPERTIES_ENTRIES = 32;
    private static final int MAX_CLIENT_PROPERTY_KEY_LENGTH = 64;
    private static final int MAX_CLIENT_PROPERTY_VALUE_LENGTH = 256;
    private static final int MAX_CLIENT_PROPERTIES_TOTAL_SIZE_BYTES = 4 * 1024;
    private static final String RESERVED_CLIENT_PROPERTY_PREFIX = "rocketmq.";
    private static final Pattern CLIENT_PROPERTY_KEY_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9_.-]*");

    private String endpoints;
    private SessionCredentialsProvider sessionCredentialsProvider = null;
    private Duration requestTimeout = Duration.ofSeconds(3);
    private boolean sslEnabled = true;
    private String namespace = "";
    private int maxStartupAttempts = 3;
    private final Map<String, String> clientProperties = new LinkedHashMap<>();

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setEndpoints(String endpoints) {
        checkNotNull(endpoints, "endpoints should not be null");
        this.endpoints = endpoints;
        return this;
    }

    /**
     * Config the session credential provider.
     *
     * @param sessionCredentialsProvider session credential provider.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setCredentialProvider(SessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = checkNotNull(sessionCredentialsProvider, "credentialsProvider should not " +
            "be null");
        return this;
    }

    /**
     * Configure request timeout for ordinary RPC.
     *
     * <p>request timeout is 3s by default. Especially, the RPC request timeout for long-polling of
     * {@link SimpleConsumer} is increased by request timeout here based on the
     * {@linkplain SimpleConsumerBuilder#setAwaitDuration(Duration) await duration}.
     *
     * @param requestTimeout RPC request timeout.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout should not be null");
        return this;
    }

    /**
     * Enable or disable the use of Secure Sockets Layer (SSL) for network transport.
     *
     * @param sslEnabled A boolean value indicating whether SSL should be enabled or not.
     * @return The {@link ClientConfigurationBuilder} instance, to allow for method chaining.
     */
    public ClientConfigurationBuilder enableSsl(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    /**
     * Configure namespace for client
     * @param namespace namespace
     * @return The {@link ClientConfigurationBuilder} instance, to allow for method chaining.
     */
    public ClientConfigurationBuilder setNamespace(String namespace) {
        this.namespace = checkNotNull(namespace, "namespace should not be null");
        return this;
    }

    /**
     * Configure maxStartupAttempts for client
     *
     * @param maxStartupAttempts max attempt times when client startup
     * @return The {@link ClientConfigurationBuilder} instance, to allow for method chaining.
     */
    public ClientConfigurationBuilder setMaxStartupAttempts(int maxStartupAttempts) {
        checkArgument(maxStartupAttempts > 0, "maxStartupAttempts should more than 0");
        this.maxStartupAttempts = maxStartupAttempts;
        return this;
    }

    /**
     * Add a client instance property reported to server-side client runtime.
     *
     * @param key property key.
     * @param value property value.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder addClientProperty(String key, String value) {
        validateClientPropertyEntry(key, value);
        Map<String, String> candidate = new LinkedHashMap<>(clientProperties);
        candidate.put(key, value);
        validateClientPropertiesLimits(candidate);
        this.clientProperties.clear();
        this.clientProperties.putAll(candidate);
        return this;
    }

    /**
     * Set client instance properties reported to server-side client runtime, replacing existing properties.
     *
     * @param properties client properties.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setClientProperties(Map<String, String> properties) {
        checkNotNull(properties, "clientProperties should not be null");
        Map<String, String> candidate = new LinkedHashMap<>(properties);
        validateClientProperties(candidate);
        this.clientProperties.clear();
        this.clientProperties.putAll(candidate);
        return this;
    }

    /**
     * Remove a client instance property.
     *
     * @param key property key.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder removeClientProperty(String key) {
        checkNotNull(key, "client property key should not be null");
        this.clientProperties.remove(key);
        return this;
    }

    /**
     * Clear all client instance properties.
     *
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder clearClientProperties() {
        this.clientProperties.clear();
        return this;
    }

    /**
     * Finalize the build of {@link ClientConfiguration}.
     *
     * @return the client configuration builder instance.
     */
    public ClientConfiguration build() {
        checkNotNull(endpoints, "endpoints should not be null");
        checkNotNull(requestTimeout, "requestTimeout should not be null");
        // Keep build() defensive for maps supplied through setClientProperties or future builder paths.
        validateClientProperties(clientProperties);
        return new ClientConfiguration(endpoints, sessionCredentialsProvider, requestTimeout, sslEnabled, namespace,
            maxStartupAttempts, clientProperties);
    }

    private static void validateClientProperties(Map<String, String> properties) {
        validateClientProperties(properties, true);
    }

    private static void validateClientPropertiesLimits(Map<String, String> properties) {
        validateClientProperties(properties, false);
    }

    /**
     * Performs a client-side pre-check for client properties before the settings are reported to the server.
     *
     * <p>This validation mirrors the public proto constraints as closely as possible without depending on generated
     * proto classes in the client-apis module. The server still has final authority and may reject the properties with
     * its own validation result.
     */
    private static void validateClientProperties(Map<String, String> properties, boolean validateEntries) {
        checkArgument(properties.size() <= MAX_CLIENT_PROPERTIES_ENTRIES,
            "clientProperties should not contain more than %s entries", MAX_CLIENT_PROPERTIES_ENTRIES);
        int totalSize = 0;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (validateEntries) {
                validateClientPropertyEntry(key, value);
            }
            totalSize += computeClientPropertySize(key, value);
        }
        checkArgument(totalSize <= MAX_CLIENT_PROPERTIES_TOTAL_SIZE_BYTES,
            "clientProperties total size should not exceed %s bytes", MAX_CLIENT_PROPERTIES_TOTAL_SIZE_BYTES);
    }

    private static void validateClientPropertyEntry(String key, String value) {
        checkNotNull(key, "client property key should not be null");
        checkNotNull(value, "client property value should not be null");
        checkArgument(!key.isEmpty(), "client property key should not be empty");
        checkArgument(key.length() <= MAX_CLIENT_PROPERTY_KEY_LENGTH,
            "client property key length should not exceed %s characters", MAX_CLIENT_PROPERTY_KEY_LENGTH);
        checkArgument(CLIENT_PROPERTY_KEY_PATTERN.matcher(key).matches(),
            "client property key should start with a letter and only contain letters, digits, dot, underscore or "
                + "hyphen");
        checkArgument(!key.startsWith(RESERVED_CLIENT_PROPERTY_PREFIX),
            "client property key should not use reserved prefix %s", RESERVED_CLIENT_PROPERTY_PREFIX);
        checkArgument(value.length() <= MAX_CLIENT_PROPERTY_VALUE_LENGTH,
            "client property value length should not exceed %s characters", MAX_CLIENT_PROPERTY_VALUE_LENGTH);
    }

    /**
     * Computes the serialized size of one {@code client_properties} map entry in protobuf wire format.
     *
     * <p>A {@code map<string, string>} field is encoded as repeated entry messages. Each entry is written as Settings
     * field 9, containing key field 1 and value field 2. Client property keys are constrained to ASCII, so
     * {@link String#length()} equals the UTF-8 byte size for keys; values may contain non-ASCII characters and must use
     * their UTF-8 byte size.
     */
    private static int computeClientPropertySize(String key, String value) {
        int keySize = key.length();
        int valueSize = value.getBytes(StandardCharsets.UTF_8).length;
        int entrySize = computeTagSize(1)
            + computeUInt32SizeNoTag(keySize)
            + keySize
            + computeTagSize(2)
            + computeUInt32SizeNoTag(valueSize)
            + valueSize;
        return computeTagSize(9)
            + computeUInt32SizeNoTag(entrySize)
            + entrySize;
    }

    /**
     * Computes the protobuf wire-format size of a field tag for the given field number.
     */
    private static int computeTagSize(int fieldNumber) {
        return computeUInt32SizeNoTag(fieldNumber << 3);
    }

    /**
     * Computes the protobuf unsigned int32 varint size.
     */
    private static int computeUInt32SizeNoTag(int value) {
        if ((value & (~0 << 7)) == 0) {
            return 1;
        }
        if ((value & (~0 << 14)) == 0) {
            return 2;
        }
        if ((value & (~0 << 21)) == 0) {
            return 3;
        }
        if ((value & (~0 << 28)) == 0) {
            return 4;
        }
        return 5;
    }
}
