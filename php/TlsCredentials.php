<?php
/**
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

namespace Apache\Rocketmq;

use Grpc\ChannelCredentials;

/**
 * TlsCredentials - TLS/SSL configuration for gRPC connections.
 *
 * Static factory methods provide common TLS configurations.
 * The `toChannelCredentials()` method returns a Grpc\ChannelCredentials
 * instance for use with the gRPC PHP extension.
 */
class TlsCredentials
{

    /**
     * Initialize TLS credentials.
     *
     * @param bool $isInsecure Whether to use insecure (plaintext) connection
     * @param string|null $caCertPath Path to CA certificate file
     * @param string|null $clientCertPath Path to client certificate file
     * @param string|null $clientKeyPath Path to client private key file
     * @param bool $verifyPeer Whether to verify peer certificate
     * @param bool $verifyPeerName Whether to verify peer name
     */
    private function __construct(
        private readonly bool $isInsecure = false,
        private readonly ?string $caCertPath = null,
        private readonly ?string $clientCertPath = null,
        private readonly ?string $clientKeyPath = null,
        private readonly bool $verifyPeer = true,
        private readonly bool $verifyPeerName = true
    ) {
    }

    /**
     * Create insecure credentials (no TLS, plaintext connection).
     *
     * @return self New instance configured for insecure connections
     */
    public static function createInsecure(): self
    {
        return new self(isInsecure: true);
    }

    /**
     * Create default TLS credentials.
     *
     * Enables TLS with peer and peer-name verification using the system CA bundle.
     * Use createWithCa() or createMtls() for custom certificate configurations.
     *
     * @return self New instance configured with secure TLS (verifyPeer=true)
     */
    public static function createDefault(): self
    {
        return new self(
            isInsecure: false,
            verifyPeer: true,
            verifyPeerName: true
        );
    }

    /**
     * Create TLS credentials with a custom CA certificate.
     *
     * @param string $caCertPath Path to CA certificate file
     * @return self New instance configured with the given CA certificate
     */
    public static function createWithCa(string $caCertPath): self
    {
        return new self(
            isInsecure: false,
            caCertPath: $caCertPath,
            verifyPeer: true,
            verifyPeerName: true
        );
    }

    /**
     * Create mutual TLS (mTLS) credentials with client certificate and key.
     *
     * @param string $clientCertPath Path to client certificate file
     * @param string $clientKeyPath Path to client private key file
     * @param string|null $caCertPath Optional CA certificate path (null = system CA)
     * @return self New instance configured for mTLS authentication
     */
    public static function createMtls(
        string $clientCertPath,
        string $clientKeyPath,
        ?string $caCertPath = null
    ): self {
        return new self(
            isInsecure: false,
            caCertPath: $caCertPath,
            clientCertPath: $clientCertPath,
            clientKeyPath: $clientKeyPath,
            verifyPeer: true,
            verifyPeerName: true
        );
    }

    /**
     * Create TLS credentials that skip peer verification (for development only).
     *
     * ⚠️ SECURITY WARNING: This method bypasses TLS certificate verification and allows
     * man-in-the-middle attacks. It should ONLY be used in isolated development/testing
     * environments with full awareness of security risks.
     *
     * NEVER use this in production!
     *
     * @return self New instance with peer verification disabled
     * @internal Not intended for public API usage - use explicit configuration instead
     */
    public static function createInsecureDev(): self
    {
        trigger_error(
            "SECURITY WARNING: TlsCredentials::createInsecureDev() creates credentials with TLS verification disabled. " .
            "This allows man-in-the-middle attacks and should ONLY be used in isolated test environments. " .
            "DO NOT use in production!",
            E_USER_WARNING
        );
        
        Logger::getInstance('TlsCredentials')->error(
            "CRITICAL SECURITY WARNING: Creating insecure TLS credentials with certificate verification disabled! " .
            "This allows man-in-the-middle attacks and compromises connection security. " .
            "ONLY use in isolated development/testing environments. " .
            "NEVER deploy this in production under any circumstances!"
        );
        
        return new self(
            isInsecure: false,
            verifyPeer: false,
            verifyPeerName: false
        );
    }

    /**
     * Convert to Grpc\ChannelCredentials for use with gRPC client.
     *
     * Note: gRPC PHP extension's createSsl() only accepts 3 optional string parameters:
     *   createSsl(string|null $pem_root_certs, string|null $private_key, string|null $cert_chain)
     * It does NOT support verification options. Peer cert verification is always enabled by gRPC C-core.
     *
     * @return \Grpc\ChannelCredentials|null Null for insecure connections, ChannelCredentials instance for TLS
     * @throws \RuntimeException If certificate file cannot be read
     */
    public function toChannelCredentials()
    {
        if ($this->isInsecure) {
            return ChannelCredentials::createInsecure();
        }

        $rootCert = null;
        if ($this->caCertPath !== null) {
            $rootCert = file_get_contents($this->caCertPath);
        }

        $privateKey = null;
        $certChain = null;
        if ($this->clientCertPath !== null && $this->clientKeyPath !== null) {
            $privateKey = file_get_contents($this->clientKeyPath);
            $certChain = file_get_contents($this->clientCertPath);
        }

        // gRPC createSsl() signature: createSsl(string|null, string|null, string|null)
        return ChannelCredentials::createSsl($rootCert, $privateKey, $certChain);
    }

    /**
     * Get gRPC channel arguments derived from this TLS configuration.
     *
     * These args should be merged into the opts array when creating the gRPC stub.
     * For verifyPeer=false (dev only), sets grpc.ssl_target_name_override to bypass
     * hostname verification. Note: gRPC C-core always verifies the certificate itself;
     * only the hostname matching can be overridden.
     *
     * @return array Associative array of gRPC channel args
     */
    public function getChannelArgs(): array
    {
        $args = [];
        if (!$this->isInsecure && !$this->verifyPeerName) {
            $args['grpc.ssl_target_name_override'] = '';
            $args['grpc.default_authority'] = '';
        }
        return $args;
    }

    /**
     * Check if this is insecure (plaintext) credentials.
     *
     * @return bool True if insecure, false if TLS is enabled
     */
    public function isInsecure(): bool
    {
        return $this->isInsecure;
    }

    /**
     * Get the CA certificate path.
     *
     * @return string|null The CA certificate file path, or null if using system CA
     */
    public function getCaCertPath(): ?string
    {
        return $this->caCertPath;
    }

    /**
     * Get the client certificate path (for mTLS).
     *
     * @return string|null The client certificate file path, or null if mTLS is not configured
     */
    public function getClientCertPath(): ?string
    {
        return $this->clientCertPath;
    }

    /**
     * Get the client key path (for mTLS).
     *
     * @return string|null The client private key file path, or null if mTLS is not configured
     */
    public function getClientKeyPath(): ?string
    {
        return $this->clientKeyPath;
    }

    /**
     * Check if peer certificate verification is enabled.
     *
     * @return bool True if peer certificate is verified, false if verification is skipped
     */
    public function shouldVerifyPeer(): bool
    {
        return $this->verifyPeer;
    }

    /**
     * Check if peer name verification is enabled.
     *
     * @return bool True if peer hostname is verified against the certificate, false otherwise
     */
    public function shouldVerifyPeerName(): bool
    {
        return $this->verifyPeerName;
    }
}
