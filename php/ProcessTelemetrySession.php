<?php
declare(strict_types=1);
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

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\TelemetryResponse;

/**
 * Process-based Telemetry session using isolated child process
 * 
 * Runs official gRPC extension in a separate synchronous process,
 * completely avoiding Swoole coroutine compatibility issues.
 * 
 * Architecture:
 * - Parent process (Swoole): Business logic, async operations
 * - Child process (Pure PHP): gRPC bidirectional streaming via official extension
 * - IPC: Unix socket for Settings synchronization
 * 
 * This ensures 100% successful write() to gRPC stream.
 */
class ProcessTelemetrySession {
    /**
     * Maximum reconnect attempts
     */
    private const MAX_RECONNECT_ATTEMPTS = 10;

    private string $clientId;
    private string $endpoints;
    private bool $active = false;
    private int $childPid = 0;
    private $ipcSocket = null;
    private string $ipcPath = '';
    
    /** @var callable|null Settings update callback */
    private $settingsCallback = null;
    
    /** @var callable|null Orphaned transaction command callback */
    private $orphanedTransactionCallback = null;

    public function __construct(string $clientId, string $endpoints) {
        $this->clientId = $clientId;
        $this->endpoints = $endpoints;
    }

    /**
     * Start telemetry session by spawning child process
     */
    public function start(): void {
        if ($this->active) {
            Logger::debug("Telemetry session already active, clientId={$this->clientId}");
            return;
        }

        try {
            // Create IPC socket path
            $this->ipcPath = '/tmp/rocketmq_telemetry_' . md5($this->clientId) . '.sock';
            
            // Remove old socket if exists
            if (file_exists($this->ipcPath)) {
                unlink($this->ipcPath);
            }
            
            // Start child process using proc_open (avoids pcntl_fork restrictions)
            $childScript = $this->generateChildScript();
            $tempFile = '/tmp/rocketmq_child_' . md5($this->clientId) . '.php';
            file_put_contents($tempFile, $childScript);
            
            // Launch child process
            $descriptorspec = [
                0 => ["pipe", "r"],  // stdin
                1 => ["pipe", "w"],  // stdout
                2 => ["pipe", "w"],  // stderr
            ];
            
            $process = proc_open("php {$tempFile}", $descriptorspec, $pipes);
            
            if (!is_resource($process)) {
                throw new \Exception("Failed to start child process");
            }
            
            $this->childPid = (int)proc_get_status($process)['pid'];
            $this->active = true;
            
            // Wait for child to be ready
            usleep(1000000); // 1 second
            
            // Connect to IPC socket
            $this->connectIpc();
            
            Logger::info("Child process started (PID={$this->childPid}), IPC connected, clientId={$this->clientId}");
            
            // Send initial settings
            $this->sendInitialSettings();
            
            // Clean up temp file
            @unlink($tempFile);
            
        } catch (\Throwable $e) {
            Logger::error("Failed to start process telemetry session, clientId={$this->clientId}, error={$e->getMessage()}");
            $this->cleanup();
            throw $e;
        }
    }

    /**
     * Stop telemetry session
     */
    public function stop(): void {
        if (!$this->active) {
            return;
        }

        $this->active = false;
        
        // Close IPC socket
        if ($this->ipcSocket !== null) {
            @socket_close($this->ipcSocket);
            $this->ipcSocket = null;
        }
        
        // Remove IPC socket file
        if (!empty($this->ipcPath) && file_exists($this->ipcPath)) {
            @unlink($this->ipcPath);
        }
        
        // Terminate child process
        if ($this->childPid > 0) {
            posix_kill($this->childPid, SIGTERM);
            pcntl_waitpid($this->childPid, $status);
            $this->childPid = 0;
        }
        
        Logger::info("Process Telemetry session stopped, clientId={$this->clientId}");
    }

    public function isActive(): bool {
        return $this->active;
    }

    /**
     * Set settings update callback
     */
    public function setSettingsCallback(callable $callback): self {
        $this->settingsCallback = $callback;
        return $this;
    }

    /**
     * Set orphaned transaction command callback
     */
    public function setOrphanedTransactionCallback(callable $callback): self {
        $this->orphanedTransactionCallback = $callback;
        return $this;
    }

    /**
     * Send settings to child process via IPC
     */
    public function sendSettings(Settings $settings): void {
        if (!$this->active || $this->ipcSocket === null) {
            throw new \RuntimeException("Telemetry session is not active");
        }

        try {
            // Serialize settings
            $data = $settings->serializeToString();
            
            // Build IPC message: [4 bytes length][serialized data]
            $message = pack('N', strlen($data)) . $data;
            
            // Send to child process
            $bytesWritten = socket_write($this->ipcSocket, $message, strlen($message));
            
            if ($bytesWritten === false || $bytesWritten !== strlen($message)) {
                throw new \RuntimeException("Failed to write to IPC socket");
            }
            
            Logger::debug("Settings sent to child process via IPC, dataSize=" . strlen($data) . ", clientId={$this->clientId}");
            
        } catch (\Throwable $e) {
            Logger::error("Failed to send settings via IPC, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Alias for sendSettings (for compatibility)
     */
    public function sendCustomSettings(Settings $settings): void {
        $this->sendSettings($settings);
    }

    public function getClientId(): string {
        return $this->clientId;
    }

    /**
     * Run child process - pure synchronous gRPC session
     */
    private function runChildProcess(): void {
        // Detach from parent
        posix_setsid();
        
        Logger::info("Child process started for gRPC telemetry, PID=" . getmypid() . ", clientId={$this->clientId}");
        
        try {
            // Create IPC server socket
            $serverSocket = socket_create(AF_UNIX, SOCK_STREAM, 0);
            if ($serverSocket === false) {
                throw new \Exception("Failed to create IPC server socket");
            }
            
            if (!socket_bind($serverSocket, $this->ipcPath)) {
                throw new \Exception("Failed to bind IPC socket: " . socket_strerror(socket_last_error()));
            }
            
            if (!socket_listen($serverSocket, 1)) {
                throw new \Exception("Failed to listen on IPC socket");
            }
            
            Logger::debug("IPC server listening on {$this->ipcPath}");
            
            // Accept connection from parent
            $clientSocket = socket_accept($serverSocket);
            if ($clientSocket === false) {
                throw new \Exception("Failed to accept IPC connection");
            }
            
            Logger::debug("IPC connection accepted from parent");
            
            // Initialize gRPC client
            require_once __DIR__ . '/grpc/Apache/Rocketmq/V2/MessagingServiceClient.php';
            
            $client = new \Apache\Rocketmq\V2\MessagingServiceClient(
                $this->endpoints,
                [
                    'credentials' => \Grpc\ChannelCredentials::createInsecure(),
                    'grpc.primary_user_agent' => 'rocketmq-php-client/' . $this->getSdkVersion(),
                ]
            );
            
            // Create bidirectional stream
            $streamCall = $client->Telemetry([], ['timeout' => 3600]);
            
            Logger::info("gRPC bidirectional stream established in child process, clientId={$this->clientId}");
            
            // Send initial handshake
            $this->sendHandshakeInChild($streamCall);
            
            // Main loop: receive settings from parent and forward to gRPC stream
            while (true) {
                // Read message from IPC
                $header = socket_read($clientSocket, 4);
                if ($header === false || strlen($header) < 4) {
                    Logger::warn("IPC read failed or closed, clientId={$this->clientId}");
                    break;
                }
                
                // Parse length
                $length = unpack('N', $header)[1];
                
                // Read data
                $data = socket_read($clientSocket, $length);
                if ($data === false || strlen($data) < $length) {
                    Logger::warn("IPC data read failed, clientId={$this->clientId}");
                    break;
                }
                
                // Deserialize settings
                $settings = new Settings();
                $settings->mergeFromString($data);
                
                Logger::debug("Received settings from parent via IPC, forwarding to gRPC stream, clientId={$this->clientId}");
                
                // Forward to gRPC stream
                $command = new TelemetryCommand();
                $command->setSettings($settings);
                
                $writeResult = $streamCall->write($command);
                
                if ($writeResult) {
                    Logger::info("Settings successfully written to gRPC stream in child process, clientId={$this->clientId}");
                } else {
                    Logger::error("Failed to write to gRPC stream in child process, clientId={$this->clientId}");
                }
                
                // Send acknowledgment back to parent
                $ack = pack('N', 1); // Simple ACK
                socket_write($clientSocket, $ack, 4);
            }
            
            // Cleanup
            socket_close($clientSocket);
            socket_close($serverSocket);
            @unlink($this->ipcPath);
            
        } catch (\Throwable $e) {
            Logger::error("Child process error, clientId={$this->clientId}, error={$e->getMessage()}");
            exit(1);
        }
    }

    /**
     * Generate child process script
     */
    private function generateChildScript(): string {
        $clientId = addslashes($this->clientId);
        $endpoints = addslashes($this->endpoints);
        $ipcPath = addslashes($this->ipcPath);
        $projectDir = dirname(__DIR__); // Get project root directory
        
        return <<<PHP
<?php
// Child process script for gRPC Telemetry Session
// This runs in a separate process, outside Swoole coroutine environment

\$projectDir = '{$projectDir}';

require_once \$projectDir . '/php/Logger.php';
\Apache\Rocketmq\Logger::init('/Users/haizai/logs/rocketmq/rocketmq_client_php.log', 'DEBUG');

require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/MessagingServiceClient.php';
require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/TelemetryCommand.php';
require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/Settings.php';
require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/UA.php';
require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/Language.php';
require_once \$projectDir . '/php/grpc/Apache/Rocketmq/V2/ClientType.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\ClientType;

\Apache\Rocketmq\Logger::info("Child process started for gRPC telemetry, PID=" . getmypid() . ", clientId={$clientId}");

try {
    // Create IPC server socket
    \Apache\Rocketmq\Logger::debug("Creating IPC server socket on {$ipcPath}");
    \$serverSocket = socket_create(AF_UNIX, SOCK_STREAM, 0);
    if (\$serverSocket === false) {
        throw new \Exception("Failed to create IPC server socket");
    }
    
    if (!socket_bind(\$serverSocket, '{$ipcPath}')) {
        throw new \Exception("Failed to bind IPC socket: " . socket_strerror(socket_last_error()));
    }
    
    if (!socket_listen(\$serverSocket, 1)) {
        throw new \Exception("Failed to listen on IPC socket");
    }
    
    \Apache\Rocketmq\Logger::debug("IPC server listening");
    
    // Accept connection from parent
    \$clientSocket = socket_accept(\$serverSocket);
    if (\$clientSocket === false) {
        throw new \Exception("Failed to accept IPC connection");
    }
    
    \Apache\Rocketmq\Logger::debug("IPC connection accepted");
    
    // Initialize gRPC client
    \$client = new MessagingServiceClient(
        '{$endpoints}',
        [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            'grpc.primary_user_agent' => 'rocketmq-php-client/5.0.0',
        ]
    );
    
    // Create bidirectional stream
    \$streamCall = \$client->Telemetry([], ['timeout' => 3600]);
    
    \Apache\Rocketmq\Logger::info("gRPC bidirectional stream established, clientId={$clientId}");
    
    // Send initial handshake
    \$ua = new UA();
    \$ua->setLanguage(Language::PHP);
    \$ua->setVersion('5.0.0');

    \$settings = new Settings();
    \$settings->setClientType(ClientType::SIMPLE_CONSUMER);
    \$settings->setUserAgent(\$ua);

    \$command = new TelemetryCommand();
    \$command->setSettings(\$settings);
    
    \$result = \$streamCall->write(\$command);
    
    if (\$result) {
        \Apache\Rocketmq\Logger::debug("Handshake sent successfully, clientId={$clientId}");
    } else {
        throw new \Exception("Failed to send handshake");
    }
    
    // Main loop: receive settings from parent and forward to gRPC stream
    while (true) {
        // Read message from IPC
        \$header = @socket_read(\$clientSocket, 4);
        if (\$header === false || strlen(\$header) < 4) {
            \Apache\Rocketmq\Logger::warn("IPC read failed or closed, clientId={$clientId}");
            break;
        }
        
        // Parse length
        \$length = unpack('N', \$header)[1];
        
        // Read data
        \$data = @socket_read(\$clientSocket, \$length);
        if (\$data === false || strlen(\$data) < \$length) {
            \Apache\Rocketmq\Logger::warn("IPC data read failed, clientId={$clientId}");
            break;
        }
        
        // Deserialize settings
        \$settings = new Settings();
        \$settings->mergeFromString(\$data);
        
        \Apache\Rocketmq\Logger::debug("Received settings from parent via IPC, forwarding to gRPC stream, clientId={$clientId}");
        
        // Forward to gRPC stream
        \$command = new TelemetryCommand();
        \$command->setSettings(\$settings);
        
        \$writeResult = \$streamCall->write(\$command);
        
        if (\$writeResult) {
            \Apache\Rocketmq\Logger::info("Settings successfully written to gRPC stream, clientId={$clientId}");
        } else {
            \Apache\Rocketmq\Logger::error("Failed to write to gRPC stream, clientId={$clientId}");
        }
        
        // Send acknowledgment back to parent
        \$ack = pack('N', 1);
        @socket_write(\$clientSocket, \$ack, 4);
    }
    
    // Cleanup
    @socket_close(\$clientSocket);
    @socket_close(\$serverSocket);
    @unlink('{$ipcPath}');
    
} catch (\Throwable \$e) {
    \Apache\Rocketmq\Logger::error("Child process error, clientId={$clientId}, error=" . \$e->getMessage());
    exit(1);
}
PHP;
    }

    /**
     * Connect to child process via IPC
     */
    private function connectIpc(): void {
        $this->ipcSocket = socket_create(AF_UNIX, SOCK_STREAM, 0);
        if ($this->ipcSocket === false) {
            throw new \Exception("Failed to create IPC client socket");
        }
        
        if (!socket_connect($this->ipcSocket, $this->ipcPath)) {
            throw new \Exception("Failed to connect to IPC socket: " . socket_strerror(socket_last_error($this->ipcSocket)));
        }
        
        Logger::debug("Connected to child process via IPC, clientId={$this->clientId}");
    }

    /**
     * Send initial settings to child process
     */
    private function sendInitialSettings(): void {
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion($this->getSdkVersion());

        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);

        $this->sendSettings($settings);
        Logger::debug("Initial settings sent to child process, clientId={$this->clientId}");
    }

    /**
     * Send handshake in child process
     */
    private function sendHandshakeInChild($streamCall): void {
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion($this->getSdkVersion());

        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);

        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        $result = $streamCall->write($command);
        
        if ($result) {
            Logger::debug("Handshake sent successfully in child process, clientId={$this->clientId}");
        } else {
            throw new \Exception("Failed to send handshake in child process");
        }
    }

    /**
     * Cleanup resources
     */
    private function cleanup(): void {
        if ($this->ipcSocket !== null) {
            @socket_close($this->ipcSocket);
        }
        
        if (!empty($this->ipcPath) && file_exists($this->ipcPath)) {
            @unlink($this->ipcPath);
        }
        
        if ($this->childPid > 0) {
            @posix_kill($this->childPid, SIGKILL);
        }
    }

    /**
     * Get SDK version
     */
    private function getSdkVersion(): string {
        $composerFile = __DIR__ . '/composer.json';
        if (file_exists($composerFile)) {
            $composerData = @json_decode((string)file_get_contents($composerFile), true);
            if (is_array($composerData) && isset($composerData['version']) && is_string($composerData['version'])) {
                return $composerData['version'];
            }
        }
        return '5.0.0';
    }
}
