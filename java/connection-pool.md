# RocketMQ Java Client Connection Pool

## Overview

The RocketMQ Java client now supports connection pooling to optimize resource usage when multiple client instances connect to the same endpoints. This feature is particularly useful in scenarios where you have multiple producers or consumers in the same application connecting to the same RocketMQ cluster.

## Benefits

- **Resource Efficiency**: Multiple client instances share the same underlying RPC connections
- **Reduced Connection Overhead**: Fewer TCP connections to the RocketMQ brokers
- **Better Performance**: Reduced connection establishment time for new clients
- **Backward Compatibility**: Existing code continues to work without changes

## How It Works

### Without Connection Pool (Default)
Each `ClientManagerImpl` instance maintains its own set of RPC connections:
```
Client A -> ClientManagerImpl A -> RpcClient A -> Broker
Client B -> ClientManagerImpl B -> RpcClient B -> Broker  
Client C -> ClientManagerImpl C -> RpcClient C -> Broker
```

### With Connection Pool
Multiple `PooledClientManagerImpl` instances share RPC connections:
```
Client A -> PooledClientManagerImpl A ┐
Client B -> PooledClientManagerImpl B ├─> Shared RpcClient -> Broker
Client C -> PooledClientManagerImpl C ┘
```

## Usage

### Enable Connection Pool

```java
ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
    .setEndpoints("localhost:8080")
    .enableConnectionPool(true)  // Enable connection pooling
    .build();
```

### Disable Connection Pool (Default)

```java
ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
    .setEndpoints("localhost:8080")
    .enableConnectionPool(false)  // Disable connection pooling (default)
    .build();
```

## Implementation Details

### Architecture

- `PooledClientManagerImpl` extends `ClientManagerImpl`
- Global static connection pool shared across all instances
- Reference counting for proper connection lifecycle management
- Thread-safe implementation with read-write locks
- **Performance Optimizations:**
  - Lock-free fast path for cached connections using `ConcurrentHashMap`
  - Minimal lock contention for high-frequency `getRpcClient` calls
  - Address order independence using `NormalizedEndpoints`

### Connection Lifecycle

1. **Creation**: When a new endpoint is accessed, a pooled connection is created
2. **Sharing**: Subsequent clients reuse existing connections via reference counting
3. **Cleanup**: Connections are closed when all references are removed
4. **Idle Management**: Unused connections are cleaned up after idle timeout

### Thread Safety

- Global connection pool protected by `ReadWriteLock`
- Reference counting is synchronized
- Local references tracked per client manager instance

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `connectionPoolEnabled` | `false` | Enable/disable connection pooling |

## Performance Optimizations

### 1. High-Performance getRpcClient
- **Fast Path**: Lock-free access for cached connections using `ConcurrentHashMap`
- **Slow Path**: Only acquires locks when cache miss occurs
- **Minimal Contention**: Reduces lock contention in high-frequency scenarios

### 2. Address Order Independence
- **Problem**: `["127.0.0.1:8080", "127.0.0.2:8080"]` vs `["127.0.0.2:8080", "127.0.0.1:8080"]` were treated as different endpoints
- **Solution**: `NormalizedEndpoints` class sorts addresses consistently
- **Benefit**: Same addresses in any order share the same connection

```java
// These will now share the same connection:
String endpoints1 = "127.0.0.1:8080;127.0.0.2:8080";
String endpoints2 = "127.0.0.2:8080;127.0.0.1:8080";
```

## Best Practices

1. **Enable for Multiple Clients**: Use connection pooling when creating multiple producers/consumers
2. **Same Endpoints**: Most beneficial when clients connect to the same broker endpoints  
3. **Resource Management**: Always properly close clients to release connection references
4. **Monitoring**: Monitor connection usage in production environments
5. **High Concurrency**: Optimized for scenarios with frequent `getRpcClient` calls

## Example

See `ConnectionPoolExample.java` and `OptimizedConnectionPoolExample.java` for complete examples demonstrating:
- Creating clients with and without connection pooling
- Multiple clients sharing connections
- Address order independence optimization
- Performance optimization benefits
- Proper resource cleanup

## Migration

Existing applications can enable connection pooling by simply adding:
```java
.enableConnectionPool(true)
```

No other code changes are required.
