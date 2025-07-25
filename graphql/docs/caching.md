<\!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Caching System

## Table of Contents
- [Overview](#overview)
- [Cache Types](#cache-types)
- [Configuration](#configuration)
- [Key Generation](#key-generation)
- [Cache Management](#cache-management)
- [Monitoring](#monitoring)
- [Best Practices](#best-practices)

## Overview

The caching system provides flexible, configurable caching for GraphQL query results with support for both in-memory and distributed caching solutions.

### Features
- Multiple cache providers (in-memory, Redis)
- Configurable TTL
- Automatic cache invalidation
- Thread-safe operations
- Cache statistics and monitoring
- Cache key generation based on query content and context

## Cache Types

### In-Memory Cache
```java
class InMemoryGraphQLCache implements GraphQLCache {
    private final Cache<String, ExecutionResult> cache;

    public InMemoryGraphQLCache(long ttlSeconds) {
        this.cache = CacheBuilder.newBuilder()
            .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
            .recordStats()
            .build();
    }
}
```

Features:
- Built on Guava Cache
- Thread-safe operations
- Automatic resource management
- LRU eviction policy
- Statistics collection
- Best for single-instance deployments

### Redis Cache
```java
class RedisGraphQLCache implements GraphQLCache {
    private final RedisCommands<String, ExecutionResult> redisCommands;
    private final long ttlSeconds;

    public RedisGraphQLCache(String redisUrl, long ttlSeconds) {
        this.redisClient = RedisClient.create(redisUrl);
        this.connection = redisClient.connect(new GraphQLRedisCodec());
        this.redisCommands = connection.sync();
        this.ttlSeconds = ttlSeconds;
    }
}
```

Features:
- Distributed caching
- High availability
- Cluster support
- Pub/sub capabilities
- Atomic operations
- Best for multi-instance deployments

## Configuration

### Environment Variables
```bash
# Cache type selection
export GRAPHQL_CACHE_TYPE=memory|redis

# Cache TTL
export GRAPHQL_CACHE_TTL=300  # 5 minutes

# Redis configuration
export REDIS_URL=redis://localhost:6379
export REDIS_PASSWORD=optional-password
export REDIS_DATABASE=0
```

### Programmatic Configuration
```java
// In-memory cache
GraphQLCacheModule cache = new GraphQLCacheModule.Builder()
    .withCacheType("memory")
    .withTtlSeconds(300)
    .build();

// Redis cache
GraphQLCacheModule cache = new GraphQLCacheModule.Builder()
    .withCacheType("redis")
    .withTtlSeconds(300)
    .withRedisUrl("redis://localhost:6379")
    .build();
```

## Key Generation

### Key Components
- GraphQL query
- User role
- Query variables
- Authentication context

### Implementation
```java
public String generateKey(String query, String role) {
    try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(query);
        keyBuilder.append("|").append(role != null ? role : "");

        byte[] hash = digest.digest(
            keyBuilder.toString().getBytes(StandardCharsets.UTF_8)
        );

        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
        LOGGER.error("Failed to generate cache key", e);
        return null;
    }
}
```

## Cache Management

### Cache Operations
```java
// Put
public void put(String key, ExecutionResult value) {
    cache.put(key, value);
    LOGGER.debug("Cached result for key: {}", key);
}

// Get
public ExecutionResult get(String key) {
    ExecutionResult result = cache.get(key);
    LOGGER.debug("Cache {} for key: {}",
        result != null ? "hit" : "miss", key);
    return result;
}

// Invalidate
public void invalidate(String key) {
    cache.invalidate(key);
    LOGGER.debug("Invalidated cache for key: {}", key);
}
```

### Cache Eviction
```java
// Time-based eviction
.expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)

// Size-based eviction
.maximumSize(maxCacheSize)

// Custom eviction
.removalListener(notification ->
    LOGGER.debug("Evicted cache entry: {}", notification.getKey())
)
```

## Monitoring

### Cache Statistics
```java
public CacheStats getStatistics() {
    return cache.stats();
}

// Example metrics
public class CacheMetrics {
    private long hitCount;
    private long missCount;
    private long loadSuccessCount;
    private long loadFailureCount;
    private double hitRate;
    private double missRate;
    private long evictionCount;
}
```

### Monitoring Integration
```java
// Prometheus metrics
public void recordMetrics() {
    CacheStats stats = cache.stats();

    Metrics.counter("cache.hits").increment(stats.hitCount());
    Metrics.counter("cache.misses").increment(stats.missCount());
    Metrics.gauge("cache.size").set(cache.size());
    Metrics.gauge("cache.hit_rate").set(stats.hitRate());
}
```

## Best Practices

### Cache Keys
1. Include all relevant query components
2. Use consistent serialization
3. Consider case sensitivity
4. Handle null values
5. Use secure hashing

### Cache Sizing
1. Monitor memory usage
2. Set appropriate TT
