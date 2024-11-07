package org.apache.calcite.adapter.graphql;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import graphql.ExecutionResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A caching module for GraphQL query results supporting both in-memory and Redis-based caching strategies.
 * Compatible with Java 8 and above.
 */
public class GraphQLCacheModule {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLCacheModule.class);
  private final GraphQLCache cache;

  public void close() {
    try {
      if (cache instanceof RedisGraphQLCache) {
        ((RedisGraphQLCache) cache).close();
      }
    } catch(Exception ignored) {}
  }

  public static class Builder {
    @Nullable private String cacheType;
    private Long ttlSeconds;
    private String redisUrl;

    public Builder() {
      this.ttlSeconds = 300L;
      this.redisUrl = "redis://localhost:6379";
    }

    public Builder withEnvironmentConfig() {
      // Environment variables as fallback
      if (this.cacheType == null) {
        this.cacheType = System.getenv("GRAPHQL_CACHE_TYPE");
      }
      if (this.ttlSeconds == 300L) {
        String ttl = System.getenv("GRAPHQL_CACHE_TTL");
        if (ttl != null) {
          this.ttlSeconds = Long.parseLong(ttl);
        }
      }
      if ("redis://localhost:6379".equals(this.redisUrl)) {
        String redisUrl = System.getenv("REDIS_URL");
        if (redisUrl != null) {
          this.redisUrl = redisUrl;
        }
      }
      return this;
    }

    public Builder withOperandConfig(@Nullable Map<String, Object> config) {
      if (config != null) {
        // Operand configuration takes precedence
        Object type = config.get("type");
        if (type != null) {
          this.cacheType = type.toString();
        }

        Object ttl = config.get("ttl");
        if (ttl != null) {
          this.ttlSeconds = ((Number) ttl).longValue();
        }

        Object url = config.get("url");
        if (url != null) {
          this.redisUrl = url.toString();
        }
      }
      return this;
    }

    public Builder withCacheType(String cacheType) {
      this.cacheType = cacheType;
      return this;
    }

    public Builder withTtlSeconds(long ttlSeconds) {
      this.ttlSeconds = ttlSeconds;
      return this;
    }

    public Builder withRedisUrl(String redisUrl) {
      this.redisUrl = redisUrl;
      return this;
    }

    public GraphQLCacheModule build() {
      return new GraphQLCacheModule(this);
    }
  }

  // Private constructor used by Builder
  private GraphQLCacheModule(Builder builder) {
    if (builder.cacheType == null) {
      LOGGER.info("Caching disabled - no cache type specified");
      this.cache = new NoOpGraphQLCache();
    } else if ("redis".equalsIgnoreCase(builder.cacheType)) {
      this.cache = new RedisGraphQLCache(builder.redisUrl, builder.ttlSeconds);
      LOGGER.info("Redis caching enabled with TTL {} seconds", builder.ttlSeconds);
    } else if ("memory".equalsIgnoreCase(builder.cacheType)) {
      this.cache = new InMemoryGraphQLCache(builder.ttlSeconds);
      LOGGER.info("In-memory caching enabled with TTL {} seconds", builder.ttlSeconds);
    } else {
      LOGGER.warn("Unknown cache type '{}' - disabling cache", builder.cacheType);
      this.cache = new NoOpGraphQLCache();
    }
  }

  public @Nullable String generateKey(String query, @Nullable String role) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      String keyBuilder = query +
          "|" + (role != null ? role : "");

      byte[] hash = digest.digest(keyBuilder.getBytes(StandardCharsets.UTF_8));
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

  public void put(String key, ExecutionResult value) {
    cache.put(key, value);
  }

  public @Nullable ExecutionResult get(String key) {
    return cache.get(key);
  }

  public void invalidate(String key) {
    cache.invalidate(key);
  }
}

interface GraphQLCache {
  void put(String key, ExecutionResult value);
  ExecutionResult get(String key);
  void invalidate(String key);
}

class NoOpGraphQLCache implements GraphQLCache {
  private static final Logger LOGGER = LogManager.getLogger(NoOpGraphQLCache.class);

  @Override
  public void put(String key, ExecutionResult value) {
    LOGGER.debug("Caching disabled - not storing result for key: {}", key);
  }

  @Override
  public ExecutionResult get(String key) {
    LOGGER.debug("Caching disabled - no cached result for key: {}", key);
    return null;
  }

  @Override
  public void invalidate(String key) {
    LOGGER.debug("Caching disabled - no cache to invalidate for key: {}", key);
  }
}

class InMemoryGraphQLCache implements GraphQLCache {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryGraphQLCache.class);
  private final Cache<String, ExecutionResult> cache;

  public InMemoryGraphQLCache(long ttlSeconds) {
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
        .build();
  }

  @Override
  public void put(String key, ExecutionResult value) {
    cache.put(key, value);
    LOGGER.debug("Cached result for key: {}", key);
  }

  @Override
  public ExecutionResult get(String key) {
    ExecutionResult result = cache.getIfPresent(key);
    LOGGER.debug("Cache {} for key: {}", result != null ? "hit" : "miss", key);
    return result;
  }

  @Override
  public void invalidate(String key) {
    cache.invalidate(key);
    LOGGER.debug("Invalidated cache for key: {}", key);
  }
}

class RedisGraphQLCache implements GraphQLCache {
  private static final Logger LOGGER = LogManager.getLogger(RedisGraphQLCache.class);
  private final RedisCommands<String, ExecutionResult> redisCommands;
  private final long ttlSeconds;
  private final StatefulRedisConnection<String, ExecutionResult> connection;
  private final RedisClient redisClient;

  public RedisGraphQLCache(String redisUrl, long ttlSeconds) {
    this.redisClient = RedisClient.create(redisUrl);
    this.connection = redisClient.connect(new GraphQLRedisCodec());
    this.redisCommands = connection.sync();
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public void put(String key, ExecutionResult value) {
    redisCommands.setex(key, ttlSeconds, value);
    LOGGER.debug("Cached result in Redis for key: {}", key);
  }

  @Override
  public ExecutionResult get(String key) {
    ExecutionResult result = redisCommands.get(key);
    LOGGER.debug("Redis cache {} for key: {}", result != null ? "hit" : "miss", key);
    return result;
  }

  @Override
  public void invalidate(String key) {
    redisCommands.del(key);
    LOGGER.debug("Invalidated Redis cache for key: {}", key);
  }

  public void close() {
    connection.close();
    redisClient.shutdown();
  }
}
