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

# Implementation Details

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Query Processing](#query-processing)
- [Cache Implementation](#cache-implementation)
- [Type System](#type-system)
- [Security Implementation](#security-implementation)
- [Performance Optimization](#performance-optimization)
- [Error Handling](#error-handling)
- [Monitoring](#monitoring)
- [Deployment](#deployment)

## Architecture Overview

### High-Level Architecture
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   SQL Client    │─────▶    Calcite      │─────▶    GraphQL      │
│                 │     │    Adapter      │     │    Endpoint     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                         ▲
                               │                         │
                               ▼                         │
                        ┌─────────────────┐     ┌─────────────────┐
                        │     Cache       │     │     Redis       │
                        │    Manager     │─────▶│    (Optional)   │
                        └─────────────────┘     └─────────────────┘
```

### Component Interactions
```java
public class GraphQLAdapter {
    private final GraphQLExecutor executor;
    private final GraphQLCacheModule cache;
    private final TypeMapper typeMapper;
    private final SecurityManager security;

    public RelNode convert(SqlNode sqlNode) {
        // Convert SQL to Calcite RelNode
        RelNode relNode = sqlToRelConverter.convert(sqlNode);

        // Apply optimization rules
        relNode = optimizeRelNode(relNode);

        // Convert to GraphQL
        String graphql = relToGraphQLConverter.convert(relNode);

        return executeGraphQL(graphql);
    }
}
```

## Core Components

### GraphQL Executor
```java
public class GraphQLExecutor {
    private final OkHttpClient client;
    private final ObjectMapper mapper;
    private final GraphQLCacheModule cache;

    public ExecutionResult execute(String query) {
        // Check cache
        String cacheKey = cache.generateKey(query);
        ExecutionResult cached = cache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        // Execute query
        Request request = buildRequest(query);
        Response response = client.newCall(request).execute();
        ExecutionResult result = parseResponse(response);

        // Cache result
        cache.put(cacheKey, result);

        return result;
    }
}
```

### Type System Implementation
```java
public class TypeMapper {
    private final Map<String, SqlTypeName> graphqlToSqlTypes;
    private final Map<SqlTypeName, String> sqlToGraphqlTypes;

    public SqlTypeName toSqlType(GraphQLType graphqlType) {
        return graphqlToSqlTypes.get(graphqlType.getName());
    }

    public String toGraphQLType(SqlTypeName sqlType) {
        return sqlToGraphQLTypes.get(sqlType);
    }

    private void initializeTypeMappings() {
        // Basic types
        graphqlToSqlTypes.put("Int", SqlTypeName.INTEGER);
        graphqlToSqlTypes.put("Float", SqlTypeName.FLOAT);
        graphqlToSqlTypes.put("String", SqlTypeName.VARCHAR);
        // ... more mappings
    }
}
```

## Query Processing

### SQL to RelNode Conversion
```java
public class SqlToRelConverter {
    public RelNode convert(SqlNode sqlNode) {
        // Validate SQL
        sqlNode = validator.validate(sqlNode);

        // Convert to RelNode
        RelRoot root = converter.convertQuery(sqlNode, false, true);

        // Apply optimizations
        RelNode optimized = optimizer.optimize(root.rel);

        return optimized;
    }
}
```

### RelNode to GraphQL Conversion
```java
public class RelToGraphQLConverter {
    public String convert(RelNode relNode) {
        GraphQLQuery query = new GraphQLQuery();

        // Handle different node types
        if (relNode instanceof LogicalFilter) {
            addWhereClause(query, (LogicalFilter) relNode);
        } else if (relNode instanceof LogicalSort) {
            addOrderByClause(query, (LogicalSort) relNode);
        }
        // ... handle other node types

        return query.toString();
    }

    private void addWhereClause(GraphQLQuery query, LogicalFilter filter) {
        RexNode condition = filter.getCondition();
        String whereClause = convertRexNodeToGraphQL(condition);
        query.where(whereClause);
    }
}
```

## Cache Implementation

### Cache Key Generation
```java
public class CacheKeyGenerator {
    public String generateKey(String query, Map<String, Object> variables) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(query);

        if (variables != null) {
            // Sort variables to ensure consistent keys
            TreeMap<String, Object> sortedVars = new TreeMap<>(variables);
            for (Map.Entry<String, Object> entry : sortedVars.entrySet()) {
                keyBuilder.append("|")
                    .append(entry.getKey())
                    .append("=")
                    .append(entry.getValue());
            }
        }

        // Generate SHA-256 hash
        return hashString(keyBuilder.toString());
    }
}
```

### Redis Integration
```java
public class RedisCache implements Cache {
    private final RedisClient client;
    private final ObjectMapper mapper;

    @Override
    public void put(String key, Object value, Duration ttl) {
        String json = mapper.writeValueAsString(value);
        client.setex(key, ttl.getSeconds(), json);
    }

    @Override
    public Optional<Object> get(String key) {
        String json = client.get(key);
        if (json == null) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(json, Object.class));
    }
}
```

## Security Implementation

### Authentication
```java
public class SecurityManager {
    public void validateToken(String token) {
        try {
            // Verify JWT token
            Claims claims = Jwts.parser()
                .setSigningKey(getPublicKey())
                .parseClaimsJws(token)
                .getBody();

            // Validate claims
            validateClaims(claims);

        } catch (Exception e) {
            throw new SecurityException("Invalid token", e);
        }
    }

    private void validateClaims(Claims claims) {
        // Check expiration
        Date expiration = claims.getExpiration();
        if (expiration.before(new Date())) {
            throw new SecurityException("Token expired");
        }

        // Validate roles
        List<String> roles = claims.get("roles", List.class);
        validateRoles(roles);
    }
}
```

### Authorization
```java
public class AuthorizationManager {
    public void checkAccess(String username, String resource, String action) {
        // Get user roles
        Set<String> roles = getUserRoles(username);

        // Check permissions
        boolean hasPermission = roles.stream()
            .anyMatch(role -> hasPermission(role, resource, action));

        if (!hasPermission) {
            throw new AccessDeniedException(
                "User " + username + " cannot " + action + " " + resource);
        }
    }
}
```

## Performance Optimization

### Query Optimization
```java
public class QueryOptimizer {
    public RelNode optimize(RelNode node) {
        // Apply standard rules
        RelNode optimized = applyStandardRules(node);

        // Cost-based optimization
        if (shouldUseCBO(optimized)) {
            optimized = applyCostBasedOptimization(optimized);
        }

        // Special handling for common patterns
        optimized = optimizeCommonPatterns(optimized);

        return optimized;
    }

    private RelNode optimizeCommonPatterns(RelNode node) {
        // Optimize JOIN ordering
        if (hasMultipleJoins(node)) {
            node = optimizeJoinOrder(node);
        }

        // Push down predicates
        if (hasPredicates(node)) {
            node = pushDownPredicates(node);
        }

        return node;
    }
}
```

### Connection Pooling
```java
public class ConnectionPool {
    private final Queue<GraphQLConnection> pool;
    private final int maxSize;
    private final Duration maxWait;

    public GraphQLConnection acquire() throws TimeoutException {
        long start = System.currentTimeMillis();
        while (true) {
            GraphQLConnection conn = pool.poll();
            if (conn != null) {
                if (conn.isValid()) {
                    return conn;
                }
                // Connection invalid, create new one
                conn = createConnection();
            }

            if (System.currentTimeMillis() - start > maxWait.toMillis()) {
                throw new TimeoutException("Connection pool timeout");
            }

            Thread.sleep(100);
        }
    }
}
```

## Error Handling

### Error Categories
```java
public class ErrorHandler {
    public void handleError(Throwable error) {
        if (error instanceof QuerySyntaxException) {
            handleSyntaxError((QuerySyntaxException) error);
        } else if (error instanceof SecurityException) {
            handleSecurityError((SecurityException) error);
        } else if (error instanceof ResourceException) {
            handleResourceError((ResourceException) error);
        } else {
            handleUnexpectedError(error);
        }
    }

    private void handleSyntaxError(QuerySyntaxException error) {
        // Log error details
        logger.error("Query syntax error: {}", error.getMessage());

        // Create user-friendly message
        String userMessage = createUserMessage(error);

        // Report to monitoring system
        metrics.incrementCounter("query.syntax.errors");

        throw new UserFacingException(userMessage, error);
    }
}
```

### Retry Logic
```java
public class RetryHandler {
    private final int maxRetries;
    private final Duration initialDelay;
    private final Duration maxDelay;

    public <T> T executeWithRetry(Supplier<T> operation) {
        int attempts = 0;
        Duration delay = initialDelay;

        while (true) {
            try {
                return operation.get();
            } catch (Exception e) {
                if (!isRetryable(e) || attempts >= maxRetries) {
                    throw e;
                }

                attempts++;
                sleep(delay);
                delay = calculateNextDelay(delay);
            }
        }
    }

    private Duration calculateNextDelay(Duration currentDelay) {
        Duration newDelay = currentDelay.multipliedBy(2);
        return newDelay.compareTo(maxDelay) > 0 ? maxDelay : newDelay;
    }
}
```

## Monitoring

### Metrics Collection
```java
public class MetricsCollector {
    private final MeterRegistry registry;

    public void recordQueryExecution(String queryType, long duration) {
        Timer.builder("query.execution")
            .tag("type", queryType)
            .register(registry)
            .record(duration, TimeUnit.MILLISECONDS);
    }

    public void recordCacheOperation(String operation, boolean success) {
        Counter.builder("cache.operations")
            .tag("operation", operation)
            .tag("success", String.valueOf(success))
            .register(registry)
            .increment();
    }
}
```

### Health Checks
```java
public class HealthChecker {
    public Health check() {
        Health.Builder health = new Health.Builder();

        // Check database connection
        if (!checkDatabase()) {
            health.down()
                .withDetail("database", "unreachable");
        }

        // Check cache
        if (!checkCache()) {
            health.down()
                .withDetail("cache", "unreachable");
        }

        // Check memory
        MemoryStatus memStatus = checkMemory();
        if (memStatus.usage > 0.9) {
            health.status("WARNING")
                .withDetail("memory", "high usage: " + memStatus.usage);
        }

        return health.build();
    }
}
```

## Deployment

### Docker Configuration
```dockerfile
FROM openjdk:11-jre-slim

# Add application jar
COPY target/graphql-adapter.jar /app/
COPY config/ /app/config/

# Set environment
ENV JAVA_OPTS="-Xmx2g -XX:+UseG1GC"
ENV GRAPHQL_ENDPOINT="http://hasura:8080/v1/graphql"

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8080/health || exit 1

# Run application
CMD ["java", "-jar", "/app/graphql-adapter.jar"]
```

### Kubernetes Configuration
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: graphql-adapter
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: graphql-adapter
        image: graphql-adapter:latest
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        env:
        - name: GRAPHQL_ENDPOINT
          valueFrom:
            configMapKeyRef:
              name: graphql-config
              key: endpoint
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: redis-credentials
              key: url
```

See also:
- [Features](features.md)
- [Configuration](configuration.md)
- [Limitations](limitations.md)
