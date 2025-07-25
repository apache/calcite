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

# Configuration Guide

## Table of Contents
- [Basic Configuration](#basic-configuration)
- [Authentication](#authentication)
- [Cache Configuration](#cache-configuration)
- [Performance Tuning](#performance-tuning)
- [Security Configuration](#security-configuration)
- [Advanced Options](#advanced-options)

## Basic Configuration

### Model File
Create a model file `model.json`:

```json
{
  "version": "1.0",
  "defaultSchema": "hasura",
  "schemas": [
    {
      "name": "hasura",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.graphql.GraphQLSchemaFactory",
      "operand": {
        "endpoint": "https://your-hasura-instance.hasura.app/v1/graphql",
        "role": "admin",           // Optional: Hasura role
        "user": "user-id",         // Optional: Hasura user
        "auth": "Bearer token",    // Optional: Authorization header
        "maxConnections": 10,      // Optional: Connection pool size
        "timeout": 30000,          // Optional: Query timeout in ms
        "retryCount": 3           // Optional: Query retry count
      }
    }
  ]
}
```

### Environment Variables
```bash
# Required
export HASURA_ENDPOINT=https://your-hasura-instance.hasura.app/v1/graphql

# Authentication
export HASURA_ADMIN_SECRET=your-admin-secret
export HASURA_JWT_TOKEN=your-jwt-token

# Cache Configuration
export GRAPHQL_CACHE_TYPE=memory|redis
export GRAPHQL_CACHE_TTL=300
export REDIS_URL=redis://localhost:6379

# Performance
export MAX_CONNECTIONS=10
export QUERY_TIMEOUT=30000
export RETRY_COUNT=3
export MAX_ROWS=1000

# Security
export SSL_ENABLED=true
export TRUST_STORE_PATH=/path/to/truststore
export TRUST_STORE_PASSWORD=password
```

## Authentication

### Authentication Methods

#### 1. Admin Secret
```json
{
  "operand": {
    "auth": "Bearer your-admin-secret"
  }
}
```

#### 2. JWT Token
```json
{
  "operand": {
    "auth": "Bearer eyJhbGciOiJ..."
  }
}
```

#### 3. Role-based Access
```json
{
  "operand": {
    "role": "user",
    "user": "user-123"
  }
}
```

### Custom Headers
```json
{
  "operand": {
    "headers": {
      "X-Custom-Header": "value",
      "X-Another-Header": "another-value"
    }
  }
}
```

## Cache Configuration

### In-Memory Cache
```json
{
  "operand": {
    "cache": {
      "type": "memory",
      "ttl": 300,
      "maxSize": "1000",
      "concurrencyLevel": 10
    }
  }
}
```

### Redis Cache
```json
{
  "operand": {
    "cache": {
      "type": "redis",
      "ttl": 300,
      "url": "redis://localhost:6379",
      "password": "optional-password",
      "database": 0,
      "maxConnections": 10
    }
  }
}
```

## Performance Tuning

### Connection Pool
```json
{
  "operand": {
    "pool": {
      "maxSize": 10,
      "minIdle": 2,
      "maxWaitMs": 1000,
      "keepAliveMs": 30000
    }
  }
}
```

### Query Optimization
```json
{
  "operand": {
    "optimizer": {
      "enabled": true,
      "joinReorderingEnabled": true,
      "inferPredicates": true,
      "pushdownEnabled": true
    }
  }
}
```

### Resource Limits
```json
{
  "operand": {
    "limits": {
      "maxRows": 1000,
      "maxExecutionTime": 30000,
      "maxMemoryMb": 1024
    }
  }
}
```

## Security Configuration

### SSL/TLS Settings
```json
{
  "operand": {
    "ssl": {
      "enabled": true,
      "trustStorePath": "/path/to/truststore",
      "trustStorePassword": "password",
      "protocol": "TLS",
      "verifyHostname": true
    }
  }
}
```

### Row-Level Security
```json
{
  "operand": {
    "security": {
      "rowLevelEnabled": true,
      "defaultPredicate": "tenant_id = CURRENT_TENANT()"
    }
  }
}
```

## Advanced Options

### Logging Configuration
```json
{
  "operand": {
    "logging": {
      "level": "DEBUG",
      "queryLogging": true,
      "metricsEnabled": true,
      "slowQueryThresholdMs": 1000
    }
  }
}
```

### Error Handling
```json
{
  "operand": {
    "errorHandling": {
      "retryCount": 3,
      "retryDelayMs": 1000,
      "failFast": false,
      "ignoreWarnings": false
    }
  }
}
```

### Schema Management
```json
{
  "operand": {
    "schema": {
      "cacheEnabled": true,
      "cacheTtl": 3600,
      "refreshInterval": 3600,
      "strictMode": true
    }
  }
}
```

See also:
- [Features](features.md)
- [Caching System](caching.md)
- [Implementation Details](implementation.md)
