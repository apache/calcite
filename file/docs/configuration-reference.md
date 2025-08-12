# Configuration Reference

This document provides comprehensive configuration options for the Apache Calcite File Adapter.

## Configuration Scope and Hierarchy

### Multi-Engine Architecture for Optimized Workloads

**Key Insight:** The File Adapter allows multiple schemas with different execution engines in a single connection, enabling you to optimize each workload with its ideal engine while still being able to query across all schemas.

### Schema-Level Configuration

Each schema operates as an independent instance with its own:
- Execution engine (Parquet, Arrow, DuckDB, LINQ4J)
- Storage provider (Local, S3, HTTP, SharePoint)
- Memory and performance settings
- Name transformation rules
- Statistics and caching configuration

**Example: Hybrid Architecture for Different Workloads**

```json
{
  "schemas": [
    {
      "name": "hot_cache",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/cache",
        "executionEngine": "arrow",     // In-memory, no spillover
        "batchSize": 10000,             // Larger batches for in-memory
        "primeCache": true              // Pre-load on startup
      }
    },
    {
      "name": "data_lake",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory", 
      "operand": {
        "storageType": "s3",
        "bucket": "company-data-lake",
        "executionEngine": "parquet",   // Handles TB-scale with spillover
        "memoryThreshold": 536870912,   // 512MB before spilling to disk
        "spilloverDirectory": "/fast-ssd/spillover"
      }
    },
    {
      "name": "analytics",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/analytics",
        "executionEngine": "duckdb",    // Advanced SQL features
        "duckdbConfig": {
          "memory_limit": "16GB",
          "threads": 32,
          "enable_progress_bar": false
        }
      }
    },
    {
      "name": "realtime_api",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "http",
        "baseUrl": "https://api.example.com",
        "executionEngine": "linq4j",    // Simple row processing
        "refreshInterval": "30 seconds"  // Fresh data
      }
    }
  ]
}
```

### Cross-Schema Queries and Optimization

With multiple schemas configured with different engines, you can:

1. **Query across engines seamlessly:**
```sql
-- Join in-memory cached data with S3 data lake
SELECT 
  cache.user_id,
  cache.session_data,
  lake.historical_purchases
FROM hot_cache.active_users cache
JOIN data_lake.purchase_history lake 
  ON cache.user_id = lake.customer_id
WHERE cache.last_active > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

2. **Create cross-engine materialized views:**
```sql
-- Combine data from multiple sources into optimized view
CREATE MATERIALIZED VIEW unified_analytics AS
SELECT 
  api.current_price,
  analytics.prediction,
  lake.historical_avg
FROM realtime_api.prices api
JOIN analytics.ml_predictions analytics 
  ON api.symbol = analytics.symbol
JOIN data_lake.price_history lake 
  ON api.symbol = lake.ticker
```

3. **Optimize query execution paths:**
- Filter pushdown happens at the schema level
- Each engine optimizes its portion independently
- Calcite handles cross-schema join optimization
- Materialized views can use the most appropriate engine for their workload

### Global Configuration (System Properties)

Some settings are global and configured via Java system properties:

| Property | Description | Default |
|----------|-------------|---------|
| `calcite.spillover.dir` | Base directory for spillover files | System temp directory |
| `calcite.file.statistics.cache.directory` | Directory for statistics cache | `.stats_cache` |
| `-Xmx` | JVM heap size | JVM default |
| `-XX:MaxDirectMemorySize` | Direct memory for Arrow | JVM default |

**Setting System Properties:**
```bash
java -Dcalcite.spillover.dir=/fast-disk/spillover \
     -Dcalcite.file.statistics.cache.directory=/cache/stats \
     -Xmx8g \
     -XX:MaxDirectMemorySize=4g \
     -jar your-application.jar
```

## Schema Factory Configuration

### Environment Variable Support

The File Adapter supports environment variables in configuration values using the `${VAR_NAME}` syntax:

```json
{
  "storageConfig": {
    "accessKey": "${AWS_ACCESS_KEY}",
    "secretKey": "${AWS_SECRET_KEY}",
    "apiToken": "${API_TOKEN}"
  }
}
```

Additionally, some settings can be configured directly via environment variables:

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `CALCITE_STATISTICS_HLL_ENABLED` | Enable HyperLogLog statistics | `true` |
| `CALCITE_STATISTICS_HLL_PRECISION` | HLL precision (4-16) | `14` |
| `CALCITE_STATISTICS_AUTO_GENERATE` | Auto-generate statistics | `true` |
| `CALCITE_STATISTICS_CACHE_MAX_AGE` | Cache max age in ms | `3600000` |
| `ARROW_GPU_ENABLED` | Enable GPU acceleration for Arrow | `true` |

### Basic Properties

| Property | Type | Description | Default | Required | Env Variable |
|----------|------|-------------|---------|----------|--------------|
| `directory` | String | Base directory path for file discovery | - | Yes | Use `${VAR}` syntax |
| `recursive` | Boolean | Enable recursive directory scanning | `true` | No | - |
| `executionEngine` | String | Execution engine selection | `parquet` | No | - |
| `tableNameCasing` | String | Table name transformation strategy | `SMART_CASING` | No | - |
| `columnNameCasing` | String | Column name transformation strategy | `SMART_CASING` | No | - |
| `enableStatistics` | Boolean | Generate column statistics for optimization | `true` | No | `CALCITE_STATISTICS_HLL_ENABLED` |

### Name Casing Strategies

The File Adapter supports flexible name transformation for tables and columns:

| Strategy | Description | Example Input | Example Output |
|----------|-------------|---------------|----------------|
| `SMART_CASING` | Converts to snake_case (default) | `CustomerOrders` | `customer_orders` |
| `UPPER` | Converts to uppercase | `customer_orders` | `CUSTOMER_ORDERS` |
| `LOWER` | Converts to lowercase | `Customer_Orders` | `customer_orders` |
| `UNCHANGED` | Preserves original casing | `CustomerOrders` | `CustomerOrders` |

**Configuration Example:**
```json
{
  "tableNameCasing": "LOWER",      // All table names lowercase
  "columnNameCasing": "UNCHANGED"   // Preserve column names as-is
}
```

**Note:** This is NOT case-sensitivity configuration. These settings control how names are transformed when creating tables/columns from files. Queries still follow Calcite's SQL case-sensitivity rules (typically case-insensitive for unquoted identifiers).

### File Processing Options

| Property | Type | Description | Default | Env Variable |
|----------|------|-------------|---------|--------------|
| `filePatterns` | String[] | Include files matching these patterns | `["*"]` | - |
| `excludePatterns` | String[] | Exclude files matching these patterns | `[]` | - |
| `maxFileSize` | Long | Maximum file size in bytes | `1GB` | - |
| `compressionSupport` | Boolean | Enable compressed file support | `true` | - |
| `encoding` | String | Default file encoding | `UTF-8` | - |

### Performance Options

| Property | Type | Description | Default | Env Variable |
|----------|------|-------------|---------|--------------|
| `cacheDirectory` | String | Directory for Parquet cache files | `.parquet_cache` | Use `${VAR}` syntax |
| `enableDiskSpillover` | Boolean | Allow disk spillover for large datasets | `true` | - |
| `spilloverThreshold` | String | Memory threshold for spillover | `256MB` | - |
| `maxConcurrentFiles` | Integer | Maximum concurrent file processing | `4` | - |
| `batchSize` | Integer | Row batch size for processing | `1000` | - |
| `memoryThreshold` | Long | Memory threshold in bytes | `83886080` | - |
| `primeCache` | Boolean | Pre-load cache on startup | `true` | - |

## Execution Engine Configuration

### Engine Architecture Philosophy

The File Adapter implements **specialized execution engines** rather than a one-size-fits-all approach. Each engine is optimized for specific workload characteristics:

- **Parquet**: Handles unlimited dataset sizes through spillover
- **Arrow**: Maximizes in-memory performance with SIMD vectorization
- **DuckDB**: Provides advanced SQL analytics capabilities
- **LINQ4J**: Offers simple, low-overhead row processing

This specialization ensures optimal performance for each use case without compromising functionality.

### Parquet Engine (Default for Large Datasets)

The Parquet engine specializes in handling datasets larger than available memory by converting all file formats to Parquet with automatic spillover.

```json
{
  "executionEngine": "parquet",
  "parquetConfig": {
    "compression": "snappy",
    "pageSize": "1MB",
    "enableStatistics": true,
    "enableDictionary": true
  }
}
```

### DuckDB Engine

Optimized for analytical workloads with advanced SQL features.

```json
{
  "executionEngine": "duckdb",
  "duckdbConfig": {
    "memoryLimit": "2GB",
    "threads": 4,
    "enableOptimizations": true
  }
}
```

### Arrow Engine

Best for in-memory processing of smaller datasets.

```json
{
  "executionEngine": "arrow",
  "arrowConfig": {
    "batchSize": 1000,
    "enableVectorization": true
  }
}
```

## Storage Provider Configuration

### Overview

Storage providers determine **how** files are accessed. Configure at schema-level to use a specific storage system, or let the adapter auto-detect from URL schemes.

**Selection Priority:**
1. Schema-level `storageType` → Forces ALL files to use that provider
2. Table-level URL scheme → Auto-detected (s3://, https://, etc.)
3. Default → Local file system

### Available Storage Providers

| Provider | Schema Config | URL Scheme | Use Case |
|----------|--------------|------------|----------|
| Local File System | `"storageType": "local"` | `/path` or `file://` | Development, small datasets |
| Amazon S3 | `"storageType": "s3"` | `s3://` | Cloud data lakes |
| HTTP/HTTPS | `"storageType": "http"` | `http://` or `https://` | REST APIs, web data |
| SharePoint | `"storageType": "sharepoint"` | N/A | Enterprise documents |
| Microsoft Graph | `"storageType": "graph"` | N/A | Office 365, OneDrive |
| FTP/SFTP | `"storageType": "ftp"/"sftp"` | `ftp://` or `sftp://` | Legacy systems |

### Basic Configuration Examples

#### Schema-Level (All Files Use Same Provider)
```json
{
  "storageType": "s3",
  "storageConfig": {
    "bucket": "my-bucket",
    "region": "us-east-1"
  }
}
```

#### Table-Level (Mixed Storage)
```json
{
  "tables": [
    {"name": "local_data", "url": "/data/file.csv"},
    {"name": "cloud_data", "url": "s3://bucket/file.parquet"},
    {"name": "api_data", "url": "https://api.com/data.json"}
  ]
}
```

### Detailed Configuration

For complete configuration options for each storage provider, see:
- **[Storage Providers Documentation](storage-providers.md)** - Full details on each provider
- **[S3 Configuration](storage-providers.md#amazon-s3)** - AWS S3 and compatible storage
- **[HTTP/API Configuration](storage-providers.md#httphttps)** - REST APIs and web services
- **[SharePoint Configuration](storage-providers.md#microsoft-sharepoint)** - SharePoint Online/On-premises
- **[FTP/SFTP Configuration](storage-providers.md#ftp-and-sftp)** - File transfer protocols

## Type Mapping Configuration

### CSV Type Inference

```json
{
  "csvConfig": {
    "enableTypeInference": true,
    "inferenceOptions": {
      "sampleSize": 1000,
      "nullStrings": ["", "null", "NULL", "N/A"],
      "dateFormats": ["yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy"],
      "timestampFormats": ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"]
    }
  }
}
```

### Custom Type Mappings

```json
{
  "typeMapping": {
    "customTypes": {
      "id_field": "BIGINT",
      "amount_field": "DECIMAL(10,2)",
      "status_field": "VARCHAR(20)"
    },
    "columnMappings": {
      "customer_id": "id_field",
      "total_amount": "amount_field"
    }
  }
}
```

## Advanced Features Configuration

### Materialized Views

```json
{
  "materializedViews": [
    {
      "name": "monthly_sales",
      "sql": "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(amount) as total FROM sales GROUP BY YEAR(order_date), MONTH(order_date)",
      "refreshInterval": "1 HOUR",
      "dependencies": ["sales"],
      "options": {
        "enableCache": true,
        "compression": "snappy"
      }
    }
  ]
}
```

### Multi-Table JSON Extraction

```json
{
  "jsonConfig": {
    "multiTableExtraction": {
      "enabled": true,
      "tables": [
        {
          "name": "customers",
          "jsonPath": "$.data.customers[*]",
          "columns": {
            "id": "$.id",
            "name": "$.name",
            "email": "$.contact.email"
          }
        }
      ]
    }
  }
}
```

### Partitioned Tables

```json
{
  "partitioning": {
    "enabled": true,
    "autoDetect": true,
    "tables": [
      {
        "name": "sales_data",
        "partitionPattern": "year={year}/month={month}/day={day}/*.parquet",
        "partitionColumns": [
          {"name": "year", "type": "INTEGER"},
          {"name": "month", "type": "INTEGER"},
          {"name": "day", "type": "INTEGER"}
        ]
      }
    ]
  }
}
```

## Security Configuration

### Access Control

```json
{
  "security": {
    "enableAccessControl": true,
    "allowedPaths": ["/data/public", "/data/reports"],
    "deniedPaths": ["/data/private", "/data/sensitive"],
    "filePermissions": {
      "readable": true,
      "writable": false,
      "executable": false
    }
  }
}
```

### Credential Management

```json
{
  "credentials": {
    "provider": "environment",
    "encryption": {
      "enabled": true,
      "algorithm": "AES-256-GCM",
      "keySource": "environment"
    }
  }
}
```

## Environment Variable Usage Guide

### Three Ways to Use Environment Variables

#### 1. Variable Substitution in JSON (`${VAR}` syntax)

Use `${VAR_NAME}` in any string value in your JSON configuration:

```json
{
  "directory": "${DATA_DIR}",
  "storageConfig": {
    "bucket": "${S3_BUCKET}",
    "accessKey": "${AWS_ACCESS_KEY}",
    "secretKey": "${AWS_SECRET_KEY}"
  },
  "cacheDirectory": "${CACHE_DIR}/parquet"
}
```

**Setting the variables:**
```bash
export DATA_DIR=/data/warehouse
export S3_BUCKET=my-data-bucket
export AWS_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export CACHE_DIR=/fast-disk/cache
```

#### 2. Direct Environment Variables

Some settings are automatically read from environment variables:

```bash
# Statistics configuration
export CALCITE_STATISTICS_HLL_ENABLED=true
export CALCITE_STATISTICS_HLL_PRECISION=14
export CALCITE_STATISTICS_AUTO_GENERATE=true

# GPU acceleration
export ARROW_GPU_ENABLED=true
```

#### 3. System Properties (Java -D flags)

System properties override environment variables:

```bash
java -Dcalcite.spillover.dir=/fast-disk/spillover \
     -Dcalcite.file.statistics.cache.directory=/cache/stats \
     -Dcalcite.file.statistics.hll.enabled=true \
     -jar your-app.jar
```

### Priority Order

Configuration values are resolved in this order (highest priority first):
1. System properties (`-D` flags)
2. Direct environment variables (e.g., `CALCITE_STATISTICS_HLL_ENABLED`)
3. JSON configuration with `${VAR}` substitution
4. Default values

### Security Best Practices

**DO:**
- Use environment variables for sensitive data (passwords, tokens, keys)
- Set restrictive permissions on configuration files
- Use credential managers or vaults in production

**DON'T:**
- Hard-code credentials in JSON files
- Commit files with actual credentials to version control
- Log configuration values that may contain secrets

**Example secure configuration:**
```json
{
  "storageConfig": {
    "authentication": {
      "clientId": "${SHAREPOINT_CLIENT_ID}",
      "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
      "tenantId": "${TENANT_ID}"
    }
  }
}
```

## Complete Example

```json
{
  "version": "1.0",
  "defaultSchema": "files",
  "schemas": [
    {
      "name": "files",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data",
        "recursive": true,
        "executionEngine": "parquet",
        "enableStatistics": true,
        "cacheDirectory": ".parquet_cache",
        "filePatterns": ["*.csv", "*.json", "*.xlsx"],
        "excludePatterns": ["*temp*", "*backup*"],
        "storageProvider": {
          "type": "s3",
          "bucket": "my-data-lake",
          "region": "us-east-1"
        },
        "csvConfig": {
          "enableTypeInference": true,
          "inferenceOptions": {
            "sampleSize": 10000
          }
        },
        "tables": [
          {
            "name": "events",
            "url": "events.csv"
          },
          {
            "name": "users",
            "url": "users.json"
          },
          {
            "name": "transactions",
            "url": "transactions.xlsx"
          }
        ],
        "materializedViews": [
          {
            "name": "daily_summary",
            "sql": "SELECT DATE(timestamp) as date, COUNT(*) as records FROM events GROUP BY DATE(timestamp)",
            "refreshInterval": "1 HOUR"
          }
        ]
      }
    }
  ]
}
```