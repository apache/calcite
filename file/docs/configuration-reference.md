# Configuration Reference

This document provides comprehensive configuration options for the Apache Calcite File Adapter.

## Environment Variable Substitution

The File Adapter supports environment variable substitution in all configuration values using the `${VAR_NAME}` or `${VAR_NAME:default}` syntax. This enables dynamic configuration without modifying model files.

### Syntax

- `${VAR_NAME}` - Substitutes with environment variable value; fails if not defined
- `${VAR_NAME:default}` - Substitutes with environment variable value; uses default if not defined

### Type Conversion

Environment variables are automatically converted to appropriate JSON types:
- **Numbers**: `"${PORT:8080}"` → `8080` (unquoted number)
- **Booleans**: `"${ENABLED:true}"` → `true` (unquoted boolean)  
- **Strings**: `"${NAME:value}"` → `"value"` (quoted string)

### Resolution Order

1. Environment variables (`System.getenv()`)
2. System properties (`System.getProperty()`) - useful for testing
3. Default value if specified
4. Error if no value found and no default provided

### Examples

**Basic Usage:**
```json
{
  "schemas": [{
    "name": "${SCHEMA_NAME:sales}",
    "operand": {
      "directory": "${DATA_DIR:/data}",
      "executionEngine": "${ENGINE_TYPE:PARQUET}",
      "batchSize": "${BATCH_SIZE:1000}",
      "ephemeralCache": "${USE_TEMP_CACHE:true}"
    }
  }]
}
```

**Cloud Storage Configuration:**
```json
{
  "operand": {
    "storageType": "s3",
    "storageConfig": {
      "bucketName": "${S3_BUCKET}",
      "region": "${AWS_REGION:us-east-1}",
      "accessKey": "${AWS_ACCESS_KEY_ID}",
      "secretKey": "${AWS_SECRET_ACCESS_KEY}"
    }
  }
}
```

**Multi-Environment Setup:**
```bash
# Development
export ENVIRONMENT=dev
export DATA_DIR=/dev/data
export ENGINE_TYPE=LINQ4J
export LOG_LEVEL=DEBUG

# Production
export ENVIRONMENT=prod
export DATA_DIR=s3://prod-bucket/data
export ENGINE_TYPE=DUCKDB
export LOG_LEVEL=ERROR
```

**Docker/Kubernetes Integration:**
```yaml
# docker-compose.yml
services:
  calcite:
    image: calcite-file-adapter
    environment:
      - DATA_DIR=/data
      - ENGINE_TYPE=PARQUET
      - CACHE_SIZE=2048
      - S3_BUCKET=${S3_BUCKET}
      - AWS_REGION=${AWS_REGION}
```

### Best Practices

1. **Use descriptive variable names**: `CALCITE_FILE_ENGINE_TYPE` instead of `ENGINE`
2. **Always provide defaults** for non-sensitive configuration
3. **Never commit secrets**: Use environment variables for credentials
4. **Document required variables** in your deployment guide
5. **Validate early**: Check for required variables at startup

## Configuration Scope and Hierarchy

### Multi-Engine Architecture for Optimized Workloads

**Key Insight:** The File Adapter allows multiple schemas with different execution engines in a single connection, enabling you to optimize each workload with its ideal engine while still being able to query across all schemas.

### Schema-Level Configuration

Each schema operates as an independent instance with its own:
- **Unique name** (required) - Schema names must be unique within the same connection
- Execution engine (Parquet (default), DuckDB, Arrow, Vectorized, LINQ4J)
- Storage provider (Local, S3, HTTP, SharePoint)
- Memory and performance settings
- Name transformation rules

#### Schema Naming Requirements

- **Uniqueness**: Schema names must be unique within the same connection
- **Case-sensitive**: `"Sales"` and `"sales"` are different schemas
- **No duplicates**: Attempting to create schemas with duplicate names will throw an `IllegalArgumentException`
- **Validation**: Duplicate detection occurs early during schema creation to prevent configuration errors

```json
{
  "schemas": [
    {"name": "sales", "operand": {"directory": "/data/sales"}},
    {"name": "hr", "operand": {"directory": "/data/hr"}},
    {"name": "finance", "operand": {"directory": "/data/finance"}}
  ]
}
```

**Breaking Change Note**: Prior versions allowed duplicate schema names with silent replacement. Current versions validate uniqueness and throw descriptive errors to prevent unexpected behavior.

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

2. **Configure cross-engine materialized views:**
```json
// In the model.json configuration for analytics schema:
{
  "name": "analytics",
  "executionEngine": "parquet",
  "materializations": [{
    "view": "unified_analytics",
    "table": "unified_analytics_mv",
    "sql": "SELECT api.current_price, analytics.prediction, lake.historical_avg FROM realtime_api.prices api JOIN analytics.ml_predictions analytics ON api.symbol = analytics.symbol JOIN data_lake.price_history lake ON api.symbol = lake.ticker"
  }]
}
```
Note: Materialized views are configured in model.json, not created via SQL DDL.

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

Optimized for analytical workloads with advanced SQL features, enhanced with Calcite's pre-optimizer.

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

**Unique Architecture: Pre-Optimizer with Advanced Statistics**

The File Adapter's DuckDB integration includes a sophisticated pre-optimizer layer that enhances DuckDB's native capabilities:

1. **HyperLogLog (HLL) Statistics Integration**
   - Pre-computed cardinality estimates for all columns
   - COUNT(DISTINCT) queries execute in 0ms (sub-millisecond)
   - Statistics not available to standalone DuckDB

2. **Query Interception and Optimization**
   - Calcite's optimizer evaluates queries before DuckDB
   - Replaces expensive operations with pre-computed results
   - Falls back to DuckDB for complex analytical processing

3. **Performance Benefits**
   ```sql
   -- This query returns instantly (0ms) with HLL optimization
   SELECT COUNT(DISTINCT customer_id) FROM large_table;
   
   -- Without HLL: DuckDB would scan all data (1-3ms for 10K rows, seconds for millions)
   -- With HLL: Returns pre-computed estimate immediately
   ```

4. **Automatic Statistics Collection**
   - HLL sketches built during initial data load
   - Cached for subsequent queries
   - Configurable accuracy/memory tradeoff

**Why This Matters:**
- DuckDB alone cannot access these pre-computed statistics
- The pre-optimizer provides "impossible" performance for certain queries
- Combines DuckDB's analytical power with Calcite's optimization intelligence

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
  "materializations": [
    {
      "view": "monthly_sales",        // Name to use in queries
      "table": "monthly_sales_mv",    // Storage file name
      "sql": "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(amount) as total FROM sales GROUP BY YEAR(order_date), MONTH(order_date)"
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

## Apache Iceberg Configuration

### Basic Iceberg Table Configuration

Configure Iceberg tables within the file adapter for advanced time travel and temporal analytics:

```json
{
  "name": "iceberg_orders",
  "format": "iceberg",
  "url": "/path/to/iceberg/orders",
  "catalogType": "hadoop",
  "warehousePath": "/iceberg/warehouse"
}
```

### Time Travel Configuration

#### Single Point-in-Time
```json
{
  "name": "orders_snapshot",
  "format": "iceberg",
  "url": "/path/to/iceberg/orders",
  "snapshotId": 3821550127947089009
}
```

#### Time Range Queries (Recommended)
```json
{
  "name": "orders_timeline",
  "format": "iceberg", 
  "url": "/path/to/iceberg/orders",
  "timeRange": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-02-01T00:00:00Z",
    "snapshotColumn": "snapshot_time"
  }
}
```

### Performance-Optimized Iceberg with DuckDB

For 10-20x performance improvement on temporal analytics:

```json
{
  "name": "high_performance_analytics",
  "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
  "operand": {
    "executionEngine": "duckdb",
    "tables": [
      {
        "name": "sales_history",
        "format": "iceberg",
        "url": "/iceberg/warehouse/sales",
        "timeRange": {
          "start": "2024-01-01T00:00:00Z",
          "end": "2024-12-31T23:59:59Z"
        }
      }
    ]
  }
}
```

### Iceberg Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `format` | String | Yes | - | Must be "iceberg" |
| `url` | String | Yes | - | Path to Iceberg table |
| `catalogType` | String | No | "hadoop" | Catalog type (hadoop, rest) |
| `warehousePath` | String | No | - | Warehouse root path |
| `snapshotId` | Long | No | - | Specific snapshot ID for point-in-time queries |
| `asOfTimestamp` | String | No | - | Query as of specific timestamp |
| `timeRange` | Object | No | - | Time range configuration for temporal queries |
| `timeRange.start` | String | Required if timeRange | - | Start time (ISO-8601) |
| `timeRange.end` | String | Required if timeRange | - | End time (ISO-8601) |
| `timeRange.snapshotColumn` | String | No | "snapshot_time" | Name of snapshot timestamp column |

### Time Range Query Examples

Once configured, you can run powerful temporal analytics:

```sql
-- Trend analysis across snapshots
SELECT 
  snapshot_time,
  COUNT(*) as daily_orders,
  AVG(amount) as avg_order_value
FROM sales_history
GROUP BY snapshot_time
ORDER BY snapshot_time;

-- Compare first and last snapshots
SELECT 
  customer_id,
  SUM(CASE WHEN snapshot_time = (SELECT MIN(snapshot_time) FROM sales_history) 
           THEN amount ELSE 0 END) as initial_spend,
  SUM(CASE WHEN snapshot_time = (SELECT MAX(snapshot_time) FROM sales_history) 
           THEN amount ELSE 0 END) as final_spend
FROM sales_history
GROUP BY customer_id;

-- Time window analysis
SELECT product_id, COUNT(DISTINCT snapshot_time) as snapshot_appearances
FROM sales_history
WHERE snapshot_time BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY product_id
HAVING COUNT(DISTINCT snapshot_time) > 1;
```

## HTML Crawler Configuration

The HTML crawler provides sophisticated control over what generates tables from web pages:

### Data File Discovery Patterns

Control which linked data files are discovered and processed:

```json
{
  "htmlConfig": {
    "crawlConfig": {
      "enabled": true,
      "maxDepth": 2,
      
      // Pattern-based data file discovery
      "dataFilePattern": ".*\\.(csv|xlsx?|parquet|json)$",  // Regex for included files
      "dataFileExcludePattern": ".*(test|temp|backup).*",    // Regex for excluded files
      
      // Set to null to disable data file processing entirely
      // "dataFilePattern": null
    }
  }
}
```

### HTML Table Extraction Control

Fine-tune which HTML tables are extracted:

```json
{
  "htmlConfig": {
    "crawlConfig": {
      // Control HTML table extraction
      "generateTablesFromHtml": true,    // Enable/disable HTML table extraction
      "htmlTableMinRows": 2,              // Minimum rows (filters out headers-only)
      "htmlTableMaxRows": 10000           // Maximum rows (prevents huge tables)
    }
  }
}
```

### Common HTML Crawler Scenarios

**Scenario 1: Only CSV Files from Web Pages**
```json
{
  "crawlConfig": {
    "dataFilePattern": ".*\\.csv$",
    "generateTablesFromHtml": false
  }
}
```

**Scenario 2: Only HTML Tables, No Downloads**
```json
{
  "crawlConfig": {
    "dataFilePattern": null,
    "generateTablesFromHtml": true,
    "htmlTableMinRows": 3
  }
}
```

**Scenario 3: Specific Data Files with Filtered HTML Tables**
```json
{
  "crawlConfig": {
    "dataFilePattern": ".*\\.(csv|parquet)$",
    "dataFileExcludePattern": ".*archive.*",
    "generateTablesFromHtml": true,
    "htmlTableMinRows": 5,
    "htmlTableMaxRows": 1000
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
        "filePatterns": ["*.csv", "*.json", "*.xlsx", "*.html"],
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
        "htmlConfig": {
          "crawlConfig": {
            "enabled": true,
            "dataFilePattern": ".*\\.(csv|xlsx?)$",
            "generateTablesFromHtml": true,
            "htmlTableMinRows": 2
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
          },
          {
            "name": "web_data",
            "url": "https://example.com/data.html"
          }
        ],
        "materializations": [
          {
            "view": "daily_summary",
            "table": "daily_summary_mv",
            "sql": "SELECT DATE(timestamp) as date, COUNT(*) as records FROM events GROUP BY DATE(timestamp)"
          }
        ]
      }
    }
  ]
}
```