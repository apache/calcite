# Apache Calcite File Adapter

The File Adapter is a high-performance data adapter that enables Apache Calcite to query files in multiple formats as SQL tables. It provides automatic schema discovery, advanced optimization features, and support for various storage systems.

## Features

### Supported File Formats
- **CSV/TSV** with automatic type inference
- **JSON** with multi-table extraction and JSONPath support  
- **YAML** structured data files
- **Parquet** columnar storage format
- **Apache Arrow** in-memory columnar format
- **Excel** (.xlsx, .xls) with multi-sheet support
- **Word** (.docx) table extraction
- **PowerPoint** (.pptx) slide table extraction
- **HTML** table extraction with web crawling
- **Markdown** table extraction
- **XML** with XPath-based table extraction
- **Apache Iceberg** table format

### Storage Systems

Storage providers can be configured at two levels:

**Schema-level** (all files use same provider):
- **Amazon S3** - List and access all files in S3 bucket
- **HTTP/HTTPS** - Access files from web servers
- **SharePoint** - Browse SharePoint document libraries

**Table-level** (auto-detected from URL):
- `s3://bucket/file.csv` → S3 storage
- `https://api.com/data.json` → HTTP storage
- `/local/path/file.parquet` → Local storage

Mix storage types by using explicit table URLs or multiple schemas.

### Performance Features
- **Specialized Execution Engines** - Each optimized for specific workloads:
  - **Parquet**: Large datasets (>2GB) with automatic spillover
  - **Arrow**: In-memory analytics (<2GB) with SIMD vectorization
  - **DuckDB**: Complex analytical queries with advanced SQL features
- **Automatic Parquet Conversion** - Convert all formats to optimized columnar storage
- **HyperLogLog Statistics** - Advanced cardinality estimation for query optimization
- **Materialized Views** - Pre-computed results with automatic refresh
- **Query Optimization** - Column pruning, filter pushdown, join reordering
- **Unlimited Dataset Sizes** - Automatic disk spillover for memory management (Parquet engine)
- **Distributed Caching** - Redis support for cluster environments

### Data Discovery
- **Automatic Schema Discovery** - Zero-configuration table creation
- **Recursive Directory Scanning** - Hierarchical data lake support
- **Glob Pattern Matching** - Process multiple files with patterns like `*.csv`
- **Partitioned Tables** - Automatic partition detection and pruning
- **Multi-Table Extraction** - Extract multiple tables from single JSON/Excel files

## Quick Start

### Basic Configuration

Create a model configuration file:

```json
{
  "version": "1.0",
  "defaultSchema": "FILES",
  "schemas": [
    {
      "name": "FILES",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/path/to/data"
      }
    }
  ]
}
```

### Connect and Query

```bash
# Start Calcite with the File Adapter
./sqlline -m model.json

# Query discovered tables
SELECT * FROM files.customers LIMIT 10;
SELECT COUNT(*) FROM files.sales_data;
```

### Example Queries

```sql
-- Query CSV file with automatic type detection
SELECT customer_id, order_date, total_amount 
FROM sales_2024_q1 
WHERE order_date >= DATE '2024-01-01';

-- Query JSON with nested data extraction
SELECT customer.name, address.city, order_total
FROM customer_orders;

-- Query Excel file with multiple sheets
SELECT * FROM financial_report_summary;
SELECT * FROM financial_report_details;

-- Query across multiple file formats
SELECT s.total_amount, c.customer_name
FROM sales_data s
JOIN customer_info c ON s.customer_id = c.id;
```

## Configuration Reference

### Configuration Scope

**Schema-Level:** Each schema can have completely independent configurations:
- Different execution engines (one schema uses Arrow, another uses Parquet)
- Different storage providers (one schema on S3, another on local disk)
- Different casing rules, statistics settings, etc.

**Environment Variables:** Supported in all string configuration values using `${VAR_NAME}` syntax:
```json
{
  "directory": "${DATA_DIR}",
  "storageConfig": {
    "accessKey": "${AWS_ACCESS_KEY}",
    "secretKey": "${AWS_SECRET_KEY}"
  }
}
```

**Global Settings:** Configured via Java system properties:
- JVM memory settings (`-Xmx`, `-XX:MaxDirectMemorySize`)
- Spillover base directory (`-Dcalcite.spillover.dir`)
- Statistics cache directory (`-Dcalcite.file.statistics.cache.directory`)

### Schema Factory Options

#### Core Configuration

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `directory` | String | Base directory path | Required |
| `recursive` | Boolean | Scan subdirectories | `true` |
| `directoryPattern` | String | Glob pattern for directory discovery | `null` |
| `tables` | Array | Explicit table definitions | `[]` |

#### Execution Engine

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `executionEngine` | String | Engine: `parquet`, `duckdb`, `arrow`, `linq4j` | `parquet` |
| `batchSize` | Integer | Row batch size for processing | `10000` |
| `memoryThreshold` | Long | Memory threshold for spillover (bytes) | `83886080` (80MB) |
| `duckdbConfig` | Object | DuckDB-specific configuration | See DuckDB section |

#### Storage Provider

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `storageType` | String | Storage: `local`, `s3`, `http`, `sharepoint`, `ftp` | `local` |
| `storageConfig` | Object | Storage-specific configuration | `{}` |

#### Name Transformation

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `tableNameCasing` | String | Table names: `SMART_CASING`, `UPPER`, `LOWER`, `UNCHANGED` | `SMART_CASING` |
| `columnNameCasing` | String | Column names: `SMART_CASING`, `UPPER`, `LOWER`, `UNCHANGED` | `SMART_CASING` |

#### Advanced Features

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `materializations` | Array | Materialized view definitions | `[]` |
| `views` | Array | View definitions | `[]` |
| `partitionedTables` | Array | Partitioned table definitions | `[]` |
| `refreshInterval` | String | Default refresh interval (e.g., "5 minutes") | `null` |
| `flatten` | Boolean | Flatten nested JSON/YAML structures | `false` |
| `csvTypeInference` | Object | CSV type inference configuration | `null` |
| `primeCache` | Boolean | Pre-load cache on startup | `true` |

### DuckDB Configuration

When using `executionEngine: "duckdb"`, configure with:

```json
{
  "duckdbConfig": {
    "memory_limit": "4GB",
    "temp_directory": "/tmp/duckdb",
    "threads": 8,
    "max_memory": "80%",
    "enable_progress_bar": false,
    "preserve_insertion_order": true
  }
}
```

### Storage Provider Configuration

#### AWS S3
```json
{
  "storageType": "s3",
  "bucket": "my-data-bucket",
  "region": "us-east-1",
  "accessKey": "${AWS_ACCESS_KEY}",
  "secretKey": "${AWS_SECRET_KEY}"
}
```

#### HTTP/REST API
```json
{
  "storageType": "http",
  "baseUrl": "https://api.example.com/data",
  "authType": "bearer",
  "authToken": "${API_TOKEN}",
  "headers": {
    "User-Agent": "Calcite-File-Adapter/1.0"
  }
}
```

#### Microsoft SharePoint
```json
{
  "storageType": "sharepoint",
  "siteUrl": "https://company.sharepoint.com/sites/data",
  "clientId": "${SHAREPOINT_CLIENT_ID}",
  "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
  "tenantId": "${TENANT_ID}"
}
```

## Advanced Features

### Materialized Views

Create pre-computed views for complex queries:

```json
{
  "materializedViews": [
    {
      "name": "monthly_sales_summary",
      "sql": "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(total) as total_sales FROM sales GROUP BY YEAR(order_date), MONTH(order_date)",
      "refreshInterval": "1 HOUR"
    }
  ]
}
```

### Multi-Table JSON Extraction

Extract multiple tables from a single JSON file:

```json
{
  "jsonTables": [
    {
      "name": "customers",
      "path": "$.customers[*]"
    },
    {
      "name": "orders", 
      "path": "$.orders[*]"
    }
  ]
}
```

### Partitioned Tables

Automatically detect and utilize partitioned data:

```json
{
  "partitionedTables": [
    {
      "name": "sales_data",
      "partitionPattern": "year={year}/month={month}/*.parquet",
      "partitionColumns": ["year", "month"]
    }
  ]
}
```

## Performance Optimization

### Execution Engine Selection

- **Parquet Engine** (Default) - Best overall performance with automatic caching
- **DuckDB Engine** - Optimal for analytical workloads and complex aggregations  
- **Arrow Engine** - Best for in-memory processing of smaller datasets
- **LINQ4J Engine** - Row-based processing for simple queries

### Memory Management

The adapter automatically manages memory usage:
- Large datasets spill to disk when memory limits are reached
- Configurable spillover thresholds and cache sizes
- Redis distributed caching for cluster deployments

### Query Optimization

Automatic optimizations include:
- **Column Pruning** - Read only required columns
- **Filter Pushdown** - Apply filters at the storage level
- **Join Reordering** - Optimize join execution order
- **Partition Pruning** - Skip irrelevant partitions
- **Statistics-based Optimization** - Use HyperLogLog cardinality estimates

## Complete Configuration Example

### Multiple Schemas with Different Engines

```json
{
  "version": "1.0",
  "defaultSchema": "REPORTS",
  "schemas": [
    {
      "name": "REALTIME",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/realtime",
        "executionEngine": "arrow",
        "batchSize": 1000,
        "tableNameCasing": "UPPER"
      }
    },
    {
      "name": "ANALYTICS",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/warehouse",
        "executionEngine": "duckdb",
        "duckdbConfig": {
          "memory_limit": "16GB",
          "temp_directory": "/fast-disk/duckdb-temp"
        }
      }
    },
    {
      "name": "REPORTS",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/reports",
        "executionEngine": "parquet",
        "primeCache": true,
        "tableNameCasing": "LOWER"
      }
    }
  ]
}
```

**Query Examples with Multiple Schemas:**

```sql
-- Query from REALTIME schema (Arrow engine, in-memory)
SELECT * FROM REALTIME.live_events WHERE timestamp > NOW() - INTERVAL '1' HOUR;

-- Query from ANALYTICS schema (DuckDB engine, complex SQL)
SELECT customer_id, 
       SUM(amount) OVER (PARTITION BY region ORDER BY date) as running_total
FROM ANALYTICS.sales_history;

-- Query from REPORTS schema (Parquet engine, spillover)
SELECT * FROM REPORTS.monthly_summary WHERE year = 2024;

-- Cross-schema join
SELECT r.event_id, r.timestamp, a.customer_name
FROM REALTIME.live_events r
JOIN ANALYTICS.customers a ON r.customer_id = a.id;
```

## Documentation

### User Guide
- [Configuration Reference](docs/configuration-reference.md) - Complete configuration options
- [Supported File Formats](docs/supported-formats.md) - All supported formats and features
- [Storage Providers](docs/storage-providers.md) - Cloud, HTTP, SharePoint, and more
- [Performance Tuning](docs/performance-tuning.md) - Optimization strategies and engine selection

### Advanced Features
- [CSV Type Inference](docs/csv-type-inference.md) - Automatic column type detection
- [Apache Iceberg Integration](docs/iceberg-integration.md) - Time travel and schema evolution

### Reference
- [API Reference](docs/api-reference.md) - Programmatic usage
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details.