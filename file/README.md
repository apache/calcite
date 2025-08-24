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
- **Multiple Execution Engines Per Connection** - Use different engines for different schemas:
  - **Parquet**: Large datasets (>2GB) with automatic spillover
  - **Arrow**: In-memory analytics (<2GB) with SIMD vectorization
  - **DuckDB**: Complex analytical queries with advanced SQL features (10-20x performance improvement)
  - **Mix & Match**: Each schema can use its optimal engine
- **Cross-Schema Materialized Views** - Join data across different engines and storage systems
- **Automatic Parquet Conversion** - Convert all formats to optimized columnar storage
- **HyperLogLog Statistics** - Advanced cardinality estimation for query optimization
- **Query Optimization** - Column pruning, filter pushdown, join reordering
- **Unlimited Dataset Sizes** - Automatic disk spillover for memory management (Parquet engine)
- **Distributed Caching** - Redis support for cluster environments

#### Why Use File Adapter with DuckDB?

The File Adapter + DuckDB combination creates a **declarative data pipeline** that transforms complex data operations into simple SQL:

**Traditional Approach** (Python/pandas):
```python
# Multiple steps, explicit data loading, memory management
df1 = pd.read_csv('sales.csv')
df2 = pd.read_json('customers.json') 
df3 = pd.read_parquet('products.parquet')
merged = df1.merge(df2).merge(df3)
result = merged.groupby('region').agg({'amount': 'sum'}).sort_values('amount', ascending=False)
```

**File Adapter + DuckDB** (Declarative SQL):
```sql
-- Single query, automatic optimization, no memory concerns
SELECT region, SUM(amount) as total_sales
FROM sales s
JOIN customers c ON s.customer_id = c.id  
JOIN products p ON s.product_id = p.id
GROUP BY region
ORDER BY total_sales DESC;
```

**Key Benefits**:
- **Performance**: 10-20x faster than pandas/Spark for analytical workloads
- **Simplicity**: No ETL pipelines - just declare what you want
- **Flexibility**: Mix any file formats (CSV + JSON + Parquet + Excel) seamlessly
- **Scale**: Handle datasets larger than memory with automatic spillover
- **SQL Ecosystem**: Use any SQL tool (BI tools, notebooks, applications)
- **Advanced Optimization**: Pre-optimizer with HyperLogLog statistics provides instant COUNT(DISTINCT) results that DuckDB alone cannot achieve

### Data Discovery
- **Automatic Schema Discovery** - Zero-configuration table creation
- **Recursive Directory Scanning** - Hierarchical data lake support
- **Glob Pattern Matching** - Tables can use glob patterns (e.g., `sales/*.csv`) to combine multiple files into a single table
- **Partitioned Tables** - Automatic partition detection and pruning
- **Multi-Table Extraction** - Extract multiple tables from single files (JSON, Excel, XML, HTML, Markdown, Word, PowerPoint)

## Quick Start

### Basic Configuration

Create a model configuration file:

```json
{
  "version": "1.0",
  "defaultSchema": "files",
  "schemas": [
    {
      "name": "files",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/path/to/data"
      }
    }
  ]
}
```

### Configuration with Environment Variables

Use environment variables for flexible deployment across environments:

```json
{
  "version": "1.0",
  "defaultSchema": "${SCHEMA_NAME:files}",
  "schemas": [
    {
      "name": "${SCHEMA_NAME:files}",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "${DATA_DIR:/data}",
        "executionEngine": "${ENGINE_TYPE:PARQUET}",
        "ephemeralCache": "${USE_TEMP_CACHE:false}",
        "storageType": "${STORAGE_TYPE:local}",
        "storageConfig": {
          "bucketName": "${S3_BUCKET}",
          "region": "${AWS_REGION:us-east-1}"
        }
      }
    }
  ]
}
```

Then set environment variables:
```bash
export DATA_DIR=/production/data
export ENGINE_TYPE=DUCKDB
export STORAGE_TYPE=s3
export S3_BUCKET=my-data-bucket
export AWS_REGION=us-west-2

./sqlline -m model.json
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
-- Files in /path/to/data/:
--   sales_2024_q1.csv
--   customer_orders.json  
--   financial_report.xlsx (with sheets: Summary, Details)
--   sales_data.parquet
--   customer_info.csv

-- Query CSV file (sales_2024_q1.csv → table: sales_2024_q1)
SELECT customer_id, order_date, total_amount 
FROM files.sales_2024_q1 
WHERE order_date >= DATE '2024-01-01';

-- Query JSON with nested data (customer_orders.json → table: customer_orders)
SELECT customer.name, address.city, order_total
FROM files.customer_orders;

-- Query Excel sheets (financial_report.xlsx → tables: financial_report__summary, financial_report__details)
SELECT * FROM files.financial_report__summary;
SELECT * FROM files.financial_report__details;

-- Query across formats (sales_data.parquet, customer_info.csv)
SELECT s.total_amount, c.customer_name
FROM files.sales_data s
JOIN files.customer_info c ON s.customer_id = c.id;
```

## Configuration Reference

### Configuration Scope

**Schema-level:** Each schema can have completely independent configurations:
- Different execution engines (one schema uses Arrow, another uses Parquet)
- Different storage providers (one schema on S3, another on local disk)
- Different casing rules, statistics settings, etc.

**Environment Variables:** Supported in all configuration values using `${VAR_NAME}` or `${VAR_NAME:default}` syntax:
```json
{
  "directory": "${DATA_DIR:/data}",                    // With default value
  "executionEngine": "${CALCITE_FILE_ENGINE_TYPE}",     // Required variable
  "batchSize": "${BATCH_SIZE:1000}",                   // Numbers auto-converted
  "ephemeralCache": "${USE_TEMP_CACHE:true}",          // Booleans auto-converted
  "storageConfig": {
    "accessKey": "${AWS_ACCESS_KEY}",
    "secretKey": "${AWS_SECRET_KEY}"
  }
}
```
Variables are resolved from environment variables first, then system properties (for testing).

**Global Settings:** Configured via Java system properties:
- JVM memory settings (`-Xmx`, `-XX:MaxDirectMemorySize`)
- Spillover base directory (`-Dcalcite.spillover.dir`)
- Statistics cache directory (`-Dcalcite.file.statistics.cache.directory`)

### Key Configuration Options

**Common schema configuration properties:**
- `directory` - Base directory path for file discovery
- `directoryPattern` - Glob pattern to filter files within directory (e.g., `2024/*.csv`, `**/reports/*.json`)
- `executionEngine` - Choose from `parquet`, `arrow`, `duckdb`, `linq4j`
- `storageType` - Storage provider: `local`, `s3`, `http`, `sharepoint`
- `recursive` - Scan subdirectories (default: `true`, ignored if `directoryPattern` is set)
- `tableNameCasing` - Transform table names: `SMART_CASING`, `UPPER`, `LOWER`, `UNCHANGED`

**📚 For complete configuration options, see [Configuration Reference](docs/configuration-reference.md)**

### Execution Engine Configuration

Each schema can use a different execution engine optimized for its workload:
- **Parquet** - Best for large datasets with spillover support
- **Arrow** - In-memory processing with SIMD vectorization
- **DuckDB** - Advanced SQL analytics features
- **LINQ4J** - Simple row-based processing

**📚 For detailed configuration, see [Configuration Reference](docs/configuration-reference.md)**

### Storage Provider Configuration

The File Adapter supports multiple storage systems (Local, S3, HTTP/HTTPS, SharePoint, FTP/SFTP) with automatic detection or explicit configuration.

**Quick Examples:**
```json
{
  "storageType": "s3",           // Use S3 for all files in schema
  "storageConfig": {
    "bucket": "my-data-bucket",
    "region": "us-east-1"
  }
```

Or use URL-based auto-detection:
- `s3://bucket/file.parquet` → S3 Storage
- `https://api.com/data.json` → HTTP Storage  
- `/local/path/file.csv` → Local Storage

**📚 For complete configuration details, see [Storage Providers Documentation](docs/storage-providers.md)**

## Advanced Features

### Multi-Engine Optimization Pattern

**Optimize different workloads with multiple execution engines in a single connection:**

```json
{
  "schemas": [
    {
      "name": "hot_data",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/hot",
        "executionEngine": "arrow",  // In-memory for frequently accessed data
        "batchSize": 10000
      }
    },
    {
      "name": "warehouse",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "s3://data-lake/warehouse",
        "executionEngine": "parquet",  // Spillover for massive datasets
        "storageType": "s3",
        "memoryThreshold": 268435456  // 256MB before spillover
      }
    },
    {
      "name": "analytics",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/analytics",
        "executionEngine": "duckdb",  // Complex analytical queries
        "duckdbConfig": {
          "memory_limit": "8GB",
          "threads": 16
        }
      }
    }
  ]
}
```

**Query across different engines seamlessly:**
```sql
-- Join hot in-memory data with warehouse data
SELECT h.user_id, h.session_id, w.purchase_total
FROM hot_data.active_sessions h
JOIN warehouse.purchases w ON h.user_id = w.user_id
WHERE h.last_activity > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Query combining multiple engines
SELECT 
  a.product_category,
  COUNT(DISTINCT h.user_id) as active_users,
  SUM(w.revenue) as total_revenue
FROM analytics.product_metrics a
JOIN hot_data.user_activity h ON a.product_id = h.product_id  
JOIN warehouse.sales w ON a.product_id = w.product_id
GROUP BY a.product_category;
```

### Materialized Views (Configuration Only)

**Note:** The File Adapter is read-only. Materialized views are defined in the model configuration, not via SQL DDL.

Configure pre-computed views in your schema (requires Parquet execution engine):

```json
{
  "executionEngine": "parquet",  // Required for materialized views
  "materializedViews": [
    {
      "view": "monthly_summary",      // Name used in SQL queries
      "table": "monthly_sales_data",  // Filename for cached Parquet file (.parquet added)
      "sql": "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(total) as total_sales FROM sales GROUP BY YEAR(order_date), MONTH(order_date)"
    }
  ]
}
```

**Key properties:**
- `view`: The table name you'll use in SQL queries (e.g., `SELECT * FROM monthly_summary`)
- `table`: The filename for the materialized Parquet file (stored as `.materialized_views/monthly_sales_data.parquet`)
- `sql`: The query to materialize (executed once on first access)

**Cross-Schema Materialized Views:**

The schema containing the MV must use Parquet engine, but can query data from any schema:

```json
{
  "name": "analytics",
  "executionEngine": "parquet",  // This schema must use parquet
  "materializedViews": [
    {
      "view": "unified_customer_view",
      "table": "unified_customer_view",
      "sql": "SELECT c.customer_id, c.name, s3.lifetime_value FROM hot_data.customers c JOIN warehouse.customer_analytics s3 ON c.customer_id = s3.id"
    }
  ]
}
```

Once configured, query the materialized view like any table:
```sql
SELECT * FROM analytics.unified_customer_view WHERE lifetime_value > 1000;
```

### Multi-Table Extraction

Extract multiple tables from various file formats:

**JSON Files** - Extract different arrays as separate tables:
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

**Excel Files** - Each sheet becomes a separate table:
- `financial_report.xlsx` → `financial_report__summary`, `financial_report__details`, `financial_report__charts`

**XML Files** - Extract different elements as tables:
```json
{
  "xmlTables": [
    {
      "name": "products",
      "xpath": "//catalog/product"
    },
    {
      "name": "categories",
      "xpath": "//catalog/category"
    }
  ]
}
```

**HTML Files** - Each `<table>` element becomes a separate table:
- `report.html` → `report__table_1`, `report__table_2`, `report__table_3`

**Word Documents** - Extract all tables:
- `document.docx` → `document__table_1`, `document__table_2`

**PowerPoint Presentations** - Tables include slide context:
- `presentation.pptx` with titled slide → `presentation__slide_title__table_name`
- `presentation.pptx` with untitled slide → `presentation__slide2__table_name`

**Markdown Files** - Extract all markdown tables:
- `documentation.md` → `documentation__table_1`, `documentation__table_2`

### Glob Pattern Tables

Tables can use glob patterns (`*`, `?`, `[]`) to combine multiple files into a single table:

```json
{
  "tables": [
    {
      "name": "all_sales",
      "url": "sales/*.csv"  // Combines all CSV files in sales directory
    },
    {
      "name": "yearly_data",
      "url": "file:///data/2024-*.parquet"  // All 2024 Parquet files
    },
    {
      "name": "logs",
      "url": "logs/**/*.json"  // All JSON files recursively
    },
    {
      "name": "s3_data",
      "url": "s3://bucket/path/2024-*.csv"  // S3 glob pattern (limited support)
    }
  ]
}
```

**Storage Support for Glob Patterns:**
- **Local files** (`file://` or bare paths): ✅ Supported
- **S3** (`s3://`): ✅ Supported
- **HTTP/HTTPS**: ❌ Not supported - no directory listing capability
- **FTP/SFTP**: ❌ Not supported

The adapter automatically:
- Detects glob patterns in local file paths
- Creates a GlobParquetTable that consolidates matching files
- Caches the combined data as Parquet for efficient querying
- Supports refresh intervals for updating when files change

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

## Refresh and Change Detection

### How Refresh Works

Tables can be configured with a `refreshInterval` to detect file changes:

```json
{
  "tables": [
    {
      "name": "live_data",
      "url": "data.csv",
      "refreshInterval": "5 minutes"
    }
  ]
}
```

**Refresh behavior:**
- **Check on query** - When queried, checks if refresh interval has elapsed
- **File change detection** - Uses modification time (local) or ETag/Last-Modified (HTTP)
- **Lazy refresh** - No background polling; refresh happens on access
- **Fresh data on same query** - If file changed, query returns updated data immediately

### Refresh Support by File Type

| File Type | Refresh Support | Notes |
|-----------|-----------------|-------|
| CSV/TSV | ✅ Data only | Re-reads data on interval; schema fixed at startup |
| JSON/YAML | ✅ Data only | Re-reads data on interval; schema fixed at startup |
| Parquet | ✅ Data only | Re-read each query; schema fixed at startup |
| Arrow | ❌ None | Loaded once at startup |
| Excel/Word/PowerPoint | ❌ None | Converted once at startup |
| HTML | ❌ None | Converted once at startup |
| XML | ❌ None | Converted once at startup |
| Markdown | ❌ None | Converted once at startup |

**Important:** All schemas are determined at startup and never change. Refresh only updates data within the existing schema. Schema changes require a restart.

### Important Limitations

**Refresh vs. Performance Tradeoff (Parquet/DuckDB engines):**
- If `refreshInterval` is set: **Disables Parquet conversion**, reads CSV/JSON directly (slower but refreshable)
- Without `refreshInterval`: Converts to Parquet cache once (fast but static)
- **Cannot have both** Parquet performance and refresh capability currently
- DuckDB engine **not actually used** for CSV/JSON when refresh is enabled (falls back to Calcite's native CSV/JSON readers)

**Complex formats (Excel, HTML, Word, PowerPoint):**
- Converted to JSON **once** at schema initialization
- Changes to source files **not detected** even with refresh configured
- Must restart to pick up changes

**Materialized views:**
- Computed **once** on first access
- **Never refresh** even if source tables have refresh enabled
- Delete `.materialized_views/*.parquet` files to force recomputation

## Performance Optimization

The File Adapter automatically optimizes queries through:
- **Intelligent Engine Selection** - Each schema uses its optimal engine
- **Automatic Memory Management** - Spillover to disk when needed
- **Query Optimization** - Column pruning, filter pushdown, partition pruning
- **Statistics-based Planning** - HyperLogLog cardinality estimates

**📚 For detailed tuning strategies, see [Performance Tuning Guide](docs/performance-tuning.md)**

## Complete Configuration Example

### Multiple Schemas with Different Engines

```json
{
  "version": "1.0",
  "defaultSchema": "reports",
  "schemas": [
    {
      "name": "realtime",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/realtime",
        "executionEngine": "arrow",
        "batchSize": 1000,
        "tableNameCasing": "LOWER",
        "tables": [
          {
            "name": "live_events",
            "url": "events.csv",
            "refreshInterval": "30 seconds"
          }
        ],
        "views": [
          {
            "name": "active_sessions",
            "sql": "SELECT user_id, COUNT(*) as event_count FROM live_events WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '5' MINUTE GROUP BY user_id"
          }
        ]
      }
    },
    {
      "name": "analytics",
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
      "name": "reports",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/reports",
        "executionEngine": "parquet",
        "primeCache": true,
        "tableNameCasing": "LOWER",
        "materializedViews": [
          {
            "view": "quarterly_summary",
            "table": "quarterly_summary_cache",
            "sql": "SELECT quarter, SUM(revenue) as total_revenue FROM analytics.sales GROUP BY quarter"
          }
        ],
        "views": [
          {
            "name": "current_month",
            "sql": "SELECT * FROM realtime.live_events WHERE MONTH(timestamp) = MONTH(CURRENT_DATE)"
          }
        ]
      }
    }
  ]
}
```

**Query Examples with Multiple Schemas:**

```sql
-- Query refreshable table (re-reads CSV every 30 seconds if changed)
SELECT * FROM realtime.live_events WHERE timestamp > NOW() - INTERVAL '1' HOUR;

-- Query view (always fresh, re-executes on each query)
SELECT * FROM realtime.active_sessions;

-- Query from analytics schema (DuckDB engine, complex SQL)
SELECT customer_id, 
       SUM(amount) OVER (PARTITION BY region ORDER BY date) as running_total
FROM analytics.sales_history;

-- Query materialized view (pre-computed, fast)
SELECT * FROM reports.quarterly_summary WHERE total_revenue > 1000000;

-- Query regular view (fresh data from cross-schema query)
SELECT * FROM reports.current_month;

-- Cross-schema join
SELECT r.event_id, r.timestamp, a.customer_name
FROM realtime.live_events r
JOIN analytics.customers a ON r.customer_id = a.id;
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