<!--
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

# CSV NextGen Adapter

The CSV NextGen adapter provides high-performance CSV and TSV processing for Apache Calcite with pluggable execution engines optimized for different workload types.

## Features

### Execution Engines

1. **LINQ4J** - Traditional row-by-row processing
   - Best for: OLTP workloads, small datasets, row-wise operations
   - Performance: Standard Calcite enumerable performance

2. **ARROW** - Arrow-based columnar processing
   - Best for: Medium datasets, mixed workloads, filtering operations
   - Performance: Competitive across all operations, excels at filtering

3. **VECTORIZED** (Default) - Optimized vectorized Arrow processing
   - Best for: OLAP workloads, large datasets, analytical queries
   - Performance: 1.2-2.6x faster than LINQ4J, with benefits scaling with dataset size

📊 **[View Detailed Performance Results](PERFORMANCE_RESULTS.md)** - Comprehensive benchmarks across 100K-1M row datasets

### Key Optimizations

- **Batch Processing**: Configurable batch sizes (default: 2048 rows)
- **Columnar Access**: Cache-friendly memory access patterns
- **Vectorized Operations**: SIMD-style chunked processing for aggregations and filtering
- **Zero-Copy Projection**: Direct vector manipulation without row materialization
- **Auto-Discovery**: Automatically scans directories for CSV/TSV files

## Configuration

### Basic Model.json

```json
{
  "version": "1.0",
  "defaultSchema": "csvnextgen",
  "schemas": [
    {
      "name": "csvnextgen",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactory",
      "operand": {
        "directory": "data/",
        "executionEngine": "vectorized",
        "batchSize": 2048
      }
    }
  ]
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `directory` | String | Required* | Base directory containing CSV files |
| `file` | String | Optional | Single CSV file path (alternative to directory) |
| `tableName` | String | Optional | Table name when using single file (default: filename without extension) |
| `executionEngine` | String | `"vectorized"` | Engine type: `"linq4j"`, `"arrow"`, or `"vectorized"` |
| `batchSize` | Integer | `2048` | Number of rows per batch for columnar engines |
| `tables` | Array | Optional | Explicit table definitions |

*Either `directory` or `file` is required

### Single File Configuration

For scenarios where you want to work with a single CSV file instead of a directory:

```json
{
  "version": "1.0",
  "defaultSchema": "my_data",
  "schemas": [
    {
      "name": "my_data",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactory",
      "operand": {
        "file": "data/sales.csv",
        "tableName": "sales_data",
        "executionEngine": "vectorized"
      }
    }
  ]
}
```

**Key Features:**
- `file`: Path to a single CSV/TSV file (supports .csv, .tsv, .csv.gz, .tsv.gz)
- `tableName`: Optional custom table name (defaults to filename without extension)
- Uses the same execution engines and performance optimizations as directory mode
- Supports relative and absolute file paths

**Example with auto-generated table name:**
```json
{
  "operand": {
    "file": "reports/quarterly_results.csv",
    "executionEngine": "vectorized"
  }
}
```
This creates a table named `QUARTERLY_RESULTS` from the file `quarterly_results.csv`.

### Explicit Table Configuration

```json
{
  "operand": {
    "directory": "data/",
    "executionEngine": "vectorized",
    "tables": [
      {
        "name": "sales",
        "file": "sales.csv",
        "header": true
      },
      {
        "name": "products",
        "file": "products.tsv",
        "header": true
      }
    ]
  }
}
```

### Multiple Schemas with Different Engines

```json
{
  "schemas": [
    {
      "name": "oltp_data",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactory",
      "operand": {
        "directory": "transactional/",
        "executionEngine": "linq4j"
      }
    },
    {
      "name": "analytics_data",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactory",
      "operand": {
        "directory": "warehouse/",
        "executionEngine": "vectorized",
        "batchSize": 4096
      }
    }
  ]
}
```

## Usage Examples

### Schema Factory Direct Usage

```java
// Create schema programmatically
Map<String, Object> operand = new HashMap<>();
operand.put("directory", "/path/to/csv/files");
operand.put("executionEngine", "vectorized");
operand.put("batchSize", 2048);

SchemaPlus rootSchema = Frameworks.createRootSchema(true);
Schema schema = CsvNextGenSchemaFactory.INSTANCE.create(rootSchema, "csv", operand);
```

### Performance Comparison Example

```sql
-- Same analytical query on different execution engines
SELECT region, SUM(revenue), AVG(profit_margin)
FROM sales
WHERE profit_margin > 15.0
GROUP BY region;
```

**Performance Results (1M rows):**
- **LINQ4J**: 842ms (baseline)
- **ARROW**: 1088ms (0.77x vs LINQ4J)
- **VECTORIZED**: 319ms (**2.64x faster** than LINQ4J)

**Key Insight**: Vectorized engine shows dramatic improvements for aggregation-heavy analytical queries.

📈 **[See Full Performance Analysis](PERFORMANCE_RESULTS.md)** for detailed benchmarks across all operation types.

## File Support

- **CSV files**: `.csv`, `.csv.gz`
- **TSV files**: `.tsv`, `.tsv.gz`
- **Auto-detection**: Automatically detects separator (comma vs tab)
- **Headers**: Configurable header row detection
- **Compression**: Transparent gzip support
- **Recursive scanning**: Scans subdirectories automatically

## Performance Characteristics

### Vectorized Engine Benefits by Operation Type

| Operation | Performance Improvement | Best Performance At |
|-----------|----------------------|-------------------|
| **Projection** | Up to 1.35x faster | Large datasets (1M+ rows) |
| **Filtering** | Variable* | Small-medium datasets |
| **Aggregation** | Up to 2.64x faster | All dataset sizes |
| **COUNT Operations** | 1.04-1.23x faster | Consistent across scales |
| **MIN/MAX** | 1.06-1.12x faster | Large datasets |

*Note: Filtering performance varies by dataset size and predicate complexity

### Scaling Characteristics

- **Small datasets** (<100K rows): LINQ4J competitive, Arrow leads in projection
- **Medium datasets** (100K-500K rows): Vectorized begins showing advantages
- **Large datasets** (>1M rows): Vectorized dominates, especially for aggregations

📊 **[View Complete Performance Analysis](PERFORMANCE_RESULTS.md)** - Detailed results across 100K, 500K, and 1M row datasets

### Optimal Use Cases

**LINQ4J Engine:**
- OLTP workloads with frequent small queries
- Row-wise operations and point lookups
- Small datasets where vectorization overhead outweighs benefits

**ARROW Engine:**
- Mixed workloads with both OLTP and analytical queries
- Medium-sized datasets (100K-1M rows)
- Environments where memory usage is a concern

**VECTORIZED Engine:**
- Analytical/OLAP workloads with aggregations and filtering
- Large datasets (>1M rows)
- Batch processing and reporting scenarios
- Data warehouse and ETL operations

## Implementation Details

The CSV NextGen adapter implements a pluggable execution engine architecture:

1. **Data Ingestion**: Unified CSV batch reader with configurable batch sizes
2. **Type Inference**: Automatic SQL type detection from sample data
3. **Engine Selection**: Runtime selection based on configuration
4. **Query Integration**: Standard Calcite relational algebra integration
5. **Memory Management**: Proper Arrow memory allocation and cleanup

The vectorized engine implements several key optimizations:
- Batch-oriented processing with configurable batch sizes
- Chunked aggregation with unrolled loops (8-element SIMD-style)
- Vectorized predicate evaluation for filtering
- Cache-friendly memory access patterns
- Zero-copy projection operations

## Dependencies

- Apache Calcite (core)
- Apache Arrow Java 17.0.0 (vectorized engine, upgraded from 11.0.0)
- Apache Arrow Gandiva 17.0.0 (LLVM-based expression compilation, available for future use)
- OpenCSV 5.7.1 (CSV parsing, upgraded from 2.3)
- JUnit 5 (testing)

## Version Upgrades

The CSV NextGen adapter uses the latest stable versions of key dependencies:

### Arrow Upgrade (11.0.0 → 17.0.0)
- **Performance improvements**: Better memory management and vectorization
- **New features**: Enhanced SIMD operations and columnar processing
- **Stability**: Bug fixes and improved Arrow memory allocation
- **Gandiva support**: Now includes Gandiva for potential LLVM-based expression compilation

### OpenCSV Upgrade (2.3 → 5.7.1)
- **Modern API**: Updated to use CSVReaderBuilder and CSVParserBuilder
- **Better TSV support**: Improved tab-separated value handling
- **Performance**: More efficient CSV parsing with reduced memory overhead
- **Features**: Better quote handling and escape character support
