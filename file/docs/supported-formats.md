# Supported File Formats

The Apache Calcite File Adapter supports a wide variety of file formats with automatic schema detection and type inference.

## Structured Data Formats

### CSV (Comma-Separated Values)

**File Extensions:** `.csv`, `.tsv`, `.txt`

**Features:**
- Automatic type inference for columns
- Custom delimiter support
- Header detection
- Quoted field handling
- Compressed file support (.csv.gz)

**Configuration Options:**

CSV configuration is handled through the `csvTypeInference` parameter and automatic detection. Advanced CSV configuration options are planned for future releases.

```json
{
  "csvTypeInference": {
    "enabled": true,
    "sampleSize": 1000
  }
}
```

**Supported Data Types:**
- `INTEGER` - Whole numbers
- `BIGINT` - Large integers  
- `DOUBLE` - Floating point numbers
- `DECIMAL` - Precise decimal numbers
- `BOOLEAN` - true/false values
- `DATE` - Date values
- `TIME` - Time values
- `TIMESTAMP` - Date and time values
- `VARCHAR` - Text strings

> **📖 For detailed information on CSV type inference configuration and advanced features, see [CSV Type Inference](csv-type-inference.md)**

### JSON (JavaScript Object Notation)

**File Extensions:** `.json`, `.jsonl`, `.ndjson`

**Features:**
- Nested object flattening
- Array handling
- JSONPath expressions
- Multi-table extraction from single files
- Schema inference from sample data

**Configuration Options:**
```json
{
  "flatten": true,
  "refreshInterval": "30 seconds"
}
```

**Note:** Advanced JSON configuration features like JSONPath-based table extraction are planned for future releases.

**JSONPath Examples:**
```json
{
  "customers": [
    {"id": 1, "name": "John", "orders": [{"id": 101, "total": 50.0}]}
  ]
}
```

Extracts to tables:
- `customers` table: id, name
- `orders` table: customer_id, id, total

### YAML (YAML Ain't Markup Language)

**File Extensions:** `.yaml`, `.yml`

**Features:**
- Hierarchical data structure support
- Automatic flattening of nested structures
- List and dictionary handling
- Type inference

**Example:**
```yaml
users:
  - id: 1
    name: Alice
    profile:
      age: 30
      city: New York
  - id: 2
    name: Bob
    profile:
      age: 25
      city: London
```

Results in table with columns: `id`, `name`, `profile_age`, `profile_city`

## Office Document Formats

### Microsoft Excel

**File Extensions:** `.xlsx`, `.xls`

**Features:**
- Multi-sheet support (each sheet becomes a table)
- Header detection
- Formula evaluation
- Data type inference
- Merged cell handling

**Configuration Options:**
```json
{
  "excelConfig": {
    "evaluateFormulas": true,
    "skipEmptyRows": true,
    "headerRowIndex": 0,
    "sheets": ["Sheet1", "Data", "Summary"]
  }
}
```

### Microsoft Word Documents

**File Extensions:** `.docx`

**Features:**
- Table extraction from documents
- Multi-table support per document
- Header detection
- Cell formatting preservation

**Table Detection:**
- Identifies tables based on structure
- Preserves table headers
- Handles merged cells and complex layouts

### Microsoft PowerPoint

**File Extensions:** `.pptx`

**Features:**
- Table extraction from slides
- Multi-slide processing
- Slide context information
- Title and header detection

**Slide Processing:**
- Each slide can contain multiple tables
- Tables are named based on slide number and position
- Slide titles are preserved as metadata

## Web and Markup Formats

### HTML (HyperText Markup Language)

**File Extensions:** `.html`, `.htm`

**Features:**
- Automatic table discovery using `<table>` tags
- Web crawling capabilities for linked files
- CSS selector support
- Multi-table extraction per page

**Configuration Options:**
```json
{
  "htmlConfig": {
    "tableSelector": "table",
    "headerSelector": "th",
    "skipEmptyTables": true,
    "crawlConfig": {
      "enabled": true,
      "maxDepth": 2,
      "followExternalLinks": false
    }
  }
}
```

### Markdown

**File Extensions:** `.md`, `.markdown`

**Features:**
- GitHub Flavored Markdown table support
- Multi-table extraction
- Header detection
- Link preservation

**Table Syntax Support:**
```markdown
| Name | Age | City |
|------|-----|------|
| John | 30  | NYC  |
| Jane | 25  | LA   |
```

### XML (eXtensible Markup Language)

**File Extensions:** `.xml`

**Features:**
- XPath-based data extraction
- Repeating element pattern detection
- Attribute handling
- Namespace support

**Configuration Options:**
```json
{
  "xmlConfig": {
    "rootPath": "/data/records",
    "recordPath": "./record",
    "attributePrefix": "attr_",
    "textContentColumn": "text_content"
  }
}
```

## Columnar Formats

### Apache Parquet

**File Extensions:** `.parquet`

**Features:**
- Native columnar format support
- Schema evolution
- Predicate pushdown
- Compression support
- Statistics utilization

**Advanced Features:**
- Partition pruning
- Column pruning
- Filter pushdown
- Statistics-based optimization

### Apache Arrow

**File Extensions:** `.arrow`, `.feather`

**Features:**
- In-memory columnar format
- Zero-copy data access
- High-performance processing
- Inter-process communication

**Configuration Options:**
```json
{
  "arrowConfig": {
    "batchSize": 1000,
    "enableVectorization": true,
    "compressionType": "lz4"
  }
}
```

## Data Lake Formats

### Apache Iceberg

**Features:**
- Table format for large datasets
- Schema evolution
- Time travel queries
- Snapshot isolation
- Partition evolution

**Configuration Options:**
```json
{
  "icebergConfig": {
    "catalogType": "hadoop",
    "warehousePath": "/data/warehouse",
    "enableTimeTravel": true
  }
}
```

**Example Queries:**
```sql
-- Current data
SELECT * FROM iceberg_table;

-- Time travel query
SELECT * FROM iceberg_table FOR SYSTEM_TIME AS OF '2024-01-01 10:00:00';

-- Snapshot query
SELECT * FROM iceberg_table FOR SYSTEM_VERSION AS OF 12345;
```

> **📖 For comprehensive Iceberg configuration, time travel features, and catalog setup, see [Apache Iceberg Integration](iceberg-integration.md)**

## Compressed Files

### Supported Compression Formats

- **gzip** (.gz) - Universal compression
- **bzip2** (.bz2) - High compression ratio
- **xz** (.xz) - Modern compression
- **zip** (.zip) - Archive format support

**Automatic Detection:**
The adapter automatically detects and decompresses files based on file extensions.

**Configuration:**
```json
{
  "compressionConfig": {
    "enabled": true,
    "maxUncompressedSize": "1GB",
    "tempDirectory": "/tmp/calcite-decompression"
  }
}
```

## File Naming and Table Creation

### Table Name Generation

Tables are automatically created based on file names:

| File Path | Generated Table Name |
|-----------|---------------------|
| `/data/customers.csv` | `customers` |
| `/data/sales/2024/january.xlsx` | `sales_2024_january` |
| `/data/PRODUCTS.JSON` | `products` (lowercase) |
| `/data/user-data.csv` | `user_data` (hyphens to underscores) |

### Custom Table Naming

```json
{
  "tableNameCasing": "SMART_CASING",
  "columnNameCasing": "SMART_CASING",
  "customMappings": {
    "very-long-filename.csv": "short_name"
  }
}
```

## Format-Specific Limitations

| Format | Limitations |
|--------|-------------|
| CSV | Limited nested data support |
| JSON | Memory usage scales with nesting depth |
| Excel | Formula dependencies may not resolve correctly |
| HTML | Complex CSS layouts may cause extraction issues |
| XML | Very large files may require streaming processing |
| Parquet | Schema evolution requires careful planning |

## Performance Considerations

### Format Performance Ranking

1. **Parquet** - Best overall performance, especially for analytics
2. **Arrow** - Excellent for in-memory processing
3. **CSV** - Good with type inference enabled
4. **JSON** - Performance depends on structure complexity
5. **Excel** - Slower due to format complexity
6. **HTML/XML** - Parsing overhead can be significant

### Optimization Recommendations

- Use Parquet format for frequently accessed data
- Enable automatic Parquet conversion for mixed-format environments
- Configure appropriate cache directories for converted files
- Consider partitioning for very large datasets
- Use glob patterns to limit file discovery scope