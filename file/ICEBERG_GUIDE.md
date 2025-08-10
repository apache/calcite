# Apache Iceberg Support in Calcite File Adapter

## Overview

The Calcite File Adapter now includes comprehensive support for Apache Iceberg tables, enabling SQL queries against Iceberg table formats with features like time travel, schema evolution, and metadata table access.

## Features

### Core Capabilities
- **Direct Table Access**: Query Iceberg tables through SQL
- **Time Travel**: Query historical snapshots or specific timestamps
- **Schema Evolution**: Automatic handling of schema changes
- **Metadata Tables**: Access Iceberg metadata through SQL-queryable tables
- **Multiple Catalog Support**: Hadoop, REST, and extensible for other catalogs
- **Storage Provider Integration**: Seamless integration with existing storage abstraction

### Supported Iceberg Features
- Reading Iceberg v1 and v2 table formats
- Snapshot isolation
- Partition pruning
- Column projection
- Type mapping between Iceberg and SQL types

## Configuration

### Basic Configuration

```json
{
  "version": "1.0",
  "defaultSchema": "iceberg_schema",
  "schemas": [
    {
      "name": "iceberg_schema",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "tables": [
          {
            "name": "my_iceberg_table",
            "url": "/path/to/iceberg/table",
            "format": "iceberg",
            "catalogType": "hadoop",
            "warehousePath": "/path/to/warehouse"
          }
        ]
      }
    }
  ]
}
```

### Catalog Types

#### Hadoop Catalog
```json
{
  "name": "hadoop_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/path/to/warehouse",
  "tablePath": "database.table_name"
}
```

#### REST Catalog
```json
{
  "name": "rest_table",
  "format": "iceberg",
  "catalogType": "rest",
  "uri": "http://iceberg-rest-catalog:8181",
  "tablePath": "namespace.table_name"
}
```

#### Direct Table Path
```json
{
  "name": "direct_table",
  "format": "iceberg",
  "url": "/absolute/path/to/iceberg/table/metadata"
}
```

### Storage Provider Configuration

You can also use Iceberg tables through the storage provider abstraction:

```json
{
  "storageType": "iceberg",
  "storageConfig": {
    "catalogType": "hadoop",
    "warehousePath": "/iceberg/warehouse"
  }
}
```

## Time Travel

### Query Specific Snapshot

```json
{
  "name": "historical_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/warehouse",
  "tablePath": "db.table",
  "snapshotId": 3821550127947089009
}
```

SQL Query:
```sql
SELECT * FROM historical_table;
-- Returns data from snapshot 3821550127947089009
```

### Query As Of Timestamp

```json
{
  "name": "point_in_time_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/warehouse",
  "tablePath": "db.table",
  "asOfTimestamp": "2024-01-15 10:30:00"
}
```

SQL Query:
```sql
SELECT * FROM point_in_time_table;
-- Returns data as it was at 2024-01-15 10:30:00
```

## Metadata Tables

Iceberg metadata tables are automatically exposed with a `$` suffix:

### Available Metadata Tables

1. **$history** - Table history including all snapshots
   ```sql
   SELECT * FROM my_iceberg_table$history;
   ```
   Columns: `made_current_at`, `snapshot_id`, `parent_id`, `is_current_ancestor`

2. **$snapshots** - Detailed snapshot information
   ```sql
   SELECT * FROM my_iceberg_table$snapshots;
   ```
   Columns: `committed_at`, `snapshot_id`, `parent_id`, `operation`, `manifest_list`, `summary`

3. **$files** - Data files in the table
   ```sql
   SELECT * FROM my_iceberg_table$files;
   ```
   Columns: `content`, `file_path`, `file_format`, `spec_id`, `record_count`, `file_size_in_bytes`

4. **$manifests** - Manifest files
   ```sql
   SELECT * FROM my_iceberg_table$manifests;
   ```
   Columns: `path`, `length`, `partition_spec_id`, `added_snapshot_id`, `added_data_files_count`, `existing_data_files_count`, `deleted_data_files_count`

5. **$partitions** - Partition summary
   ```sql
   SELECT * FROM my_iceberg_table$partitions;
   ```
   Columns: `partition`, `record_count`, `file_count`

## Type Mapping

| Iceberg Type | SQL Type |
|-------------|----------|
| boolean | BOOLEAN |
| integer | INTEGER |
| long | BIGINT |
| float | REAL |
| double | DOUBLE |
| decimal(p,s) | DECIMAL(p,s) |
| date | DATE |
| time | TIME |
| timestamp | TIMESTAMP |
| timestamptz | TIMESTAMP WITH LOCAL TIME ZONE |
| string | VARCHAR |
| uuid | VARCHAR(36) |
| fixed(n) | BINARY(n) |
| binary | VARBINARY |
| struct | ROW |
| list | ARRAY |
| map | MAP |

## Example Queries

### Basic Query
```sql
SELECT id, name, created_at 
FROM iceberg_table 
WHERE created_at > '2024-01-01';
```

### Time Travel Query
```sql
-- Query specific snapshot
SELECT * FROM iceberg_table_snapshot_12345;

-- Query metadata
SELECT snapshot_id, committed_at, operation 
FROM iceberg_table$snapshots 
ORDER BY committed_at DESC 
LIMIT 10;
```

### Join with Metadata
```sql
SELECT t.*, s.committed_at
FROM iceberg_table t
JOIN iceberg_table$snapshots s 
  ON s.is_current = true;
```

### Analyze Table History
```sql
SELECT 
  operation,
  COUNT(*) as count,
  MIN(committed_at) as first_operation,
  MAX(committed_at) as last_operation
FROM iceberg_table$snapshots
GROUP BY operation;
```

## Advanced Configuration

### Custom Properties
```json
{
  "name": "advanced_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/warehouse",
  "tablePath": "db.table",
  "properties": {
    "io.manifest.cache-enabled": "true",
    "io.manifest.cache.max-total-bytes": "1073741824",
    "read.split.target-size": "134217728"
  }
}
```

### Partitioned Tables
Iceberg partitioning is handled automatically. Partition pruning occurs when filter predicates match partition columns:

```sql
-- Automatically prunes partitions
SELECT * FROM partitioned_table 
WHERE year = 2024 AND month = 3;
```

## Performance Considerations

1. **Metadata Caching**: Iceberg metadata is cached per table instance
2. **Manifest Caching**: Enable manifest caching for frequently accessed tables
3. **Split Planning**: Configure split size based on your query patterns
4. **Column Projection**: Only required columns are read from Parquet files

## Limitations

1. **Write Operations**: Currently read-only; writes not yet supported
2. **Delete Files**: Position and equality delete files not yet fully supported
3. **Merge-on-Read**: Only copy-on-write tables fully supported
4. **Nested Types**: Complex nested structures have limited support

## Troubleshooting

### Common Issues

1. **Table Not Found**
   - Verify catalog configuration
   - Check warehouse path exists
   - Ensure table name includes namespace

2. **Schema Mismatch**
   - Iceberg schema evolution is automatic
   - Check for unsupported type conversions

3. **Performance Issues**
   - Enable manifest caching
   - Increase split size for large scans
   - Use partition filters when possible

### Debug Configuration
```json
{
  "name": "debug_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/warehouse",
  "tablePath": "db.table",
  "debug": true,
  "logLevel": "DEBUG"
}
```

## Integration Examples

### With Existing File Schemas
```json
{
  "schemas": [
    {
      "name": "mixed_schema",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data",
        "tables": [
          {
            "name": "csv_table",
            "url": "data.csv",
            "format": "csv"
          },
          {
            "name": "iceberg_table",
            "url": "iceberg/table",
            "format": "iceberg",
            "catalogType": "hadoop",
            "warehousePath": "/iceberg/warehouse"
          }
        ]
      }
    }
  ]
}
```

### Cross-Format Joins
```sql
SELECT 
  c.customer_id,
  c.name,
  i.total_orders
FROM csv_customers c
JOIN iceberg_orders i 
  ON c.customer_id = i.customer_id;
```

## Future Enhancements

Planned improvements for Iceberg support:

1. **Write Support**: INSERT, UPDATE, DELETE operations
2. **Advanced Time Travel**: More flexible time travel syntax
3. **Incremental Reads**: Read only changed data between snapshots
4. **Statistics Integration**: Use Iceberg statistics for query optimization
5. **Hive Metastore Catalog**: Support for HMS catalog
6. **AWS Glue Catalog**: Support for Glue Data Catalog
7. **Delete File Support**: Full support for position and equality deletes
8. **Compaction**: Automatic file compaction strategies

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API](https://iceberg.apache.org/javadoc/)
- [Iceberg Table Spec](https://iceberg.apache.org/spec/)
- [Calcite File Adapter Documentation](../README.md)