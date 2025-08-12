# Apache Iceberg Support in Calcite File Adapter

## Overview

The Calcite File Adapter includes comprehensive support for Apache Iceberg tables, enabling SQL queries against Iceberg table formats with time travel and schema handling capabilities.

## Features

### Core Capabilities  
- **Direct Table Access**: Query Iceberg tables through SQL
- **Time Travel**: Query historical snapshots via `snapshotId` or `asOfTimestamp` config
- **Schema Evolution**: Basic type mapping from Iceberg to SQL types
- **Hadoop Catalog Support**: Integration with Hadoop-based Iceberg catalogs
- **Storage Provider Integration**: Full integration with the storage provider abstraction

### Supported Iceberg Features
- Reading Iceberg tables via Hadoop catalog
- Snapshot-based time travel queries
- Basic type mapping between Iceberg and SQL types
- Storage provider integration patterns

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

#### Hadoop Catalog (Supported)
```json
{
  "name": "hadoop_table",
  "format": "iceberg",
  "catalogType": "hadoop",
  "warehousePath": "/path/to/warehouse",
  "tablePath": "database.table_name"
}
```

#### REST Catalog (Implemented)
```json
{
  "name": "rest_table",
  "format": "iceberg",
  "catalogType": "rest",
  "uri": "https://rest-catalog.example.com/v1",
  "warehouse": "/warehouse/path",
  "token": "your-auth-token"
}
```

With OAuth2 support:
```json
{
  "name": "rest_oauth_table",
  "format": "iceberg", 
  "catalogType": "rest",
  "uri": "https://rest-catalog.example.com/v1",
  "oauth2-server-uri": "https://auth.example.com/oauth/token",
  "client-id": "your-client-id",
  "client-secret": "your-client-secret"
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

### Storage Provider Configuration (Implemented)

You can use Iceberg tables through the storage provider abstraction - this is fully implemented and tested:

```json
{
  "storageType": "iceberg",
  "storageConfig": {
    "catalogType": "hadoop",
    "warehousePath": "/iceberg/warehouse"
  }
}
```

This pattern works the same as other storage providers (S3, HTTP, FTP, SFTP, SharePoint) and allows treating Iceberg catalogs as file systems where:
- Namespaces appear as directories
- Tables appear as files
- Standard StorageProvider operations (listFiles, exists, getMetadata) work seamlessly

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

## Metadata Tables (Partial Implementation)

Iceberg metadata tables are being implemented with `$` suffix:

### Currently Available Metadata Tables

1. **$history** - Table history including all snapshots ✅
   ```sql
   SELECT * FROM my_iceberg_table$history;
   ```
   Columns: `made_current_at`, `snapshot_id`, `parent_id`, `is_current_ancestor`

2. **$snapshots** - Detailed snapshot information ✅
   ```sql
   SELECT * FROM my_iceberg_table$snapshots;
   ```
   Columns: `committed_at`, `snapshot_id`, `parent_id`, `operation`, `manifest_list`, `summary`

### Additional Metadata Tables (Implemented)

3. **$files** - Data files in the table ✅
   ```sql
   SELECT * FROM my_iceberg_table$files;
   ```
   Columns: `content`, `file_path`, `file_format`, `spec_id`, `record_count`, `file_size_in_bytes`

4. **$manifests** - Manifest files ✅
   ```sql
   SELECT * FROM my_iceberg_table$manifests;
   ```
   Columns: `path`, `length`, `partition_spec_id`, `added_snapshot_id`, `added_data_files_count`, `existing_data_files_count`, `deleted_data_files_count`

5. **$partitions** - Partition summary ✅
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

## Performance Considerations (MVP)

1. **Metadata Caching**: Basic Iceberg metadata caching per table instance
2. **Type Mapping**: Efficient conversion from Iceberg to SQL types
3. **Snapshot Isolation**: Each query reads from a consistent snapshot
4. **Memory Management**: Streaming data access via IcebergEnumerator

**Note**: Partition pruning and column projection optimizations are now implemented. Advanced manifest caching is planned for future releases.

## Limitations (MVP Implementation)

1. **Write Operations**: Currently read-only; writes not yet supported
2. **Advanced Catalog Features**: Some advanced catalog operations not implemented  
3. **Write Support**: Table modifications not yet supported
4. **Delete Files**: Position and equality delete files not supported
5. **Merge-on-Read**: Only copy-on-write tables supported  
6. **Nested Types**: Complex nested structures have basic support
7. **Advanced Nested Type Operations**: Complex nested operations have basic support

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

### Debug Configuration (Future Enhancement)
*Debug and logging configuration not yet implemented*

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

**Near-term (Priority)**:
1. ✅ **Complete Metadata Tables**: Full implementation of $files, $manifests, $partitions
2. ✅ **REST Catalog**: Support for Iceberg REST catalog protocol
3. ✅ **Partition Pruning**: Optimize queries with partition filtering  
4. ✅ **Column Projection**: Optimize column access for better performance

**Medium-term**:
5. **Write Support**: INSERT, UPDATE, DELETE operations
6. **Advanced Time Travel**: More flexible time travel syntax  
7. **Delete File Support**: Full support for position and equality deletes
8. **Statistics Integration**: Use Iceberg statistics for query optimization

**Long-term**: 
9. **Hive Metastore Catalog**: Support for HMS catalog
10. **AWS Glue Catalog**: Support for Glue Data Catalog
11. **Incremental Reads**: Read only changed data between snapshots
12. **Compaction**: Automatic file compaction strategies

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API](https://iceberg.apache.org/javadoc/)
- [Iceberg Table Spec](https://iceberg.apache.org/spec/)
- [Calcite File Adapter Documentation](../README.md)