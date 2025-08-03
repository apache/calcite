# Splunk Adapter PostgreSQL-Style Metadata Implementation

## Overview

The Splunk adapter supports PostgreSQL-compatible metadata schemas with standard SQL information_schema and pg_catalog views. This allows tools and users to discover schema information using familiar PostgreSQL queries with lowercase unquoted identifiers.

## Implementation Details

### 1. **SplunkMetadataSchema.java**
Created a new class that provides the following metadata tables:

#### PostgreSQL-compatible tables:
- `pg_catalog.pg_tables` - Lists all tables in the schema
- `information_schema.tables` - Standard SQL tables view
- `information_schema.columns` - Standard SQL columns view
- `information_schema.schemata` - Standard SQL schemata view

#### Splunk-specific tables:
- `pg_catalog.splunk_indexes` - Lists Splunk indexes with metadata
- `pg_catalog.splunk_sources` - Lists table sources and CIM model information

### 2. **SplunkSchema.java Updates**
- Added `metadataSchema` field initialized in both constructors
- Removed sub-schema implementation (metadata schemas are now top-level)
- Added `getTableMapForMetadata()` method for metadata queries

### 3. **SplunkSchemaFactory.java Updates**
- Modified to add `pg_catalog` and `information_schema` as top-level schemas
- Ensures standard PostgreSQL schema structure where system catalogs are separate from user schemas

### 4. **PostgreSQL-Style Lexical Behavior**
- Implemented `SplunkConnectionConfig.java` for PostgreSQL-style case handling
- Updated `SplunkDriver.java` to set lexical properties:
  - `unquotedCasing=TO_LOWER` - Unquoted identifiers converted to lowercase
  - `quotedCasing=UNCHANGED` - Quoted identifiers preserve case
  - `caseSensitive=true` - Case-sensitive matching after normalization
  - `quoting=DOUBLE_QUOTE` - PostgreSQL-style double quotes

### 5. **Schema Structure**

The adapter creates these **top-level schemas** (not sub-schemas):
- `splunk` - User schema containing Splunk data tables
- `pg_catalog` - PostgreSQL-compatible system catalog
- `information_schema` - SQL standard metadata views

### 6. **Example Queries**

**✅ Correct - Top-level schemas with lowercase identifiers:**
```sql
-- List all tables in the splunk schema
SELECT schemaname, tablename, tableowner
FROM pg_catalog.pg_tables
WHERE schemaname = 'splunk';

-- Get column information using standard SQL
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'splunk' AND table_name = 'web'
ORDER BY ordinal_position;

-- Cross-schema join for table analysis
SELECT t.table_name, COUNT(c.column_name) as column_count
FROM information_schema.tables t
JOIN information_schema.columns c
  ON t.table_catalog = c.table_catalog
  AND t.table_schema = c.table_schema
  AND t.table_name = c.table_name
WHERE t.table_schema = 'splunk'
GROUP BY t.table_name
ORDER BY column_count DESC;

-- Query Splunk-specific metadata
SELECT index_name, is_internal
FROM pg_catalog.splunk_indexes
ORDER BY index_name;
```

**❌ Old incorrect way (sub-schemas):**
```sql
-- Don't use these - they reference sub-schemas incorrectly
SELECT * FROM splunk.pg_catalog.pg_tables;
SELECT * FROM splunk.information_schema.columns;
```

## Configuration

To enable metadata queries, no additional configuration is required. The metadata schemas are automatically available when you create a Splunk schema.

### Model File Example

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
      "operand": {
        "url": "https://localhost:8089",
        "username": "admin",
        "password": "password",
        "cimModel": "web"
      }
    }
  ]
}
```

### JDBC URL Example

```java
// Create model with PostgreSQL-style lexical settings
String modelJson = "{"
    + "\"version\":\"1.0\","
    + "\"defaultSchema\":\"splunk\","
    + "\"schemas\":[{"
    + "\"name\":\"splunk\","
    + "\"type\":\"custom\","
    + "\"factory\":\"org.apache.calcite.adapter.splunk.SplunkSchemaFactory\","
    + "\"operand\":{"
    + "\"url\":\"https://localhost:8089\","
    + "\"username\":\"admin\","
    + "\"password\":\"password\","
    + "\"cimModel\":\"web\"}"
    + "}]}";

String url = "jdbc:calcite:model=inline:" + modelJson
    + ";unquotedCasing=TO_LOWER"
    + ";quotedCasing=UNCHANGED"
    + ";caseSensitive=true";

Connection connection = DriverManager.getConnection(url);

// Query metadata with lowercase unquoted identifiers
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery(
    "SELECT schemaname, tablename FROM pg_catalog.pg_tables");
```

## Testing

### Unit Tests
- Created `SplunkMetadataDemo.java` that demonstrates the structure without requiring a live connection
- Shows how metadata tables are structured and what queries can be run

### Integration Tests
- Created `SplunkMetadataIntegrationTest.java` for testing with a live Splunk instance
- Uses `local-properties.settings` for configuration
- Tests all metadata tables and cross-schema joins

### Local Properties Configuration

Create `local-properties.settings` from the sample:

```properties
splunk.url=https://your-splunk-instance:8089
splunk.username=admin
splunk.password=your-password
splunk.test.metadata.enabled=true
```

## Benefits

1. **Standard PostgreSQL Compatibility**: Use familiar PostgreSQL queries with lowercase unquoted identifiers
2. **Proper Schema Structure**: System catalogs are top-level schemas, not sub-schemas
3. **Tool Integration**: Compatible with PostgreSQL tools and drivers
4. **Schema Discovery**: Programmatically discover tables and columns using standard SQL
5. **Case Insensitive Queries**: Natural lowercase SQL without quotes
6. **Splunk-Specific Metadata**: Access indexes and CIM model information
7. **Cross-Schema Joins**: Join metadata tables for complex schema analysis

## PostgreSQL-Style Features

### Lexical Conventions
- **Unquoted identifiers**: Converted to lowercase (`table_name` → `table_name`)
- **Quoted identifiers**: Case preserved (`"Table_Name"` → `Table_Name`)
- **Case-sensitive matching**: After normalization
- **Double-quote style**: PostgreSQL-compatible quoting

### Schema Structure
- `pg_catalog` and `information_schema` are **top-level schemas**
- Matches standard PostgreSQL structure
- No sub-schema references needed

### Natural SQL Queries
```sql
-- All work without quotes
SELECT table_name FROM information_schema.tables;
SELECT column_name, data_type FROM information_schema.columns;
SELECT schemaname, tablename FROM pg_catalog.pg_tables;
```

## Future Enhancements

1. Add real-time index statistics (event count, size) via Splunk REST API
2. Include field statistics and data profiling
3. Add views for saved searches and reports
4. Support for field extraction rules metadata
5. Integration with Splunk knowledge objects
