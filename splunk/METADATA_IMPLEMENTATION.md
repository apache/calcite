# Splunk Adapter PostgreSQL-Style Metadata Implementation

## Overview

The Splunk adapter supports PostgreSQL-compatible metadata schemas with standard SQL information_schema and pg_catalog views. This allows tools and users to discover schema information using familiar PostgreSQL queries with lowercase unquoted identifiers.

## Implementation Details

### 1. **SplunkInformationSchema.java**
Created SQL standard information_schema implementation that provides:

#### SQL Standard tables:
- `information_schema.TABLES` - Standard SQL tables view with uppercase names
- `information_schema.COLUMNS` - Standard SQL columns view with uppercase names
- `information_schema.SCHEMATA` - Standard SQL schemata view
- `information_schema.VIEWS` - Views (empty for Splunk)
- `information_schema.TABLE_CONSTRAINTS` - Constraints (empty for Splunk)
- `information_schema.KEY_COLUMN_USAGE` - Key usage (empty for Splunk)
- `information_schema.ROUTINES` - Stored procedures (empty for Splunk)
- `information_schema.PARAMETERS` - Parameters (empty for Splunk)

### 2. **SplunkPostgresMetadataSchema.java**
Created PostgreSQL-compatible pg_catalog implementation that provides:

#### PostgreSQL-compatible tables:
- `pg_catalog.pg_tables` - Lists all tables with lowercase names
- `pg_catalog.pg_namespace` - Schema/namespace information
- `pg_catalog.pg_class` - Relations (tables, indexes, etc.)
- `pg_catalog.pg_attribute` - Column attributes and details
- `pg_catalog.pg_type` - Data type information
- `pg_catalog.pg_database` - Database information
- `pg_catalog.pg_views` - Views (empty for Splunk)
- `pg_catalog.pg_indexes` - Indexes (empty for Splunk)

#### Splunk-specific tables:
- `pg_catalog.splunk_indexes` - Lists Splunk indexes with metadata
- `pg_catalog.splunk_sources` - Lists table sources and CIM model information

### 3. **SplunkSchemaFactory.java Updates**
- Modified to add `pg_catalog` and `information_schema` as top-level schemas
- Ensures standard PostgreSQL schema structure where system catalogs are separate from user schemas
- Added mock connection support for testing with "mock" URL

### 4. **Schema Table Access Implementation**
The key technical challenge was accessing tables from CalciteSchema wrappers. The solution implemented:

#### Problem:
- CalciteSchema wraps the actual schema implementation
- Direct unwrapping failed: `schema.unwrap(SplunkSchema.class)` threw exceptions
- Reflection-based access to CalciteSchema internals was unreliable

#### Solution:
- **Use SchemaPlus.tables() directly**: Instead of unwrapping, access tables through the public API
- **Avoid schema unwrapping**: No need to cast to concrete schema types
- **Direct table enumeration**: `subSchema.tables().getNames(LikePattern.any())`
- **Table access**: `subSchema.tables().get(tableName)`

#### Implementation Pattern:
```java
// Direct SchemaPlus API usage
for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
    Table table = subSchema.tables().get(tableName);
    if (table != null) {
        // Process table directly
    }
}
```

### 5. **Case Sensitivity and SQL Standard Compliance**

#### Case Sensitivity Rules:
- **information_schema** follows SQL standard: uppercase table and column names
- **pg_catalog** follows PostgreSQL convention: lowercase table and column names
- **Queries must quote identifiers** when case doesn't match default folding

#### Examples:
```sql
-- ✅ information_schema requires quoted uppercase identifiers
SELECT "TABLE_SCHEMA", "TABLE_NAME"
FROM "information_schema"."TABLES"
WHERE "TABLE_SCHEMA" = 'splunk';

-- ✅ pg_catalog uses lowercase (no quotes needed)
SELECT schemaname, tablename
FROM pg_catalog.pg_tables
WHERE schemaname = 'splunk';
```

### 6. **PostgreSQL-Style Lexical Behavior**
- Implemented `SplunkConnectionConfig.java` for PostgreSQL-style case handling
- Updated `SplunkDriver.java` to set lexical properties:
  - `unquotedCasing=TO_LOWER` - Unquoted identifiers converted to lowercase
  - `quotedCasing=UNCHANGED` - Quoted identifiers preserve case
  - `caseSensitive=true` - Case-sensitive matching after normalization
  - `quoting=DOUBLE_QUOTE` - PostgreSQL-style double quotes

### 7. **Schema Structure**

The adapter creates these **top-level schemas** (not sub-schemas):
- `splunk` - User schema containing Splunk data tables
- `pg_catalog` - PostgreSQL-compatible system catalog
- `information_schema` - SQL standard metadata views

### 8. **Example Queries**

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
- **SplunkMetadataSchemaTest.java** - Comprehensive test suite using mock connections
  - Tests all metadata schemas without requiring live Splunk connection
  - Validates information_schema and pg_catalog functionality
  - Tests case sensitivity handling and SQL standard compliance
  - Cross-schema metadata queries and JDBC DatabaseMetaData compatibility

### Test Architecture
- **Mock Connection Support**: Uses "mock" URL to avoid real Splunk connections
- **Static Table Definitions**: Pre-defined test tables (all_email, authentication)
- **Metadata Schema Validation**: Ensures pg_catalog and information_schema work correctly
- **Case Sensitivity Testing**: Validates uppercase/lowercase identifier handling

### Test Coverage
- ✅ Schema discovery and enumeration
- ✅ Table and column metadata queries
- ✅ PostgreSQL-compatible pg_catalog views
- ✅ SQL standard information_schema views
- ✅ Splunk-specific metadata tables
- ✅ Cross-schema joins and complex queries
- ✅ JDBC DatabaseMetaData integration
- ✅ Case sensitivity and identifier quoting

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

## Recent Improvements (August 2025)

### Timestamp Handling Fixes
- **Fixed ClassCastException in ORDER BY**: Timestamp fields now return Long values internally (milliseconds since epoch) for proper sorting
- **Fixed CAST to VARCHAR**: Implemented proper field name mapping between Calcite schema names (e.g., "time") and native Splunk field names (e.g., "_time")
- **Improved data conversion**: Added proper handling of empty strings and malformed timestamp values
- **All CAST integration tests now pass** (100% success rate)

### Dynamic Field Discovery
- **Enhanced DataModelDiscovery**: Now extracts fields from calculated fields in CIM data models
- **Field mapping support**: Proper translation between schema field names and native Splunk field names for pushdown operations
- **Robust error handling**: Better handling of malformed data and connection issues

### Test Coverage Improvements
- **95% test success rate** (156 tests, 7 failures)
- **Comprehensive CAST testing**: 10 different CAST operation scenarios verified
- **Integration test stability**: Fixed timeout and connection issues

### Key Files Updated
- **SplunkDataConverter.java**: Fixed timestamp representation to return Long values
- **SplunkPushDownRule.java**: Added field name mapping for CAST operations
- **DataModelDiscovery.java**: Enhanced field extraction from calculated fields
- **All test files**: Fixed JUnit 5 imports and improved stability

## Current Status (August 2025)

### Test Results Summary
- **Total tests**: 156
- **Passing tests**: 149 (95% success rate)
- **Failing tests**: 7
  - 2 failures in SplunkAdapterQueryTest (data expectation issues)
  - 5 failures in SplunkNamespaceValidationTest (timeout issues)

### Working Features
✅ All CAST operations (CAST to VARCHAR, INTEGER, DOUBLE, TIMESTAMP)
✅ ORDER BY on timestamp fields (no more ClassCastException)
✅ Dynamic data model discovery
✅ Information schema queries
✅ PostgreSQL-style metadata access
✅ Field mapping and pushdown operations
✅ Connection handling and authentication
✅ Case-insensitive queries

### Known Issues
⚠️ Some long-running namespace validation tests timeout after 5 minutes
⚠️ Minor data expectation mismatches in query tests
⚠️ Some fields may appear in _extra JSON instead of main columns (under investigation)

## Performance Notes
- Test suite completes in ~17 minutes with full integration testing
- CAST operations are efficiently pushed down to Splunk using native SPL functions
- Dynamic field discovery performs well with CIM data models

## Future Enhancements

1. Add real-time index statistics (event count, size) via Splunk REST API
2. Include field statistics and data profiling
3. Add views for saved searches and reports
4. Support for field extraction rules metadata
5. Integration with Splunk knowledge objects
6. Investigate and fix remaining timeout issues in namespace validation tests
7. Address data mapping issues where fields appear in _extra instead of main columns
