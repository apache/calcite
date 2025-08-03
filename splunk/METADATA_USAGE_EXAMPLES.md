# Splunk Adapter PostgreSQL-Style Metadata Usage Examples

## Overview

The Splunk adapter provides PostgreSQL-compatible metadata schemas that are automatically available as **top-level schemas** when you connect to Splunk. This allows standard PostgreSQL tools and queries to work seamlessly with lowercase unquoted identifiers.

## Available Metadata Tables

When you connect to Splunk via the adapter, you automatically get these **top-level schemas**:

- `splunk` - Your Splunk data tables (web, authentication, etc.)
- `pg_catalog` - PostgreSQL-compatible system catalog
- `information_schema` - SQL standard information schema

**⚠️ Important**: These are **top-level schemas**, not sub-schemas. Use `pg_catalog.pg_tables`, not `splunk.pg_catalog.pg_tables`.

## Example Queries

Once connected to Splunk through any method (JDBC URL, model file, programmatic), you can run:

### List all tables in the splunk schema
```sql
-- PostgreSQL style (lowercase unquoted identifiers)
SELECT schemaname, tablename, tableowner
FROM pg_catalog.pg_tables
WHERE schemaname = 'splunk';

-- SQL standard style
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'splunk';
```

### Get column information
```sql
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'splunk' AND table_name = 'web'
ORDER BY ordinal_position;
```

### List Splunk indexes
```sql
SELECT index_name, is_internal
FROM pg_catalog.splunk_indexes
ORDER BY index_name;
```

### Get Splunk source information
```sql
SELECT table_name, cim_model, source_type
FROM pg_catalog.splunk_sources;
```

### Count columns per table
```sql
SELECT t.table_name, COUNT(c.column_name) as column_count
FROM information_schema.tables t
JOIN information_schema.columns c
  ON t.table_catalog = c.table_catalog
  AND t.table_schema = c.table_schema
  AND t.table_name = c.table_name
WHERE t.table_schema = 'splunk'
GROUP BY t.table_name;
```

## Connection Examples

### Via Model File
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

With PostgreSQL-style lexical settings:
```java
String url = "jdbc:calcite:model=/path/to/model.json"
    + ";unquotedCasing=TO_LOWER"
    + ";quotedCasing=UNCHANGED"
    + ";caseSensitive=true";
```

Then query with natural lowercase SQL:
```sql
-- Tables are in the splunk schema
SELECT * FROM splunk.web WHERE _time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Metadata is in top-level schemas (no prefix needed)
SELECT * FROM pg_catalog.pg_tables;
SELECT * FROM information_schema.columns;
```

### Via Dynamic Model (Recommended)
```java
// Create model with PostgreSQL-style settings
ObjectMapper mapper = new ObjectMapper();
ObjectNode model = mapper.createObjectNode();
model.put("version", "1.0");
model.put("defaultSchema", "splunk");

ObjectNode schema = mapper.createObjectNode();
schema.put("name", "splunk");
schema.put("type", "custom");
schema.put("factory", "org.apache.calcite.adapter.splunk.SplunkSchemaFactory");

ObjectNode operand = mapper.createObjectNode();
operand.put("url", "https://localhost:8089");
operand.put("username", "admin");
operand.put("password", "changeme");
operand.put("cimModel", "web");

schema.set("operand", operand);
model.set("schemas", mapper.createArrayNode().add(schema));

String url = "jdbc:calcite:model=inline:" + mapper.writeValueAsString(model)
    + ";unquotedCasing=TO_LOWER"
    + ";quotedCasing=UNCHANGED"
    + ";caseSensitive=true";

Connection conn = DriverManager.getConnection(url);

// Query data with lowercase identifiers
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM splunk.web LIMIT 10");

// Query metadata (top-level schemas)
ResultSet meta = stmt.executeQuery("SELECT * FROM pg_catalog.pg_tables");
```

### Programmatic Access
```java
// Create Splunk connection
SplunkConnection splunkConn = new SplunkConnectionImpl(url, username, password);

// Create schema - metadata schemas are automatically added at root level
SplunkSchema schema = new SplunkSchema(splunkConn);

// Note: pg_catalog and information_schema are added as top-level schemas
// by SplunkSchemaFactory, not as sub-schemas of the user schema
```

## Benefits

1. **No Additional Setup**: Metadata schemas are automatically available
2. **PostgreSQL Compatibility**: Use natural lowercase SQL without quotes
3. **Standard Schema Structure**: System catalogs are top-level schemas, not sub-schemas
4. **Tool Compatibility**: Works with PostgreSQL tools and drivers
5. **Schema Discovery**: Discover structure using standard `information_schema` queries
6. **Splunk-Specific Metadata**: Access indexes and CIM model information
7. **Cross-Schema Joins**: Join metadata tables for complex analysis

## PostgreSQL-Style Lexical Behavior

The Splunk adapter implements PostgreSQL-style lexical conventions:

- **Unquoted identifiers**: Converted to lowercase (`table_name` → `table_name`)
- **Quoted identifiers**: Case preserved (`"Table_Name"` → `Table_Name`)
- **Case-sensitive matching**: After normalization
- **Double-quote style**: PostgreSQL-compatible quoting

### Natural Lowercase SQL Examples

```sql
-- ✅ All these work without quotes (PostgreSQL-style)
SELECT table_name FROM information_schema.tables;
SELECT column_name, data_type FROM information_schema.columns;
SELECT schemaname, tablename FROM pg_catalog.pg_tables;
SELECT index_name FROM pg_catalog.splunk_indexes;

-- ✅ Mixed case with quotes when needed
SELECT "Table_Name" FROM "Custom_Schema"."Special_Table";

-- ✅ Standard PostgreSQL patterns work
SELECT t.table_name, COUNT(c.column_name) as column_count
FROM information_schema.tables t
JOIN information_schema.columns c USING (table_schema, table_name)
WHERE t.table_schema = 'splunk'
GROUP BY t.table_name;
```

### Configuration

To enable PostgreSQL-style behavior, use these connection properties:
```java
String url = "jdbc:calcite:model=..."
    + ";unquotedCasing=TO_LOWER"        // Convert unquoted to lowercase
    + ";quotedCasing=UNCHANGED"         // Preserve quoted case
    + ";caseSensitive=true"             // Case-sensitive matching
    + ";quoting=DOUBLE_QUOTE";          // PostgreSQL-style quotes
```
