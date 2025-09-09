# Table Constraints in File Adapter

## Overview

The File Adapter supports defining PRIMARY KEY and FOREIGN KEY constraints for file-based tables through configuration. While these constraints are not enforced at the storage level (since files like Parquet are immutable), they serve important purposes:

1. **Query Optimization** - The Calcite optimizer uses constraint metadata to:
   - Eliminate unnecessary joins when uniqueness is guaranteed
   - Reorder joins for better performance
   - Apply constraint-based pruning
   - Generate more efficient execution plans

2. **Documentation** - Constraints document the logical data model and relationships between tables

3. **Tool Integration** - JDBC metadata APIs expose constraints to:
   - BI tools (Tableau, PowerBI, etc.)
   - SQL IDEs (DBeaver, DataGrip, etc.)
   - Data lineage tools
   - Schema documentation generators

4. **Future Compatibility** - If enforcement is added later, the metadata infrastructure is already in place

## Configuration

Constraints are defined in the table configuration within your model.json file.

### Basic Structure

```json
{
  "tables": [{
    "name": "orders",
    "type": "PartitionedParquetTable",
    "pattern": "year=*/month=*/orders_*.parquet",
    "constraints": {
      "primaryKey": ["order_id"],
      "foreignKeys": [{
        "columns": ["customer_id"],
        "targetTable": ["SCHEMA_NAME", "customers"],
        "targetColumns": ["customer_id"]
      }]
    }
  }]
}
```

### Primary Keys

Define a primary key as a list of column names that uniquely identify each row:

```json
"constraints": {
  "primaryKey": ["column1", "column2", "column3"]
}
```

Example for a composite primary key:
```json
"constraints": {
  "primaryKey": ["year", "month", "transaction_id"]
}
```

### Foreign Keys

Define foreign key relationships to other tables:

```json
"constraints": {
  "foreignKeys": [{
    "sourceTable": ["SCHEMA_NAME", "source_table"],  // Optional, defaults to current table
    "columns": ["fk_column1", "fk_column2"],
    "targetTable": ["SCHEMA_NAME", "target_table"],
    "targetColumns": ["pk_column1", "pk_column2"]
  }]
}
```

Multiple foreign keys can be defined:
```json
"constraints": {
  "foreignKeys": [
    {
      "columns": ["customer_id"],
      "targetTable": ["SALES", "customers"],
      "targetColumns": ["id"]
    },
    {
      "columns": ["product_id"],
      "targetTable": ["INVENTORY", "products"],
      "targetColumns": ["id"]
    }
  ]
}
```

### Unique Keys

In addition to primary keys, you can define other unique constraints:

```json
"constraints": {
  "primaryKey": ["id"],
  "uniqueKeys": [
    ["email"],
    ["tax_id", "country_code"]
  ]
}
```

## Complete Example

Here's a complete example showing a sales schema with constraints:

```json
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [{
    "name": "SALES",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data/sales",
      "executionEngine": "DUCKDB",
      "partitionedTables": [
        {
          "name": "customers",
          "pattern": "customers_*.parquet",
          "constraints": {
            "primaryKey": ["customer_id"],
            "uniqueKeys": [["email"], ["tax_id"]]
          }
        },
        {
          "name": "orders",
          "pattern": "year=*/month=*/orders_*.parquet",
          "partitions": {
            "style": "hive",
            "columnDefinitions": [
              {"name": "year", "type": "INTEGER"},
              {"name": "month", "type": "INTEGER"}
            ]
          },
          "constraints": {
            "primaryKey": ["year", "month", "order_id"],
            "foreignKeys": [{
              "columns": ["customer_id"],
              "targetTable": ["SALES", "customers"],
              "targetColumns": ["customer_id"]
            }]
          }
        },
        {
          "name": "order_items",
          "pattern": "year=*/month=*/order_items_*.parquet",
          "partitions": {
            "style": "hive",
            "columnDefinitions": [
              {"name": "year", "type": "INTEGER"},
              {"name": "month", "type": "INTEGER"}
            ]
          },
          "constraints": {
            "primaryKey": ["year", "month", "order_id", "line_item_id"],
            "foreignKeys": [
              {
                "columns": ["year", "month", "order_id"],
                "targetTable": ["SALES", "orders"],
                "targetColumns": ["year", "month", "order_id"]
              },
              {
                "columns": ["product_id"],
                "targetTable": ["INVENTORY", "products"],
                "targetColumns": ["product_id"]
              }
            ]
          }
        }
      ]
    }
  }]
}
```

## Impact on Query Optimization

### Join Elimination

When the optimizer knows about foreign key constraints, it can eliminate unnecessary joins:

```sql
-- Query
SELECT o.order_id, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 100

-- With FK constraint, optimizer knows every order has a valid customer
-- Can be optimized to:
SELECT order_id, amount
FROM orders
WHERE amount > 100
```

### Join Reordering

Primary key information helps the optimizer choose better join orders:

```sql
-- With PK/FK metadata, optimizer knows cardinality relationships
SELECT *
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id

-- Optimizer can reorder based on selectivity and uniqueness guarantees
```

### Uniqueness Guarantees

Primary keys provide uniqueness guarantees for aggregation optimization:

```sql
-- Optimizer knows customer_id is unique in customers table
SELECT c.customer_id, c.name, SUM(o.amount)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name

-- Can be optimized knowing c.name is functionally dependent on c.customer_id
```

## JDBC Metadata Support

Constraints are exposed through standard JDBC DatabaseMetaData APIs:

```java
Connection conn = DriverManager.getConnection("jdbc:calcite:model=model.json");
DatabaseMetaData metadata = conn.getMetaData();

// Get primary keys
ResultSet pkRS = metadata.getPrimaryKeys(null, "SALES", "orders");
while (pkRS.next()) {
    String columnName = pkRS.getString("COLUMN_NAME");
    int keySeq = pkRS.getInt("KEY_SEQ");
    System.out.println("PK Column: " + columnName + " (position: " + keySeq + ")");
}

// Get foreign keys
ResultSet fkRS = metadata.getImportedKeys(null, "SALES", "orders");
while (fkRS.next()) {
    String fkColumn = fkRS.getString("FKCOLUMN_NAME");
    String pkTable = fkRS.getString("PKTABLE_NAME");
    String pkColumn = fkRS.getString("PKCOLUMN_NAME");
    System.out.println("FK: " + fkColumn + " -> " + pkTable + "." + pkColumn);
}

// Get unique keys
ResultSet ukRS = metadata.getIndexInfo(null, "SALES", "customers", true, false);
while (ukRS.next()) {
    if (!ukRS.getBoolean("NON_UNIQUE")) {
        String columnName = ukRS.getString("COLUMN_NAME");
        String indexName = ukRS.getString("INDEX_NAME");
        System.out.println("Unique: " + indexName + " on column " + columnName);
    }
}
```

## Limitations

1. **No Enforcement** - Constraints are metadata-only and not enforced during data writes
2. **Trust-Based** - The optimizer trusts that constraint definitions are accurate
3. **Immutable Storage** - Since Parquet files are immutable, constraint violations won't be detected at write time
4. **Column Order** - Foreign key column mappings must match by position, not name

## Best Practices

1. **Define Constraints Accurately** - Incorrect constraint definitions can lead to wrong query results
2. **Include Partition Columns** - When using partitioned tables, include partition columns in keys
3. **Document Assumptions** - Comment on any assumptions about data integrity
4. **Test Query Plans** - Verify that constraints are improving query plans using EXPLAIN
5. **Version Control** - Track constraint changes in version control as they affect query behavior

## Future Enhancements

Planned improvements for constraint support:

1. **Validation Mode** - Option to validate existing data against constraints
2. **Enforcement Mode** - For writable adapters, enforce constraints on writes
3. **Derived Constraints** - Automatically infer constraints from data patterns
4. **Check Constraints** - Support for CHECK constraints for data validation
5. **Cascading Operations** - Support for CASCADE DELETE/UPDATE semantics in writable scenarios

## API Reference

### TableConstraints Class

The `org.apache.calcite.adapter.file.metadata.TableConstraints` class provides the constraint implementation:

```java
// Create constraints programmatically
Map<String, Object> constraints = new HashMap<>();
constraints.put("primaryKey", Arrays.asList("id", "version"));

Map<String, Object> fk = new HashMap<>();
fk.put("columns", Arrays.asList("customer_id"));
fk.put("targetTable", Arrays.asList("SCHEMA", "customers"));
fk.put("targetColumns", Arrays.asList("id"));
constraints.put("foreignKeys", Arrays.asList(fk));

// Apply to table configuration
tableConfig.put("constraints", constraints);
```

### Statistic Interface

Constraints are exposed through Calcite's `Statistic` interface:

```java
public interface Statistic {
    List<ImmutableBitSet> getKeys();                    // Primary and unique keys
    List<RelReferentialConstraint> getReferentialConstraints(); // Foreign keys
    boolean isKey(ImmutableBitSet columns);            // Check if columns form a key
}
```

## Related Documentation

- [Partitioned Tables](./PARTITIONED_TABLES.md) - Using constraints with partitioned tables
- [Query Optimization](./OPTIMIZATION.md) - How constraints affect query planning
- [JDBC Metadata](./JDBC_METADATA.md) - Complete JDBC metadata API reference