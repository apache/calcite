# SEC Adapter Usage with Data Directory

## Directory Structure

The SEC adapter uses a single data directory containing both source and processed files:

```
$SEC_DATA_DIRECTORY/
├── sec/           # Source SEC/XML files
│   ├── aapl-10k-2023.xml
│   └── ...
├── parquet/        # Converted Parquet files with embeddings
│   ├── financial_line_items/
│   ├── company_info/
│   ├── footnotes/
│   └── document_embeddings/
└── test-model.json # Generated model file (optional)
```

## Environment Variable

Set the `SEC_DATA_DIRECTORY` environment variable to point to your data directory:

```bash
export SEC_DATA_DIRECTORY=/Volumes/T9/sec-data
```

## Model File Configuration

With the `SecEmbeddingSchemaFactory`, you only need minimal configuration:

### Minimal Configuration (13 lines!)
```json
{
  "version": "1.0",
  "defaultSchema": "SEC",
  "schemas": [
    {
      "name": "SEC",
      "factory": "org.apache.calcite.adapter.sec.SecEmbeddingSchemaFactory",
      "operand": {
        "directory": "parquet"
      }
    }
  ]
}
```

### What You Get Automatically

The SecEmbeddingSchemaFactory provides smart defaults for everything:

✅ **Execution Engine**: DuckDB (high-performance analytics)
✅ **Embedding System**:
   - Financial vocabulary (164 terms)
   - TF-IDF based vectorization
   - 128-dimension embeddings
   - Automatic generation on conversion

✅ **Vector Functions**:
   - `COSINE_SIMILARITY(vector1, vector2)` - Most common for text similarity
   - `COSINE_DISTANCE(vector1, vector2)` - Distance metric (1 - similarity)
   - `EUCLIDEAN_DISTANCE(vector1, vector2)` - L2 distance
   - `DOT_PRODUCT(vector1, vector2)` - Raw similarity score
   - `VECTORS_SIMILAR(vector1, vector2, threshold)` - Boolean similarity check
   - `VECTOR_NORM(vector)` - Vector magnitude
   - `NORMALIZE_VECTOR(vector)` - Unit vector normalization

✅ **Standard Tables**:
   - `financial_line_items` - Partitioned financial data
   - `company_info` - Company metadata
   - `footnotes` - Partitioned footnotes
   - `document_embeddings` - Partitioned embeddings with vector column
   - `stock_prices` - Daily EOD stock prices (when enabled)

✅ **Table Constraints** (for query optimization):
   - Primary keys defined for all tables
   - Foreign key relationships between related tables
   - Constraints exposed via JDBC metadata
   - Used by optimizer for join elimination and reordering

### Comparison with Verbose Configuration

Without smart defaults, you would need 60+ lines of configuration to achieve the same setup!

## Running Tests

```bash
# Set data directory (optional, defaults to build/apple-edgar-test-data)
export SEC_DATA_DIRECTORY=/Volumes/T9/sec-test-data

# Run the embedding test
./gradlew :sec:test --tests "*.SecEmbeddingTest"
```

## Table Constraints and Query Optimization

The SEC adapter defines PRIMARY KEY and FOREIGN KEY constraints for all tables. While these constraints are not enforced (since Parquet files are immutable), they provide significant benefits:

### Benefits of Constraints

1. **Query Optimization**: The Calcite optimizer uses constraint metadata to:
   - Eliminate unnecessary joins when uniqueness is guaranteed
   - Reorder joins for better performance
   - Apply constraint-based pruning
   - Generate more efficient execution plans

2. **Tool Integration**: JDBC metadata APIs expose constraints to:
   - BI tools (Tableau, PowerBI, etc.) for automatic relationship detection
   - SQL IDEs (DBeaver, DataGrip, etc.) for visual schema design
   - Data lineage and documentation tools

### Example Constraint Definitions

The SEC adapter automatically configures these constraints:

```json
{
  "tables": [{
    "name": "financial_line_items",
    "constraints": {
      "primaryKey": ["cik", "filing_type", "year", "filing_date", "concept", "context_ref"],
      "foreignKeys": [{
        "columns": ["context_ref"],
        "targetTable": ["SEC", "filing_contexts"],
        "targetColumns": ["context_id"]
      }]
    }
  }]
}
```

### Accessing Constraint Metadata via JDBC

```java
Connection conn = DriverManager.getConnection("jdbc:calcite:model=model.json");
DatabaseMetaData metadata = conn.getMetaData();

// Get primary keys
ResultSet pkRS = metadata.getPrimaryKeys(null, "SEC", "financial_line_items");
while (pkRS.next()) {
    System.out.println("PK Column: " + pkRS.getString("COLUMN_NAME"));
}

// Get foreign keys
ResultSet fkRS = metadata.getImportedKeys(null, "SEC", "financial_line_items");
while (fkRS.next()) {
    System.out.println("FK: " + fkRS.getString("FKCOLUMN_NAME") +
                      " -> " + fkRS.getString("PKTABLE_NAME") + "." +
                      fkRS.getString("PKCOLUMN_NAME"));
}
```

## Usage in Code

```java
// Connection URL using model file
String url = "jdbc:calcite:model=path/to/model.json";

Properties info = new Properties();
info.setProperty("lex", "ORACLE");
info.setProperty("unquotedCasing", "TO_LOWER");

try (Connection conn = DriverManager.getConnection(url, info)) {
    // Query with vector similarity
    String sql = "SELECT label, concept, value, " +
                 "COSINE_SIMILARITY(embedding_vector, ?) as similarity " +
                 "FROM financial_line_items " +
                 "WHERE similarity > 0.7 " +
                 "ORDER BY similarity DESC";

    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setString(1, targetEmbedding);
    ResultSet rs = stmt.executeQuery();
    // Process results...
}
```
