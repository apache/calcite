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

### Comparison with Verbose Configuration

Without smart defaults, you would need 60+ lines of configuration to achieve the same setup!

## Running Tests

```bash
# Set data directory (optional, defaults to build/apple-edgar-test-data)
export SEC_DATA_DIRECTORY=/Volumes/T9/sec-test-data

# Run the embedding test
./gradlew :sec:test --tests "*.SecEmbeddingTest"
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
