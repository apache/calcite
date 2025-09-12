# Government Data Adapter Usage Guide

The Government Data adapter provides unified access to various U.S. government data sources through SQL queries.

## Supported Data Sources

- **SEC (Securities and Exchange Commission)** - EDGAR filing data, company financials, insider trading
- **Census (U.S. Census Bureau)** - Demographics, economic data *(coming soon)*
- **IRS (Internal Revenue Service)** - Tax statistics, exempt organizations *(coming soon)*
- **Treasury (U.S. Treasury)** - Economic indicators, debt data *(coming soon)*

## Quick Start

### Using JDBC URLs (Recommended)

```java
// Connect to SEC data using ticker symbols (automatically converted to CIKs)
String url = "jdbc:govdata:source=sec&ciks=AAPL,MSFT";
Connection conn = DriverManager.getConnection(url);

// Equivalent using actual CIK numbers
// String url = "jdbc:govdata:source=sec&ciks=0000320193,0000789019";

// Future: Connect to other government sources
// String url = "jdbc:govdata:source=census&dataset=acs&geography=state";
```

#### Company Identifier Resolution

The GovData adapter automatically resolves company identifiers using the built-in CikRegistry:

- **Ticker Symbols**: `AAPL` → `0000320193` (Apple Inc.)
- **Company Groups**: `FAANG` → 5 CIKs for Facebook/Meta, Apple, Amazon, Netflix, Google
- **Raw CIKs**: `0000320193` → Used as-is (10-digit format with leading zeros)

**Common Examples:**
```java
// Individual companies by ticker
"ciks=AAPL"              → Apple (0000320193)
"ciks=MSFT"              → Microsoft (0000789019)
"ciks=GOOGL"             → Alphabet/Google (0001652044)

// Multiple companies
"ciks=AAPL,MSFT,GOOGL"   → Three major tech companies
"ciks=MAGNIFICENT7"       → Apple, Microsoft, Google, Amazon, Meta, Tesla, NVIDIA

// Mix of tickers, groups, and CIKs
"ciks=AAPL,FAANG,0001018724" → Apple + FAANG group + Amazon (by CIK)
```

**Available Company Groups:**
- `MAGNIFICENT7` - Apple, Microsoft, Google, Amazon, Meta, Tesla, NVIDIA
- `FAANG` - Facebook/Meta, Apple, Amazon, Netflix, Google
- `BIG_TECH` - Major technology companies (14 companies)
- `BIG_BANKS` - JPMorgan, Bank of America, Wells Fargo, Citigroup, Goldman Sachs, Morgan Stanley
- `DOW30` - All 30 Dow Jones Industrial Average companies
- `SP500` - S&P 500 representative sample (top 50 by weight)
- `FORTUNE100` - Fortune 100 companies
- `SEMICONDUCTORS` - NVIDIA, Intel, AMD, Qualcomm, Micron, etc.
- `AUTO` - Tesla, Ford, GM, Stellantis
- `ENERGY` - Chevron, ExxonMobil, Phillips 66, etc.

**Resolution Process:**
1. Input string is checked against group aliases first
2. Then checked against ticker symbol registry (150+ tickers)
3. Finally treated as raw CIK if not found in registry
4. All identifiers are normalized to 10-digit CIK format (with leading zeros)

### Using Model Files

```json
{
  "version": "1.0",
  "defaultSchema": "GOV", 
  "schemas": [{
    "name": "GOV",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "ciks": ["AAPL", "MSFT"],       // Tickers automatically converted to CIKs
      // "ciks": ["0000320193", "0000789019"], // Equivalent using raw CIKs
      "startYear": 2020,
      "endYear": 2023
    }
  }]
}
```

**Model File Examples:**
```json
// Using company groups
"ciks": ["FAANG"]                  // Expands to 5 CIKs

// Using single ticker
"ciks": "AAPL"                     // Also works as string (not array)

// Mixed identifiers  
"ciks": ["AAPL", "MAGNIFICENT7", "0001018724"]  // Ticker + group + raw CIK
```

# SEC Data Source Usage

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
