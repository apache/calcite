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

### Comparison with Verbose Configuration

Without smart defaults, you would need 60+ lines of configuration to achieve the same setup!

## Running Tests

```bash
# Set data directory (optional, defaults to build/apple-edgar-test-data)
export SEC_DATA_DIRECTORY=/Volumes/T9/sec-test-data

# Run the embedding test
./gradlew :sec:test --tests "*.SecEmbeddingTest"
```

## Stock Price Data

The SEC adapter includes integrated stock price functionality that automatically downloads daily End-of-Day (EOD) prices from Yahoo Finance for all configured companies.

### Enabling Stock Prices

Stock prices are enabled by default. To control this feature:

```json
{
  "version": "1.0",
  "defaultSchema": "SEC",
  "schemas": [{
    "name": "SEC",
    "factory": "org.apache.calcite.adapter.sec.SecSchemaFactory",
    "operand": {
      "directory": "parquet",
      "fetchStockPrices": true,    // Enable stock price downloads (default: true)
      "ciks": ["AAPL", "MSFT"],
      "startYear": 2020,
      "endYear": 2023
    }
  }]
}
```

### Stock Price Table Schema

The `stock_prices` table includes:

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| `ticker` | VARCHAR | Stock ticker symbol | Partition column |
| `year` | INTEGER | Year of the data | Partition column |
| `cik` | VARCHAR | Company CIK for joins | Data column |
| `date` | VARCHAR | Trading date (YYYY-MM-DD) | Data column |
| `open` | DOUBLE | Opening price | Data column |
| `high` | DOUBLE | Daily high price | Data column |
| `low` | DOUBLE | Daily low price | Data column |
| `close` | DOUBLE | Closing price | Data column |
| `adj_close` | DOUBLE | Adjusted closing price | Data column |
| `volume` | BIGINT | Trading volume | Data column |

### Partition Strategy

Stock prices use optimized partitioning for efficient queries:
- **Directory structure**: `stock_prices/ticker=AAPL/year=2023/*.parquet`
- **Partition pruning**: Queries filtered by ticker and/or year skip irrelevant files
- **CIK column**: Included for joins with SEC filings

### Query Examples

```sql
-- Get Apple's stock prices for 2023
SELECT date, close, volume
FROM stock_prices
WHERE ticker = 'AAPL' AND year = 2023
ORDER BY date;

-- Join stock prices with financial data
SELECT 
  s.ticker,
  s.date,
  s.close as stock_price,
  f.revenue,
  f.net_income
FROM stock_prices s
JOIN financial_line_items f 
  ON s.cik = f.cik 
  AND EXTRACT(YEAR FROM s.date) = f.fiscal_year
WHERE s.ticker = 'AAPL'
  AND s.date = f.period_end_date;

-- Analyze stock performance around earnings
SELECT 
  s.ticker,
  AVG(s.close) as avg_price,
  MAX(s.high) as max_price,
  MIN(s.low) as min_price,
  f.fiscal_year,
  f.revenue
FROM stock_prices s
JOIN financial_line_items f ON s.cik = f.cik
WHERE EXTRACT(YEAR FROM s.date) = f.fiscal_year
GROUP BY s.ticker, f.fiscal_year, f.revenue
ORDER BY f.fiscal_year;

-- Find correlation between stock price and revenue
SELECT 
  s.ticker,
  CORR(s.adj_close, f.revenue) as price_revenue_correlation
FROM stock_prices s
JOIN financial_line_items f 
  ON s.cik = f.cik 
  AND DATE_TRUNC('quarter', s.date) = DATE_TRUNC('quarter', f.period_end_date)
GROUP BY s.ticker;
```

### Rate Limiting & Caching

- **Rate limiting**: Automatic throttling for Yahoo Finance API (max 3 parallel, 500ms delay)
- **Caching**: Downloads occur once per ticker/year combination
- **Incremental updates**: Only missing data is downloaded
- **Manifest tracking**: `.manifest` file prevents redundant downloads

### Test Mode

For testing without real API calls:

```json
{
  "operand": {
    "testMode": true,
    "useMockData": true,
    "fetchStockPrices": true
  }
}
```

This generates sample stock price data for testing queries without hitting the Yahoo Finance API.

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
