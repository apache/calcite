# XBRL Model File Examples

This directory contains example model files for different use cases with the XBRL adapter.

## Simplest Option: Use XBRL JDBC Driver (No Model File Needed!)

```java
// No model file required - just connect directly!
String url = "jdbc:xbrl:ciks=AAPL";
Connection conn = DriverManager.getConnection(url);

// Or with multiple companies
String url = "jdbc:xbrl:ciks=MAGNIFICENT7";
```

The `jdbc:xbrl:` driver automatically:
- Generates the model configuration inline
- Sets lex=ORACLE and unquotedCasing=TO_LOWER defaults
- Applies all smart defaults (filing types, years, etc.)

## Alternative: Model File Configuration

If you prefer using model files, here are the most common patterns:

### 1. Single Company (minimal-apple.json)
```json
{
  "version": "1.0",
  "defaultSchema": "XBRL",
  "schemas": [{
    "name": "XBRL",
    "factory": "org.apache.calcite.adapter.xbrl.XbrlEmbeddingSchemaFactory",
    "operand": {
      "ciks": "AAPL"
    }
  }]
}
```

### 2. Multiple Companies (tech-trio.json)
```json
{
  "version": "1.0",
  "defaultSchema": "XBRL",
  "schemas": [{
    "name": "XBRL",
    "factory": "org.apache.calcite.adapter.xbrl.XbrlEmbeddingSchemaFactory",
    "operand": {
      "ciks": ["AAPL", "MSFT", "GOOGL"]
    }
  }]
}
```

### 3. Company Groups (magnificent7.json)
```json
{
  "version": "1.0",
  "defaultSchema": "XBRL",
  "schemas": [{
    "name": "XBRL",
    "factory": "org.apache.calcite.adapter.xbrl.XbrlEmbeddingSchemaFactory",
    "operand": {
      "ciks": "MAGNIFICENT7"
    }
  }]
}
```

## Available Company Groups

- **FAANG**: Facebook/Meta, Apple, Amazon, Netflix, Google
- **MAGNIFICENT7**: Apple, Microsoft, Google, Amazon, Meta, Tesla, NVIDIA
- **BIG_TECH**: Major technology companies
- **BIG_BANKS**: JPMorgan Chase, Bank of America, Wells Fargo, Citigroup, Goldman Sachs
- **DOW10**: Top 10 Dow Jones Industrial Average companies
- **FORTUNE10**: Fortune 10 companies

## Default Settings

When you use these minimal configurations, the adapter automatically provides:

✅ **Filing Types**: All 15 SEC filing types (10-K, 10-Q, 8-K, etc.)
✅ **Date Range**: 2009 to current year
✅ **Stock Prices**: Daily EOD prices from Yahoo Finance (enabled by default)
✅ **Execution Engine**: DuckDB for fast queries
✅ **Embeddings**: 128-dimensional vectors with financial vocabulary
✅ **Vector Functions**: 7 similarity functions (cosine, euclidean, etc.)
✅ **Data Directory**: Automatic resolution (or use XBRL_DATA_DIRECTORY env var)
✅ **Partitioning**: Optimized Parquet partitions by company and year

## Stock Price Configuration

Stock prices are enabled by default. To control:

### Enable Stock Prices (default)
```json
{
  "operand": {
    "ciks": "AAPL",
    "fetchStockPrices": true  // Or omit - true by default
  }
}
```

### Disable Stock Prices
```json
{
  "operand": {
    "ciks": "AAPL",
    "fetchStockPrices": false  // Explicitly disable
  }
}
```

### Query Stock Prices
```sql
-- Simple price query
SELECT date, close, volume 
FROM stock_prices 
WHERE ticker = 'AAPL' AND year = 2023;

-- Join with financials
SELECT s.ticker, s.close, f.revenue
FROM stock_prices s
JOIN financial_line_items f ON s.cik = f.cik
WHERE s.ticker = 'AAPL';
```

## Advanced Examples

See the individual example files for more advanced configurations:

- `custom-years.json` - Custom date ranges
- `specific-filings.json` - Filter specific filing types
- `large-embeddings.json` - Higher dimensional embeddings
- `mixed-identifiers.json` - Mix tickers, CIKs, and groups
- `banking-sector.json` - Financial sector analysis
- `annual-only.json` - Only annual reports (10-K)
- `event-driven.json` - Focus on 8-K event filings
- `no-stock-prices.json` - Disable stock price downloads

## Comparison: JDBC Driver vs Model Files

| Approach | Pros | Cons |
|----------|------|------|
| `jdbc:xbrl:` driver | No model file needed, automatic defaults, simple URLs | Less flexibility for complex configs |
| Model files | Full control, reusable configurations, version control | Requires file management |

For most use cases, the `jdbc:xbrl:` driver is recommended for its simplicity.

## Environment Variables

Set these to override defaults:

```bash
export XBRL_DATA_DIRECTORY=/Volumes/T9/xbrl-data
export EDGAR_CIKS=AAPL,MSFT,GOOGL
export EDGAR_START_YEAR=2020
export EDGAR_END_YEAR=2023
export SEC_FETCH_STOCK_PRICES=true  # Enable stock prices (default: true)
```
