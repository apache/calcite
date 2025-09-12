# Apache Calcite Government Data Adapter

[![Build Status](https://github.com/apache/calcite/actions/workflows/gradle.yml/badge.svg)](https://github.com/apache/calcite/actions/workflows/gradle.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The Government Data Adapter provides unified SQL access to various government data sources through Apache Calcite. Currently supports SEC financial data with plans for Census, IRS, Treasury, and other government datasets.

## Quick Start

### Basic SEC Query
```java
// Connect to SEC data
String url = "jdbc:govdata:ciks=AAPL,MSFT&startYear=2023&endYear=2024";
Connection conn = DriverManager.getConnection(url);

// Query financial data
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT cik, company_name, fiscal_year, concept, value " +
    "FROM financial_line_items " + 
    "WHERE concept = 'NetIncomeLoss' AND fiscal_year >= 2023");
```

### Model-Based Configuration
```json
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "directory": "/path/to/cache",
      "ciks": ["AAPL", "MSFT", "GOOGL"],
      "startYear": 2020,
      "endYear": 2024
    }
  }]
}
```

## Features

### SEC Financial Data
- **10-K/10-Q/8-K Filings**: Automatic XBRL extraction and normalization
- **Insider Trading**: Forms 3, 4, 5 ownership data
- **Stock Prices**: Real-time and historical price integration
- **Text Analytics**: MD&A, footnotes, risk factors extraction
- **Earnings Transcripts**: 8-K earnings call content

### Data Processing
- **Smart Caching**: Intelligent local storage and refresh
- **Partitioned Storage**: Efficient Parquet organization by CIK/filing type/year  
- **Multiple Engines**: DuckDB, Arrow, LINQ4J, Parquet execution support
- **Text Similarity**: Vector embeddings for semantic search

### Company Identification
- **CIK Registry**: 13,000+ ticker-to-CIK mappings
- **Smart Groups**: FAANG, MAGNIFICENT7, RUSSELL2000, SP500, DJIA
- **Flexible Input**: Support for tickers, CIKs, company names

## Installation

### Maven
```xml
<dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-govdata</artifactId>
    <version>1.41.0-SNAPSHOT</version>
</dependency>
```

### Gradle
```kotlin
implementation("org.apache.calcite:calcite-govdata:1.41.0-SNAPSHOT")
```

## Data Sources

### Currently Supported
- **SEC EDGAR**: Financial filings, insider trading, earnings

### Planned Support
- **US Census**: Demographics, economic indicators
- **IRS**: Tax statistics, exempt organizations  
- **Treasury**: Government spending, debt data
- **Federal Reserve**: Economic data, interest rates

## Configuration

### Connection Parameters
| Parameter | Description | Example |
|-----------|-------------|---------|
| `ciks` | Companies (tickers/CIKs/groups) | `AAPL,MSFT,FAANG` |
| `startYear` | Beginning year | `2020` |
| `endYear` | Ending year | `2024` |
| `filingTypes` | SEC filing types | `10-K,10-Q,8-K` |
| `directory` | Cache directory | `/tmp/sec-cache` |
| `executionEngine` | Query engine | `duckdb` |

### Smart Groups
```java
// Use predefined groups
String url = "jdbc:govdata:ciks=MAGNIFICENT7&startYear=2023";

// Available groups: FAANG, MAGNIFICENT7, RUSSELL2000, SP500, DJIA
```

### Advanced Features
```json
{
  "operand": {
    "ciks": ["AAPL"],
    "textSimilarity": {
      "enabled": true,
      "embeddingDimension": 256,
      "provider": "local"
    },
    "extractFootnotes": true,
    "extractMDA": true,
    "fetchStockPrices": true
  }
}
```

## Table Schema

### Core Financial Tables
- `financial_line_items` - Standardized financial facts
- `financial_facts` - Raw XBRL data with contexts
- `company_metadata` - Company information and identifiers
- `management_discussion` - MD&A text extraction
- `insider_transactions` - Forms 3/4/5 ownership data
- `earnings_transcripts` - 8-K earnings call content
- `stock_prices` - Daily price and volume data

### Text Analytics Tables
- `vectorized_blobs` - Document embeddings for similarity search
- `footnotes` - Financial statement footnotes
- `risk_factors` - Risk factor disclosures

## Examples

### Financial Analysis
```sql
-- Revenue growth analysis
SELECT 
    cik, 
    fiscal_year,
    SUM(value) as total_revenue
FROM financial_line_items 
WHERE concept IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax')
  AND cik IN ('0000320193', '0000789019')  -- Apple, Microsoft
GROUP BY cik, fiscal_year
ORDER BY cik, fiscal_year;
```

### Insider Trading Analysis  
```sql
-- Recent insider transactions
SELECT 
    cik,
    person_name,
    transaction_date,
    transaction_type,
    shares_owned
FROM insider_transactions 
WHERE transaction_date >= '2024-01-01'
  AND cik = '0000320193'  -- Apple
ORDER BY transaction_date DESC;
```

### Text Similarity Search
```sql
-- Find similar business descriptions
SELECT 
    cik,
    company_name,
    COSINE_SIMILARITY(embedding, target_embedding) as similarity
FROM vectorized_blobs 
WHERE blob_type = 'business_description'
  AND COSINE_SIMILARITY(embedding, target_embedding) > 0.8;
```

## Performance

### Caching Strategy
- **Incremental Updates**: Only fetch new/changed filings
- **Partitioned Storage**: Efficient queries by company/date
- **Smart Refresh**: RSS-based change detection
- **Parallel Processing**: Multi-threaded downloads and conversion

### Query Optimization
- **Push-down Predicates**: CIK, date, concept filtering to storage
- **Columnar Storage**: Parquet format for analytical queries
- **Vectorized Execution**: DuckDB integration for fast analytics
- **Metadata Caching**: Pre-computed statistics and indexes

## Development

### Building
```bash
./gradlew :govdata:build
```

### Testing
```bash
# Unit tests
./gradlew :govdata:test

# Integration tests (requires network)
./gradlew :govdata:test -PincludeTags=integration
```

### Model Development
```bash
# Test with custom model
./gradlew :govdata:test --tests "*AppleNetIncomeTest*"
```

## Architecture

### Design Principles
1. **File Adapter Foundation**: Built on Calcite's file adapter for maximum reuse
2. **Government Data Router**: Unified entry point for multiple data sources  
3. **Extensible Schema**: Easy addition of new government data sources
4. **EDGAR Compliance**: Respects SEC rate limits and terms of service

### Component Structure
```
govdata/
├── GovDataSchemaFactory     # Main entry point and data source router
├── GovDataDriver           # JDBC driver for connection string parsing
├── sec/                    # SEC-specific implementation
│   ├── SecSchemaFactory    # SEC data schema and table management
│   ├── CikRegistry         # Ticker-to-CIK resolution
│   ├── XbrlToParquetConverter # XBRL processing pipeline
│   └── SecHttpStorageProvider # EDGAR-compliant HTTP client
└── common/                 # Shared utilities for government data
```

## Backward Compatibility

The adapter maintains compatibility with existing SEC adapter usage:

```java
// Legacy SEC adapter syntax still works
String url = "jdbc:sec:ciks=AAPL";

// Automatically routes to new GovData architecture
// with deprecation warnings
```

## Contributing

1. **File Issues**: Report bugs and feature requests on GitHub
2. **Submit PRs**: Follow Apache Calcite contribution guidelines  
3. **Add Data Sources**: Implement new government data connectors
4. **Improve Documentation**: Help expand usage examples

### Development Guidelines
- Follow Java 8 compatibility requirements
- Use proper JDBC testing patterns with model files
- Respect government API rate limits and terms of service
- Maintain SEC EDGAR compliance for financial data access

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Apache Calcite Docs](https://calcite.apache.org/)
- **Issues**: [GitHub Issues](https://github.com/apache/calcite/issues)
- **Mailing List**: [Apache Calcite Dev List](mailto:dev@calcite.apache.org)
- **Chat**: [Apache Calcite Slack](https://the-asf.slack.com/channels/calcite)

## Related Projects

- [Apache Calcite](https://calcite.apache.org/) - SQL parser and query engine
- [SEC EDGAR](https://www.sec.gov/edgar) - SEC filing database
- [DuckDB](https://duckdb.org/) - Analytical query engine
- [Apache Arrow](https://arrow.apache.org/) - Columnar data format