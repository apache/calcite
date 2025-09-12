# Stock Price Integration in SEC Adapter

## Overview

The SEC adapter includes built-in stock price functionality that automatically downloads and integrates daily End-of-Day (EOD) stock prices from Yahoo Finance. This enables powerful analytics combining fundamental SEC filing data with market price movements.

## Key Features

- **Automatic Downloads**: Fetches historical stock prices for all configured companies
- **Smart Caching**: Downloads once per ticker/year, with incremental updates
- **Rate Limiting**: Respects Yahoo Finance API limits with automatic throttling
- **Optimized Storage**: Partitioned Parquet files for efficient queries
- **SEC Integration**: CIK column enables seamless joins with filing data

## Configuration

### Enable/Disable Stock Prices

Stock prices are enabled by default. Control via:

#### Model File
```json
{
  "schemas": [{
    "name": "SEC",
    "factory": "org.apache.calcite.adapter.sec.SecSchemaFactory",
    "operand": {
      "fetchStockPrices": true,  // Enable (default: true)
      "ciks": ["AAPL", "MSFT"],
      "startYear": 2020,
      "endYear": 2023
    }
  }]
}
```

#### Connection URL
```java
// Enable stock prices
String url = "jdbc:sec:ciks=AAPL&fetchStockPrices=true";

// Disable stock prices
String url = "jdbc:sec:ciks=AAPL&fetchStockPrices=false";
```

#### Environment Variable
```bash
export SEC_FETCH_STOCK_PRICES=true
```

## Table Schema

The `stock_prices` table provides:

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| `ticker` | VARCHAR | Stock ticker symbol | Partition column (from directory) |
| `year` | INTEGER | Year of data | Partition column (from directory) |
| `cik` | VARCHAR | 10-digit CIK | Data column for joins |
| `date` | VARCHAR | Trading date (YYYY-MM-DD) | Data column |
| `open` | DOUBLE | Opening price | Nullable |
| `high` | DOUBLE | Daily high | Nullable |
| `low` | DOUBLE | Daily low | Nullable |
| `close` | DOUBLE | Closing price | Nullable |
| `adj_close` | DOUBLE | Split-adjusted close | Nullable |
| `volume` | BIGINT | Shares traded | Nullable |

## Storage Structure

### Partition Layout
```
sec-parquet/
└── stock_prices/
    ├── ticker=AAPL/
    │   ├── year=2022/
    │   │   └── AAPL_2022_prices.parquet
    │   ├── year=2023/
    │   │   └── AAPL_2023_prices.parquet
    │   └── year=2024/
    │       └── AAPL_2024_prices.parquet
    └── ticker=MSFT/
        └── year=2023/
            └── MSFT_2023_prices.parquet
```

### Why This Structure?
- **Efficient Queries**: Partition pruning on ticker and year
- **Scalability**: Each ticker/year is independent
- **Incremental Updates**: Easy to add new years
- **CIK in Data**: Enables joins without scanning partitions

## Query Patterns

### Basic Queries

```sql
-- Get recent prices for a ticker
SELECT date, close, volume
FROM stock_prices
WHERE ticker = 'AAPL' 
  AND year = 2023
ORDER BY date DESC
LIMIT 30;

-- Calculate returns
SELECT 
  ticker,
  date,
  close,
  LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close,
  (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / 
   LAG(close) OVER (PARTITION BY ticker ORDER BY date) * 100 as daily_return
FROM stock_prices
WHERE ticker = 'MSFT' AND year = 2023;
```

### Integration with SEC Data

```sql
-- Stock performance around earnings releases
SELECT 
  s.ticker,
  f.filing_date,
  f.filing_type,
  AVG(s.close) as avg_price_week_before,
  f.revenue,
  f.net_income
FROM stock_prices s
JOIN financial_line_items f ON s.cik = f.cik
WHERE s.date BETWEEN DATE_SUB(f.filing_date, INTERVAL 7 DAY) 
                 AND f.filing_date
  AND f.filing_type = '10-Q'
GROUP BY s.ticker, f.filing_date, f.filing_type, f.revenue, f.net_income;

-- Price-to-Earnings ratio over time
SELECT 
  s.ticker,
  s.date,
  s.close as stock_price,
  f.earnings_per_share,
  s.close / NULLIF(f.earnings_per_share, 0) as pe_ratio
FROM stock_prices s
JOIN (
  SELECT cik, fiscal_year, fiscal_quarter,
         net_income / shares_outstanding as earnings_per_share
  FROM financial_line_items
) f ON s.cik = f.cik
WHERE YEAR(s.date) = f.fiscal_year
  AND QUARTER(s.date) = f.fiscal_quarter;

-- Market cap calculation
SELECT 
  c.company_name,
  s.ticker,
  s.date,
  s.close * f.shares_outstanding as market_cap
FROM stock_prices s
JOIN company_info c ON s.cik = c.cik
JOIN financial_line_items f ON s.cik = f.cik
WHERE s.date = f.period_end_date
ORDER BY market_cap DESC;
```

### Advanced Analytics

```sql
-- Volatility calculation (30-day rolling)
SELECT 
  ticker,
  date,
  close,
  STDDEV(close) OVER (
    PARTITION BY ticker 
    ORDER BY date 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as volatility_30d
FROM stock_prices
WHERE year = 2023;

-- Correlation between companies
SELECT 
  s1.ticker as ticker1,
  s2.ticker as ticker2,
  CORR(s1.daily_return, s2.daily_return) as correlation
FROM (
  SELECT ticker, date,
    (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / 
     LAG(close) OVER (PARTITION BY ticker ORDER BY date) as daily_return
  FROM stock_prices
) s1
JOIN (
  SELECT ticker, date,
    (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / 
     LAG(close) OVER (PARTITION BY ticker ORDER BY date) as daily_return
  FROM stock_prices
) s2 ON s1.date = s2.date
WHERE s1.ticker < s2.ticker  -- Avoid duplicates
GROUP BY s1.ticker, s2.ticker;
```

## Rate Limiting & Performance

### Yahoo Finance API Limits
- **Max Parallel**: 3 concurrent downloads
- **Initial Delay**: 500ms between requests
- **Backoff**: Exponential increase on 429 errors
- **Max Delay**: 5000ms between requests
- **Retry Logic**: 3 attempts with exponential backoff

### Caching Strategy
- **Download Once**: Each ticker/year downloaded only once
- **Manifest File**: Tracks completed downloads
- **Incremental**: Only missing data is fetched
- **Location**: `stock_prices/downloaded.manifest`

### Performance Tips
1. **Use Partition Filters**: Always include `ticker` and/or `year` in WHERE clause
2. **Limit Date Ranges**: Use year partition to reduce scan
3. **Join on CIK**: More efficient than ticker for SEC data joins
4. **Aggregate Early**: Push down aggregations before joins

## Test Mode

For development and testing without API calls:

```json
{
  "operand": {
    "testMode": true,
    "useMockData": true,
    "fetchStockPrices": true
  }
}
```

This generates synthetic price data with:
- Consistent base prices per ticker
- Realistic daily variations
- 3 months of data per year
- Proper null handling

## Troubleshooting

### Common Issues

#### No Stock Price Data
```sql
-- Check if table exists
SELECT COUNT(*) FROM stock_prices;

-- Verify downloads completed
-- Check logs for "Downloaded stock prices" or errors
```

#### Missing Tickers
- Some CIKs may not have public tickers
- Delisted companies return 404 (handled gracefully)
- Check `CikRegistry` for ticker mappings

#### Rate Limit Errors
- Automatic retry with backoff
- Check logs for "Rate limit hit" messages
- Delay automatically increases to MAX_RATE_LIMIT_MS

#### Incomplete Data
- Yahoo may not have full history for all tickers
- Weekends/holidays have no data (expected)
- Check `downloaded.manifest` for completed downloads

### Manual Refresh

To force re-download:
1. Delete specific partition: `rm -rf stock_prices/ticker=AAPL/year=2023/`
2. Remove from manifest: Edit `stock_prices/downloaded.manifest`
3. Restart connection to trigger download

## Architecture Notes

### Design Decisions

1. **Ticker/Year Partitioning**: Optimizes most common query patterns
2. **CIK as Data Column**: Enables joins without partition scanning
3. **Yahoo Finance Source**: Free, reliable, comprehensive coverage
4. **Parquet Format**: Columnar storage for analytics
5. **Manifest Tracking**: Prevents redundant API calls

### Integration Points

- **CikRegistry**: Maps CIKs to tickers automatically
- **FileSchema**: Handles partitioned table discovery
- **SecSchemaFactory**: Orchestrates downloads and table creation
- **YahooFinanceDownloader**: Manages API interaction and rate limiting

## Future Enhancements

Potential improvements for consideration:
- Real-time price updates
- Options chain data
- International exchanges
- Cryptocurrency prices
- Alternative data sources (Alpha Vantage, IEX Cloud)
- Streaming updates via WebSocket