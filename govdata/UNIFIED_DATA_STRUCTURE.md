# Unified Government Data Structure

## Overview

All government data (SEC filings, geographic data, etc.) is stored in a single hive-partitioned parquet structure on `/Volumes/T9/govdata-parquet`. This provides:

- **Unified storage**: All government data in one location
- **Efficient queries**: Hive partitioning enables partition pruning
- **Standard format**: Everything in Parquet for consistency
- **Scalability**: Easy to add new data sources

## Directory Structure

```
/Volumes/T9/govdata-parquet/
├── source=sec/                     # SEC filing data
│   ├── type=filing/
│   │   ├── filing_type=10K/
│   │   │   ├── year=2023/
│   │   │   │   ├── cik=0000320193/
│   │   │   │   │   └── *.parquet
│   │   │   └── year=2024/
│   │   ├── filing_type=10Q/
│   │   └── filing_type=8K/
│   ├── type=market/                # Market data
│   │   └── ticker=AAPL/
│   │       └── *.parquet
│   └── type=embedding/              # Text embeddings
│       └── *.parquet
│
└── source=geo/                     # Geographic data
    ├── type=boundary/               # TIGER/Line boundaries
    │   ├── dataset=states/
    │   │   └── *.parquet
    │   ├── dataset=counties/
    │   │   └── *.parquet
    │   ├── dataset=places/
    │   │   └── *.parquet
    │   └── dataset=zcta/            # ZIP code areas
    │       └── *.parquet
    ├── type=demographic/            # Census API data
    │   ├── dataset=acs5/            # American Community Survey
    │   │   └── year=2022/
    │   │       └── *.parquet
    │   └── dataset=decennial/       # Decennial Census
    │       └── year=2020/
    │           └── *.parquet
    └── type=crosswalk/              # HUD crosswalk data
        ├── dataset=zip_county/
        │   └── quarter=2024Q3/
        │       └── *.parquet
        ├── dataset=zip_tract/
        │   └── *.parquet
        └── dataset=zip_cbsa/        # Metro areas
            └── *.parquet
```

## Partition Scheme

### Level 1: Source
- `source=sec` - Securities and Exchange Commission data
- `source=geo` - Geographic/demographic data
- `source=irs` - (future) IRS tax data
- `source=bls` - (future) Bureau of Labor Statistics

### Level 2: Type
For SEC:
- `type=filing` - Company filings (10-K, 10-Q, 8-K, etc.)
- `type=market` - Stock prices and market data
- `type=embedding` - Text vectors for semantic search

For Geographic:
- `type=boundary` - Geographic boundaries (states, counties, cities)
- `type=demographic` - Population and economic data
- `type=crosswalk` - Mappings between geographic systems

### Level 3+: Data-specific partitions
Each data type has its own partitioning scheme optimized for common queries.

## Query Examples

```sql
-- Find all Apple 10-K filings
SELECT * FROM govdata
WHERE source = 'sec' 
  AND type = 'filing' 
  AND filing_type = '10K'
  AND cik = '0000320193';

-- Get California demographics
SELECT * FROM govdata
WHERE source = 'geo'
  AND type = 'demographic'
  AND dataset = 'acs5'
  AND state_fips = '06';

-- Join company locations with demographics
SELECT 
  s.company_name,
  s.zip,
  g.median_income,
  g.population
FROM govdata s
JOIN govdata g ON s.zip = g.zip
WHERE s.source = 'sec' 
  AND g.source = 'geo'
  AND g.type = 'demographic';
```

## Benefits

1. **Single Source of Truth**: All government data in one place
2. **Partition Pruning**: Queries only scan relevant partitions
3. **Consistent Format**: Everything in Parquet with standard schemas
4. **Cross-Domain Queries**: Easy to join SEC with geographic data
5. **Incremental Updates**: New data added as new partitions
6. **Storage Efficiency**: Parquet compression reduces storage needs

## Configuration

Set the following environment variable:
```bash
export GOVDATA_PARQUET_DIR=/Volumes/T9/govdata-parquet
```

Both SEC and Geographic adapters will automatically use this unified structure.