# Design Ideas

This document contains various design ideas and architectural proposals for enhancing Calcite adapters and integrations.

---

## 1. Doculyzer-Calcite Integration

### Overview
Create a unified document analysis system by integrating doculyzer as a Calcite adapter, enabling SQL queries over vectorized documents with zero configuration for end users.

### Architecture Vision
```
Documents → Doculyzer (parsing/embedding) → Delta Lake → DuckDB → Calcite → SQL Interface
```

### Key Benefits

#### 1. Simplified Usage
- **Single SQL interface** instead of Python API
- **Zero configuration** - just point at a directory
- **Familiar tooling** - any SQL client works (DBeaver, DataGrip, JDBC/ODBC)
- **Standard SQL joins** between documents and other data sources

#### 2. Automatic Document Pipeline
Users don't need to understand:
- Document parsing pipelines
- Embedding generation
- Vector storage
- Different document formats

Everything happens automatically when querying tables.

### Storage Architecture

#### Delta Lake Backend (Recommended)
- **Append-only pattern** - Perfect match for doculyzer's document versioning
- **Time travel queries** - Query documents as of any point in time
- **ACID transactions** - Safe concurrent writes
- **DuckDB integration** - Native support via `delta_scan()`
- **Automatic optimization** - Compaction and vacuum handled by Delta

#### Table Structure
```sql
-- Main tables exposed through Calcite
DOCUMENTS           -- Latest version of each document
DOCUMENTS_HISTORY   -- All document versions
ELEMENTS           -- Document elements with embeddings
VECTOR_SEARCH()    -- Table function for similarity search
DOCULYZER_STATUS   -- Processing status and metrics
```

### Implementation Approach

#### 1. Add Delta Lake Backend to Doculyzer
Create new storage backend leveraging doculyzer's pluggable architecture:
```python
class DeltaLakeDocumentDatabase(DocumentDatabase):
    def store_document(self, doc_id, metadata, elements):
        # Append-only writes to Delta Lake
        write_deltalake(path, df, mode="append")
```

#### 2. Async Processing Model
- Documents processed asynchronously in background
- Users can query immediately with available data
- Status monitoring through `DOCULYZER_STATUS` table
- Progressive enhancement as more documents are processed

#### 3. Calcite Adapter
```java
public class DoculyzerDeltaSchema extends AbstractSchema {
    // Expose Delta Lake tables through Calcite
    // Handle vector operations via DuckDB
    // Manage async indexing transparently
}
```

### Query Examples

#### Basic Search
```sql
-- Simple text search
SELECT * FROM documents 
WHERE content LIKE '%quarterly report%';

-- Vector similarity search
SELECT * FROM vector_search('machine learning algorithms', 0.8, 10);

-- Time travel query
SELECT * FROM documents FOR TIMESTAMP AS OF '2024-01-01'
WHERE doc_id = 'report_123';
```

#### Advanced Analytics
```sql
-- Join documents with operational data
SELECT 
    d.title,
    d.content,
    s.revenue,
    s.quarter
FROM documents d
JOIN sales_data s ON d.metadata->>'quarter' = s.quarter
WHERE d.topic = 'earnings';

-- Document version history
SELECT 
    doc_id,
    COUNT(*) as version_count,
    MIN(ingested_at) as first_seen,
    MAX(ingested_at) as last_updated
FROM documents_history
GROUP BY doc_id;
```

#### Monitoring
```sql
-- Check processing status
SELECT * FROM doculyzer_status;

-- See what's being processed
SELECT * FROM document_processing_log 
WHERE status IN ('PARSING', 'EMBEDDING')
ORDER BY started_at DESC;
```

### User Experience

```bash
# One command setup
java -jar calcite-doculyzer.jar --path /my/documents

# Connect with any SQL client
sqlline> !connect jdbc:calcite:model=doculyzer.json

# Query immediately (even while indexing continues)
sqlline> SELECT title, summary(content, 100) as preview 
         FROM documents 
         WHERE vector_search('customer complaints', 0.7)
         ORDER BY relevance DESC 
         LIMIT 10;
```

### Technical Advantages

#### Storage Benefits (Delta Lake)
- **Columnar format** - Efficient analytical queries via Parquet
- **Compression** - 10-100x smaller than JSON/SQLite
- **Partitioning** - By date/source for faster queries
- **Schema evolution** - Add fields without rewriting
- **Change data feed** - Track exactly what changed

#### Query Performance
- **Pushdown optimization** - Calcite optimizer handles planning
- **Parallel processing** - Built into Delta/DuckDB
- **Vector operations** - Native DuckDB array operations
- **Caching** - At SQL layer and Delta Lake level

#### Operational Benefits
- **No blocking** - Async indexing, immediate availability
- **Transparent monitoring** - Full visibility into processing
- **Automatic optimization** - Delta Lake handles compaction
- **Time travel** - Query any historical state

### Implementation Phases

#### Phase 1: Delta Lake Backend
- Implement `storage/delta_lake.py` in doculyzer
- Test append-only document storage
- Verify time travel capabilities

#### Phase 2: DuckDB Integration
- Set up DuckDB to query Delta tables
- Implement vector similarity functions
- Test performance with large datasets

#### Phase 3: Calcite Adapter
- Create `DoculyzerDeltaSchema` class
- Implement table mappings
- Add vector search table function

#### Phase 4: Async Processing
- Background document indexing
- Status monitoring tables
- Progressive query enhancement

### Summary
This integration transforms unstructured documents into a queryable database with zero user effort. By combining doculyzer's document processing with Delta Lake's storage and Calcite's SQL interface, users get a powerful, simple, and familiar way to analyze documents at scale with full version history and time travel capabilities.

---

## 2. US Government Data Adapter (us-gov)

### Overview
Create a unified SQL interface to all major US government data sources, transforming fragmented APIs and datasets into queryable Iceberg tables with comprehensive geographic metadata for cross-agency analysis.

### Problem Statement
US government agencies provide vast amounts of valuable public data (SEC filings, economic indicators, census data, crime statistics), but accessing this data requires:
- Dealing with dozens of different APIs
- Handling various authentication methods and rate limits
- Managing different data formats (XML, JSON, CSV, XBRL)
- No unified way to query across datasets
- Complex geographic code mappings between agencies

### Proposed Solution
A comprehensive `us-gov` adapter that aggregates data from multiple government sources into normalized, queryable Iceberg tables with proper geographic metadata layer for seamless cross-dataset joins.

### Data Sources

#### Financial Data
- **SEC EDGAR**: Financial filings (10-K, 10-Q, 8-K), XBRL data, company metadata
- **Treasury**: Government debt, auction data, yield curves

#### Economic Indicators
- **Bureau of Labor Statistics (BLS)**: Employment, unemployment, inflation (CPI), wages
- **Federal Reserve (FRED)**: Interest rates, GDP, 120,000+ economic time series
- **Bureau of Economic Analysis**: GDP components, international trade

#### Demographics & Social Data
- **Census Bureau**: Population, housing, business statistics, American Community Survey
- **FBI/DOJ**: Crime statistics (UCR/NIBRS), law enforcement data
- **CDC**: Health statistics, mortality data

#### Environmental & Infrastructure
- **NOAA**: Weather, climate data, natural disasters
- **Energy Information Administration**: Energy production, consumption, prices
- **Department of Transportation**: Traffic, accidents, infrastructure

### Core Schema Design

#### Economic Indicators (Unified)
```sql
CREATE TABLE us_gov.economic_indicators (
  source_agency VARCHAR,        -- 'BLS', 'FED', 'CENSUS'
  dataset_id VARCHAR,           -- 'UNRATE', 'GDP', 'CPI'
  series_id VARCHAR,            -- Specific time series identifier
  date DATE,
  value DECIMAL,
  unit VARCHAR,                 -- 'percent', 'dollars', 'index'
  frequency VARCHAR,            -- 'monthly', 'quarterly', 'annual'
  geography_type VARCHAR,       -- 'national', 'state', 'msa', 'county'
  geography_code VARCHAR,       -- FIPS or other standard code
  seasonal_adjustment VARCHAR,  -- 'SA', 'NSA'
  last_updated TIMESTAMP
);
```

#### SEC Financial Statements (Normalized from XBRL)
```sql
CREATE TABLE us_gov.sec_income_statements (
  accession_number VARCHAR,
  cik VARCHAR,
  ticker VARCHAR,
  company_name VARCHAR,
  fiscal_year INTEGER,
  fiscal_quarter INTEGER,
  period_end_date DATE,
  -- Core financials
  revenue DECIMAL,
  cost_of_revenue DECIMAL,
  gross_profit DECIMAL,
  operating_expenses DECIMAL,
  operating_income DECIMAL,
  net_income DECIMAL,
  earnings_per_share DECIMAL,
  -- Validation
  data_quality_score DECIMAL,
  calculation_errors ARRAY<STRING>
);

CREATE TABLE us_gov.sec_balance_sheets (
  accession_number VARCHAR,
  -- Assets
  total_assets DECIMAL,
  current_assets DECIMAL,
  cash_and_equivalents DECIMAL,
  -- Liabilities & Equity
  total_liabilities DECIMAL,
  current_liabilities DECIMAL,
  shareholders_equity DECIMAL
);

-- Detail line items with parent relationships
CREATE TABLE us_gov.sec_financial_details (
  accession_number VARCHAR,
  statement_type VARCHAR,       -- 'income', 'balance', 'cashflow'
  line_item_name VARCHAR,
  xbrl_tag VARCHAR,
  amount DECIMAL,
  parent_item VARCHAR          -- For hierarchical calculations
);
```

#### Crime Statistics
```sql
CREATE TABLE us_gov.crime_statistics (
  geography_type VARCHAR,       -- 'city', 'county', 'state', 'national'
  geography_code VARCHAR,       -- FIPS or agency code
  geography_name VARCHAR,
  date DATE,
  reporting_agency VARCHAR,
  crime_category VARCHAR,       -- 'violent', 'property', 'drug'
  crime_type VARCHAR,          -- 'homicide', 'robbery', 'burglary'
  incident_count INTEGER,
  arrest_count INTEGER,
  clearance_count INTEGER,
  rate_per_100k DECIMAL,
  data_completeness DECIMAL    -- % of agencies reporting
);
```

### Geographic Metadata Layer

#### Core Geographic Tables
```sql
-- ZIP Code master reference
CREATE TABLE us_gov.zip_codes (
  zip_code VARCHAR PRIMARY KEY,
  zip_type VARCHAR,            -- 'standard', 'po_box', 'unique'
  city VARCHAR,
  state_code VARCHAR,
  county_fips VARCHAR,
  cbsa_code VARCHAR,           -- Metropolitan Statistical Area
  census_tract VARCHAR,
  congressional_district VARCHAR,
  latitude DECIMAL,
  longitude DECIMAL,
  population INTEGER,
  land_area_sq_miles DECIMAL
);

-- Geographic hierarchy and crosswalks
CREATE TABLE us_gov.geographic_crosswalk (
  zip_code VARCHAR,
  census_tract VARCHAR,
  county_fips VARCHAR,
  cbsa_code VARCHAR,
  congressional_district VARCHAR,
  state_legislative_district VARCHAR,
  school_district VARCHAR
  -- Enables joins across different geographic systems
);

-- Metropolitan areas
CREATE TABLE us_gov.metropolitan_areas (
  cbsa_code VARCHAR PRIMARY KEY,
  cbsa_name VARCHAR,
  cbsa_type VARCHAR,           -- 'Metropolitan' or 'Micropolitan'
  principal_city VARCHAR,
  total_population INTEGER,
  state_codes ARRAY<VARCHAR>   -- Can span multiple states
);

-- Industry classifications
CREATE TABLE us_gov.naics_codes (
  naics_code VARCHAR PRIMARY KEY,
  industry_title VARCHAR,
  industry_description TEXT,
  parent_code VARCHAR          -- Hierarchical structure
);
```

### Configuration Example
```json
{
  "schemas": [{
    "name": "us_gov",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.usgov.USGovSchemaFactory",
    "operand": {
      "warehousePath": "/data/us-gov-warehouse",
      "catalogType": "iceberg",
      "sources": {
        "sec": {
          "enabled": true,
          "filingTypes": ["10-K", "10-Q", "8-K"],
          "startYear": 2019,
          "xbrlValidation": true
        },
        "bls": {
          "enabled": true,
          "apiKey": "${BLS_API_KEY}",
          "series": ["UNRATE", "CPIAUCSL", "PAYEMS"],
          "geographies": ["national", "state", "msa"]
        },
        "fred": {
          "enabled": true,
          "apiKey": "${FRED_API_KEY}",
          "series": ["GDP", "FEDFUNDS", "DGS10"],
          "frequency": "monthly"
        },
        "census": {
          "enabled": true,
          "apiKey": "${CENSUS_API_KEY}",
          "datasets": ["acs5", "population", "business"],
          "vintage": 2022
        },
        "fbi": {
          "enabled": true,
          "datasets": ["ucr", "nibrs"],
          "startYear": 2015
        }
      },
      "updateSchedule": "daily",
      "retentionYears": 25
    }
  }]
}
```

### Powerful Cross-Agency Query Examples

#### Economic Analysis
```sql
-- Correlate unemployment with company performance
SELECT 
  u.date,
  u.value as unemployment_rate,
  AVG(s.revenue_growth) as avg_revenue_growth,
  COUNT(DISTINCT s.cik) as reporting_companies,
  CORR(u.value, s.revenue_growth) as correlation
FROM us_gov.economic_indicators u
JOIN us_gov.sec_quarterly_metrics s 
  ON YEAR(u.date) = s.fiscal_year 
  AND QUARTER(u.date) = s.fiscal_quarter
WHERE u.series_id = 'UNRATE'
  AND u.date >= '2020-01-01'
GROUP BY u.date, u.value;

-- Regional economic health dashboard
SELECT 
  m.cbsa_name as metro_area,
  m.principal_city,
  e.unemployment_rate,
  c.violent_crime_rate,
  h.median_home_price,
  p.population_growth_yoy,
  s.company_count,
  s.total_market_cap
FROM us_gov.metropolitan_areas m
JOIN (SELECT geography_code, value as unemployment_rate 
      FROM us_gov.economic_indicators 
      WHERE series_id = 'UNEMPLOYMENT' AND date = CURRENT_DATE - INTERVAL 1 MONTH) e
  ON m.cbsa_code = e.geography_code
JOIN us_gov.crime_statistics c ON m.cbsa_code = c.geography_code
JOIN us_gov.census_housing h ON m.cbsa_code = h.geography_code
JOIN us_gov.census_population p ON m.cbsa_code = p.geography_code
JOIN (SELECT cbsa_code, COUNT(*) as company_count, SUM(market_cap) as total_market_cap
      FROM us_gov.sec_company_locations 
      GROUP BY cbsa_code) s ON m.cbsa_code = s.cbsa_code;
```

#### Business Intelligence
```sql
-- Company location risk assessment
SELECT 
  s.ticker,
  s.company_name,
  s.headquarters_zip,
  z.city,
  z.state_name,
  m.cbsa_name as metro_area,
  -- Economic indicators
  e.unemployment_rate,
  e.gdp_growth_rate,
  -- Risk factors
  c.violent_crime_rate,
  c.property_crime_rate,
  n.disaster_risk_score,
  -- Market indicators
  COUNT(*) OVER (PARTITION BY s.naics_code, z.cbsa_code) as local_competitors,
  AVG(s.revenue) OVER (PARTITION BY z.cbsa_code) as avg_local_revenue
FROM us_gov.sec_company_locations s
JOIN us_gov.zip_codes z ON s.headquarters_zip = z.zip_code
JOIN us_gov.metropolitan_areas m ON z.cbsa_code = m.cbsa_code
JOIN us_gov.economic_indicators e ON z.cbsa_code = e.geography_code
JOIN us_gov.crime_statistics c ON z.county_fips = c.geography_code
JOIN us_gov.noaa_disaster_risk n ON z.county_fips = n.county_fips
WHERE e.date = (SELECT MAX(date) FROM us_gov.economic_indicators);

-- Financial statement quality analysis
SELECT 
  fiscal_year,
  fiscal_quarter,
  COUNT(*) as total_filings,
  AVG(data_quality_score) as avg_quality,
  SUM(CASE WHEN ARRAY_LENGTH(calculation_errors) > 0 THEN 1 ELSE 0 END) as filings_with_errors,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_income/revenue) as median_profit_margin
FROM us_gov.sec_income_statements
WHERE fiscal_year >= 2020
GROUP BY fiscal_year, fiscal_quarter
ORDER BY fiscal_year, fiscal_quarter;
```

### Implementation Architecture

#### Data Pipeline Components
```
Government APIs → Rate-Limited Fetchers → Format Normalizers → 
Quality Validators → Iceberg Writers → Calcite Adapter
```

#### Key Technical Components
- **Data Fetchers**: Rate-limited, authenticated API clients for each agency
- **XBRL Processor**: Parse and normalize SEC financial filings
- **Geographic Normalizer**: Standardize location codes across agencies
- **Quality Validator**: Ensure data consistency and calculation accuracy
- **Iceberg Manager**: Handle versioning, time travel, and incremental updates
- **Metadata Enricher**: Add industry codes, geographic hierarchies

#### Quality Assurance
```sql
-- Built-in validation views
CREATE VIEW us_gov.data_quality_metrics AS
SELECT 
  source_agency,
  dataset_id,
  date,
  COUNT(*) as record_count,
  SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values,
  AVG(data_quality_score) as avg_quality,
  MAX(last_updated) as latest_update
FROM us_gov.economic_indicators
GROUP BY source_agency, dataset_id, date;

-- XBRL calculation validation
CREATE VIEW us_gov.sec_validation_errors AS
SELECT 
  accession_number,
  ticker,
  filing_date,
  ABS(reported_total - calculated_total) as variance,
  calculation_errors
FROM us_gov.sec_filing_validations
WHERE ABS(reported_total - calculated_total) > 1000;
```

### Implementation Phases

#### Phase 1: Core Financial Data (Q1)
- SEC EDGAR XBRL processing pipeline
- Normalized financial statement tables
- Basic data quality validation
- Initial 1000 companies (S&P 500 + mid-cap)

#### Phase 2: Economic Indicators (Q2)
- BLS employment and inflation integration
- Federal Reserve FRED API integration
- Time series alignment and normalization
- Geographic metadata foundation

#### Phase 3: Enhanced Coverage (Q3)
- Census demographic data
- Crime statistics integration
- Complete geographic crosswalk tables
- Expand to all public companies

#### Phase 4: Advanced Features (Q4)
- Vector embeddings for filing text
- Real-time data feeds where available
- ML-powered data quality scoring
- International data sources

### Value Proposition

#### For Financial Analysts
- Free alternative to Bloomberg/Refinitiv ($30K+ annual savings)
- Combine market data with economic indicators
- Track companies across multiple dimensions
- Historical data with time travel queries

#### For Researchers
- Unified access to all government data
- Cross-agency analysis capabilities
- Reproducible research with versioned data
- No API rate limit concerns

#### For Businesses
- Location intelligence for site selection
- Risk assessment across multiple factors
- Competitive intelligence from public filings
- Economic trend analysis

#### For Government & Policy
- Evidence-based policy analysis
- Cross-department data integration
- Public transparency and accessibility
- Standardized reporting metrics

### Technical Advantages

#### Storage & Performance
- **Iceberg Format**: Efficient columnar storage with time travel
- **Partitioning**: By date/geography for fast queries
- **Incremental Updates**: Only process new data
- **Compression**: 10-100x smaller than raw files

#### Data Quality
- **Automated Validation**: Mathematical consistency checks
- **Cross-Reference Verification**: Validate across sources
- **Anomaly Detection**: Flag unusual values
- **Lineage Tracking**: Full audit trail

#### Scalability
- **Distributed Processing**: Spark/Flink compatible
- **Cloud Native**: Works with S3/GCS/Azure
- **Concurrent Updates**: ACID transactions
- **Schema Evolution**: Add fields without downtime

### Future Enhancements

#### Near Term
- **Vector Embeddings**: Semantic search on regulatory filings
- **GraphQL API**: Alternative query interface
- **Streaming Updates**: Real-time data where available
- **Data Notebooks**: Jupyter integration with examples

#### Long Term
- **International Data**: UK, EU, Canadian government sources
- **ML Features**: Pre-computed analytical features
- **Natural Language Queries**: "Show me unemployment vs tech stock performance"
- **Automated Insights**: Anomaly detection and trend alerts

### Summary
The `us-gov` adapter would democratize access to government data by providing a unified SQL interface to dozens of agencies, enabling powerful cross-dataset analysis that currently requires expensive commercial platforms or complex custom integrations. By leveraging Iceberg's storage capabilities and Calcite's query optimization, users get institutional-quality data access with familiar SQL tools.

---

## Future Design Ideas

*This section will contain additional design proposals and architectural ideas as they are developed.*