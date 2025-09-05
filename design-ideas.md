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

## 3. Patent and Medical Data Extensions for US Government Adapter

### Adapter Renaming and Expansion Plan

The current XBRL adapter will be renamed and expanded to become the `us-business` adapter as part of a broader logical reorganization of US government data sources. This adapter will include:
- **SEC/EDGAR** (existing XBRL functionality)
- **USPTO** patent and trademark data
- **FDA** corporate drug/device approvals
- **NIH** clinical trials with corporate sponsors

This fits into the broader logical architecture where government data is organized by domain (business, economy, demographics, etc.) rather than by agency.

### Patent Data Integration (USPTO)

#### Overview
Extend the US Government adapter with comprehensive patent data from the USPTO, enabling SQL-based patent analytics, citation network analysis, and innovation tracking.

#### Data Sources
- **USPTO Open Data Portal**: REST APIs for patents and applications
- **Patent Full-Text Search API**: Complete patent document search
- **PTAB API v2**: Patent Trial and Appeal Board proceedings
- **Bulk Data Downloads**: XML/JSON patent grants and applications
- **Patent Assignment API**: Ownership and transaction records

#### Core Tables

```sql
-- Patent grants and applications
CREATE TABLE us_gov.patents (
  patent_number VARCHAR PRIMARY KEY,
  application_number VARCHAR,
  patent_type VARCHAR,           -- 'utility', 'design', 'plant', 'reissue'
  title TEXT,
  abstract TEXT,
  filing_date DATE,
  grant_date DATE,
  priority_date DATE,
  expiration_date DATE,
  status VARCHAR,                -- 'granted', 'pending', 'abandoned', 'expired'
  -- Classification
  cpc_codes ARRAY<VARCHAR>,      -- Cooperative Patent Classification
  ipc_codes ARRAY<VARCHAR>,      -- International Patent Classification
  uspc_codes ARRAY<VARCHAR>,     -- US Patent Classification (legacy)
  -- Entities
  inventor_names ARRAY<VARCHAR>,
  assignee_name VARCHAR,
  assignee_type VARCHAR,         -- 'large', 'small', 'micro', 'individual'
  examiner_name VARCHAR,
  art_unit VARCHAR,
  -- Metrics
  claim_count INTEGER,
  independent_claim_count INTEGER,
  figure_count INTEGER,
  citation_count INTEGER,
  -- Full text search
  claims_text TEXT,
  description_text TEXT,
  embedding VECTOR(1536)         -- For semantic search
);

-- Patent citations network
CREATE TABLE us_gov.patent_citations (
  citing_patent VARCHAR,
  cited_patent VARCHAR,
  citation_type VARCHAR,         -- 'applicant', 'examiner', 'third-party'
  citation_date DATE,
  citation_context TEXT,
  PRIMARY KEY (citing_patent, cited_patent)
);

-- Patent assignments and ownership changes
CREATE TABLE us_gov.patent_assignments (
  assignment_id VARCHAR PRIMARY KEY,
  patent_number VARCHAR,
  execution_date DATE,
  recording_date DATE,
  assignor VARCHAR,
  assignee VARCHAR,
  assignment_type VARCHAR,       -- 'assignment', 'merger', 'license', 'security'
  consideration TEXT,
  correspondence_address TEXT
);

-- PTAB proceedings
CREATE TABLE us_gov.ptab_proceedings (
  proceeding_number VARCHAR PRIMARY KEY,
  proceeding_type VARCHAR,       -- 'IPR', 'PGR', 'CBM'
  patent_number VARCHAR,
  petitioner VARCHAR,
  patent_owner VARCHAR,
  filing_date DATE,
  institution_date DATE,
  final_decision_date DATE,
  status VARCHAR,
  outcome VARCHAR,                -- 'instituted', 'denied', 'settled', 'terminated'
  claims_challenged ARRAY<INTEGER>,
  claims_cancelled ARRAY<INTEGER>
);

-- Patent-to-company mapping via SEC filings
CREATE TABLE us_gov.company_patents (
  cik VARCHAR,
  ticker VARCHAR,
  company_name VARCHAR,
  patent_number VARCHAR,
  ownership_type VARCHAR,        -- 'direct', 'subsidiary', 'licensed'
  acquisition_date DATE,
  acquisition_method VARCHAR,    -- 'filed', 'purchased', 'merger'
  reported_value DECIMAL,
  sec_filing_reference VARCHAR
);
```

### Medical and Healthcare Data Integration

#### Data Sources
- **CMS Provider Data Catalog**: Healthcare provider performance
- **Medicare/Medicaid Claims API**: FHIR-compliant claims data
- **NIH APIs**: Research grants, clinical trials, publications
- **CDC APIs**: Disease surveillance, vaccination rates
- **FDA APIs**: Drug approvals, adverse events, recalls

#### Core Healthcare Tables

```sql
-- Healthcare providers
CREATE TABLE us_gov.healthcare_providers (
  npi VARCHAR PRIMARY KEY,        -- National Provider Identifier
  provider_name VARCHAR,
  provider_type VARCHAR,
  specialty VARCHAR,
  -- Location
  practice_address VARCHAR,
  practice_zip VARCHAR,
  practice_state VARCHAR,
  hospital_affiliation VARCHAR,
  -- Quality metrics
  quality_score DECIMAL,
  patient_satisfaction DECIMAL,
  readmission_rate DECIMAL,
  mortality_rate DECIMAL,
  -- Volume
  medicare_patients INTEGER,
  medicaid_patients INTEGER,
  total_procedures INTEGER,
  -- Financial
  avg_medicare_payment DECIMAL,
  total_medicare_payments DECIMAL
);

-- Medicare claims summary
CREATE TABLE us_gov.medicare_claims (
  claim_id VARCHAR PRIMARY KEY,
  beneficiary_id_hash VARCHAR,   -- De-identified
  provider_npi VARCHAR,
  service_date DATE,
  -- Diagnosis and procedure
  diagnosis_codes ARRAY<VARCHAR>, -- ICD-10
  procedure_codes ARRAY<VARCHAR>, -- CPT/HCPCS
  drg_code VARCHAR,               -- Diagnosis Related Group
  -- Costs
  submitted_charge DECIMAL,
  medicare_allowed DECIMAL,
  medicare_payment DECIMAL,
  beneficiary_payment DECIMAL,
  -- Outcome
  readmission_flag BOOLEAN,
  complication_flag BOOLEAN,
  length_of_stay INTEGER
);

-- Clinical trials registry
CREATE TABLE us_gov.clinical_trials (
  nct_number VARCHAR PRIMARY KEY, -- NCT identifier
  title TEXT,
  sponsor VARCHAR,
  sponsor_type VARCHAR,           -- 'industry', 'nih', 'other'
  phase VARCHAR,                  -- 'Phase 1', 'Phase 2', 'Phase 3', 'Phase 4'
  study_type VARCHAR,             -- 'interventional', 'observational'
  status VARCHAR,                 -- 'recruiting', 'active', 'completed'
  start_date DATE,
  completion_date DATE,
  -- Conditions and interventions
  conditions ARRAY<VARCHAR>,
  interventions ARRAY<VARCHAR>,
  drug_names ARRAY<VARCHAR>,
  -- Enrollment
  enrollment_target INTEGER,
  enrollment_actual INTEGER,
  -- Locations
  study_locations ARRAY<VARCHAR>,
  -- Results
  has_results BOOLEAN,
  primary_outcome_measure TEXT,
  primary_outcome_result TEXT
);

-- Drug approvals and safety
CREATE TABLE us_gov.fda_drugs (
  application_number VARCHAR PRIMARY KEY,
  drug_name VARCHAR,
  generic_name VARCHAR,
  sponsor VARCHAR,
  approval_date DATE,
  drug_type VARCHAR,              -- 'new_molecular_entity', 'generic', 'biosimilar'
  therapeutic_area VARCHAR,
  -- Regulatory
  orphan_designation BOOLEAN,
  breakthrough_therapy BOOLEAN,
  accelerated_approval BOOLEAN,
  -- Safety
  black_box_warning BOOLEAN,
  adverse_event_count INTEGER,
  recall_count INTEGER,
  -- Market
  patent_expiration DATE,
  exclusivity_expiration DATE,
  generic_competition BOOLEAN
);

-- NIH research grants
CREATE TABLE us_gov.nih_grants (
  grant_number VARCHAR PRIMARY KEY,
  project_title TEXT,
  principal_investigator VARCHAR,
  institution VARCHAR,
  -- Funding
  fiscal_year INTEGER,
  award_amount DECIMAL,
  total_cost DECIMAL,
  -- Research area
  institute VARCHAR,              -- 'NCI', 'NIAID', 'NHLBI', etc.
  study_section VARCHAR,
  research_keywords ARRAY<VARCHAR>,
  -- Outcomes
  publication_count INTEGER,
  patent_count INTEGER,
  clinical_trial_count INTEGER,
  -- Links
  linked_patents ARRAY<VARCHAR>,
  linked_trials ARRAY<VARCHAR>,
  pubmed_ids ARRAY<VARCHAR>
);

-- Disease surveillance
CREATE TABLE us_gov.disease_surveillance (
  report_date DATE,
  disease VARCHAR,
  icd10_code VARCHAR,
  -- Geographic
  state_code VARCHAR,
  county_fips VARCHAR,
  reporting_area VARCHAR,
  -- Metrics
  case_count INTEGER,
  hospitalization_count INTEGER,
  death_count INTEGER,
  -- Rates
  incidence_rate DECIMAL,         -- Per 100,000
  hospitalization_rate DECIMAL,
  mortality_rate DECIMAL,
  -- Vaccination (if applicable)
  vaccination_coverage DECIMAL,
  breakthrough_cases INTEGER
);
```

### Cross-Domain Query Examples

#### Innovation and Healthcare Analytics
```sql
-- Find companies with both strong patent portfolios and clinical trials
SELECT
  c.ticker,
  c.company_name,
  COUNT(DISTINCT p.patent_number) as patent_count,
  COUNT(DISTINCT t.nct_number) as trial_count,
  AVG(p.citation_count) as avg_patent_citations,
  STRING_AGG(DISTINCT t.conditions[1], ', ') as research_areas
FROM us_gov.sec_companies c
JOIN us_gov.company_patents cp ON c.cik = cp.cik
JOIN us_gov.patents p ON cp.patent_number = p.patent_number
JOIN us_gov.clinical_trials t ON c.company_name = t.sponsor
WHERE p.grant_date >= CURRENT_DATE - INTERVAL '5 years'
  AND t.status IN ('recruiting', 'active')
GROUP BY c.ticker, c.company_name
HAVING COUNT(DISTINCT p.patent_number) > 10
  AND COUNT(DISTINCT t.nct_number) > 3
ORDER BY patent_count DESC;

-- Healthcare provider quality vs Medicare payments
SELECT
  p.provider_type,
  p.specialty,
  NTILE(10) OVER (ORDER BY p.quality_score) as quality_decile,
  AVG(p.total_medicare_payments) as avg_payments,
  AVG(p.readmission_rate) as avg_readmission,
  AVG(p.patient_satisfaction) as avg_satisfaction,
  COUNT(*) as provider_count
FROM us_gov.healthcare_providers p
WHERE p.medicare_patients > 100
GROUP BY p.provider_type, p.specialty, quality_decile
ORDER BY p.provider_type, quality_decile;

-- Patent citation network analysis
WITH patent_influence AS (
  SELECT
    p.patent_number,
    p.title,
    p.assignee_name,
    COUNT(c.citing_patent) as times_cited,
    AVG(DATE_DIFF('year', p.grant_date, c2.grant_date)) as avg_citation_lag
  FROM us_gov.patents p
  LEFT JOIN us_gov.patent_citations c ON p.patent_number = c.cited_patent
  LEFT JOIN us_gov.patents c2 ON c.citing_patent = c2.patent_number
  WHERE p.grant_date >= '2015-01-01'
  GROUP BY p.patent_number, p.title, p.assignee_name
)
SELECT
  assignee_name,
  COUNT(*) as patent_count,
  AVG(times_cited) as avg_citations,
  MAX(times_cited) as max_citations,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY times_cited) as p90_citations
FROM patent_influence
GROUP BY assignee_name
HAVING COUNT(*) >= 10
ORDER BY avg_citations DESC;

-- Drug development pipeline analysis
SELECT
  d.sponsor,
  COUNT(DISTINCT d.drug_name) as approved_drugs,
  COUNT(DISTINCT t.nct_number) as active_trials,
  SUM(CASE WHEN t.phase = 'Phase 3' THEN 1 ELSE 0 END) as phase3_trials,
  AVG(DATE_DIFF('year', t.start_date, d.approval_date)) as avg_development_time,
  SUM(g.award_amount) as total_nih_funding
FROM us_gov.fda_drugs d
LEFT JOIN us_gov.clinical_trials t
  ON d.sponsor = t.sponsor
  AND t.drug_names @> ARRAY[d.drug_name]
LEFT JOIN us_gov.nih_grants g
  ON t.nct_number = ANY(g.linked_trials)
WHERE d.approval_date >= '2020-01-01'
GROUP BY d.sponsor
ORDER BY approved_drugs DESC;

-- Geographic disease patterns vs healthcare access
SELECT
  z.state_name,
  z.county_name,
  z.population,
  -- Disease burden
  SUM(ds.case_count) as total_cases,
  SUM(ds.death_count) as total_deaths,
  -- Healthcare access
  COUNT(DISTINCT hp.npi) as provider_count,
  z.population / NULLIF(COUNT(DISTINCT hp.npi), 0) as people_per_provider,
  AVG(hp.quality_score) as avg_provider_quality,
  -- Economic factors
  e.unemployment_rate,
  e.median_household_income
FROM us_gov.zip_codes z
JOIN us_gov.disease_surveillance ds ON z.county_fips = ds.county_fips
LEFT JOIN us_gov.healthcare_providers hp ON z.zip_code = hp.practice_zip
LEFT JOIN us_gov.economic_indicators e ON z.county_fips = e.geography_code
WHERE ds.report_date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY z.state_name, z.county_name, z.population,
         e.unemployment_rate, e.median_household_income
HAVING z.population > 10000
ORDER BY total_deaths DESC;
```

### Implementation Details

#### Patent Data Pipeline
```java
public class PatentDataFetcher {
  // Rate limiting: 30 requests per minute for USPTO
  private static final RateLimiter RATE_LIMITER = RateLimiter.create(0.5);

  public void fetchPatents() {
    // 1. Bulk download weekly patent grants (XML)
    downloadBulkPatentGrants();

    // 2. Parse XML to extract structured data
    parsePatentXML();

    // 3. Fetch citations via API
    fetchCitationNetwork();

    // 4. Generate embeddings for semantic search
    generatePatentEmbeddings();

    // 5. Write to Iceberg tables
    writeToIceberg();
  }
}
```

#### Healthcare Data Integration
```java
public class HealthcareDataManager {
  // FHIR client for CMS data
  private final FhirContext fhirContext = FhirContext.forR4();

  public void syncHealthcareData() {
    // 1. Fetch provider data from CMS
    fetchProviderData();

    // 2. Process FHIR-compliant claims
    processFhirClaims();

    // 3. Sync clinical trials from ClinicalTrials.gov
    syncClinicalTrials();

    // 4. Update FDA drug approvals
    updateFdaDrugData();

    // 5. Calculate quality metrics
    calculateQualityMetrics();
  }
}
```

### Configuration
```json
{
  "patents": {
    "enabled": true,
    "apiKey": "${USPTO_API_KEY}",
    "bulkDownload": true,
    "updateFrequency": "weekly",
    "historicalYears": 20,
    "generateEmbeddings": true
  },
  "healthcare": {
    "cms": {
      "enabled": true,
      "apiKey": "${CMS_API_KEY}",
      "datasets": ["providers", "claims", "quality"]
    },
    "nih": {
      "enabled": true,
      "includeGrants": true,
      "includeClinicalTrials": true
    },
    "fda": {
      "enabled": true,
      "datasets": ["drugs", "devices", "adverse_events"]
    },
    "cdc": {
      "enabled": true,
      "surveillanceData": true,
      "vaccinationData": true
    }
  }
}
```

### Value Proposition

#### For Pharmaceutical Companies
- Track competitor drug pipelines and patent expirations
- Identify partnership opportunities via grant/trial analysis
- Monitor adverse events and safety signals
- Analyze healthcare provider prescribing patterns

#### For Healthcare Systems
- Benchmark provider performance against national metrics
- Identify quality improvement opportunities
- Track disease patterns and prepare for outbreaks
- Optimize resource allocation based on community needs

#### For Investors
- Analyze biotech company pipelines and patent portfolios
- Track FDA approval patterns and success rates
- Identify emerging therapeutic areas via grant funding
- Assess market opportunities via epidemiological data

#### For Researchers
- Link research grants to publications and patents
- Track clinical trial outcomes and success rates
- Analyze disease patterns across demographics
- Identify collaboration opportunities

### Summary
These extensions transform the US Government adapter into a comprehensive platform for innovation and healthcare analytics, enabling unique insights at the intersection of intellectual property, medical research, and public health data.

## 4. US Government Data Adapters - Logical Architecture

### Overview
Rather than organizing adapters by government agency (SEC, USPTO, CDC, etc.), we organize them by logical domain to match how users think about data. This creates intuitive schemas that enable natural cross-domain analysis.

### Logical Adapter Organization

#### economy Adapter
Aggregates economic indicators and financial metrics from multiple agencies:
- **Federal Reserve (FRED)**: 800,000+ economic time series including inflation, interest rates, GDP
- **Bureau of Labor Statistics (BLS)**: Employment, unemployment, wages, productivity, CPI
- **Treasury Department**: Treasury yields, auction results, debt statistics
- **Bureau of Economic Analysis (BEA)**: GDP components, trade balance, personal income

```sql
-- Natural economic queries (each source is its own schema)
SELECT * FROM fed.inflation_cpi WHERE date >= '2020-01-01';
SELECT * FROM bls.unemployment WHERE state = 'CA';
SELECT * FROM treasury.yields WHERE maturity = '10Y';
SELECT * FROM bls.wages WHERE occupation LIKE '%Software%';
```

#### business Adapter (formerly XBRL)
Corporate and innovation data unified across agencies:
- **SEC/EDGAR**: Financial statements, XBRL data, insider trading
- **USPTO**: Patents, trademarks, assignments
- **FDA**: Drug approvals, device clearances (corporate side)
- **NIH ClinicalTrials.gov**: Industry-sponsored trials

```sql
-- Cross-agency business intelligence (each source is its own schema)
SELECT
  c.ticker,
  c.market_cap,
  COUNT(p.patent_number) as patent_count,
  COUNT(t.nct_number) as active_trials
FROM sec.companies c
LEFT JOIN uspto.patents p ON c.name = p.assignee
LEFT JOIN clinical_trials.trials t ON c.name = t.sponsor
GROUP BY c.ticker, c.market_cap;
```

#### demographics Adapter
Population, geography, and socioeconomic data:
- **Census Bureau**: Population, housing, American Community Survey
- **IRS Statistics**: Income by ZIP, tax statistics
- **USPS**: ZIP codes, address validation
- **Department of Education**: School districts, education statistics

```sql
-- Regional demographic analysis
SELECT
  z.city,
  z.state,
  d.population,
  d.median_income,
  d.education_bachelor_pct
FROM demographics.zip_codes z
JOIN demographics.census_data d ON z.zcta = d.zcta
WHERE z.state IN ('CA', 'NY', 'TX');
```

#### government Adapter
Officials and regulatory data:
- **ProPublica Congress API**: Federal legislators, votes, bills
- **FEC**: Campaign finance, political contributions
- **USA.gov**: Federal agencies, programs
- **OpenStates**: State legislators and bills
- **Google Civic Information API**: Local officials

```sql
-- Political landscape analysis
SELECT
  g.state,
  g.party,
  COUNT(*) as representatives,
  AVG(f.total_receipts) as avg_campaign_funds
FROM government.congress g
JOIN government.campaign_finance f ON g.fec_id = f.candidate_id
GROUP BY g.state, g.party;
```

#### healthcare Adapter
Medical and public health data:
- **CMS**: Medicare/Medicaid providers, claims, quality ratings
- **CDC**: Disease statistics, mortality data, vaccination rates
- **NIH**: Research grants, PubMed articles
- **FDA**: Drug/device approvals (medical side), adverse events

```sql
-- Healthcare access analysis
SELECT
  h.county,
  COUNT(DISTINCT p.npi) as provider_count,
  AVG(p.quality_rating) as avg_quality,
  c.covid_vaccination_rate
FROM healthcare.providers p
JOIN healthcare.counties h ON p.county_fips = h.fips
JOIN healthcare.cdc_data c ON h.fips = c.fips
GROUP BY h.county, c.covid_vaccination_rate;
```

### Implementation Architecture

```
calcite/
├── us-economy/       # Fed, BLS, Treasury, BEA
├── us-business/      # SEC, USPTO, FDA (corporate)
├── us-demographics/  # Census, IRS, USPS
├── us-government/    # ProPublica, FEC, USA.gov
├── us-healthcare/    # CMS, NIH, CDC, FDA (medical)
└── us-gov-bundle/    # JDBC driver bundling all adapters
```

### JDBC Driver Multi-Schema Model

Each logical adapter contains multiple schema factories for its sub-domains. The `us-gov-bundle` JDBC driver creates a unified connection with all schemas:

```java
public class UsGovDriver extends Driver {
  private String createModel() {
    return "{"
      + "schemas: ["
      // Economy adapter schemas
      + "  {name: 'fed', factory: 'org.apache.calcite.adapter.economy.fed.FedSchemaFactory'},"
      + "  {name: 'bls', factory: 'org.apache.calcite.adapter.economy.bls.BlsSchemaFactory'},"
      + "  {name: 'treasury', factory: 'org.apache.calcite.adapter.economy.treasury.TreasurySchemaFactory'},"
      // Business adapter schemas
      + "  {name: 'sec', factory: 'org.apache.calcite.adapter.business.sec.SecSchemaFactory'},"
      + "  {name: 'uspto', factory: 'org.apache.calcite.adapter.business.uspto.UsptoSchemaFactory'},"
      + "  {name: 'fda', factory: 'org.apache.calcite.adapter.business.fda.FdaSchemaFactory'},"
      + "  {name: 'clinical_trials', factory: 'org.apache.calcite.adapter.business.clinical.ClinicalSchemaFactory'},"
      // Demographics adapter schemas
      + "  {name: 'census', factory: 'org.apache.calcite.adapter.demographics.census.CensusSchemaFactory'},"
      + "  {name: 'irs', factory: 'org.apache.calcite.adapter.demographics.irs.IrsSchemaFactory'},"
      // Government adapter schemas
      + "  {name: 'congress', factory: 'org.apache.calcite.adapter.government.congress.CongressSchemaFactory'},"
      + "  {name: 'fec', factory: 'org.apache.calcite.adapter.government.fec.FecSchemaFactory'},"
      // Healthcare adapter schemas
      + "  {name: 'cms', factory: 'org.apache.calcite.adapter.healthcare.cms.CmsSchemaFactory'},"
      + "  {name: 'cdc', factory: 'org.apache.calcite.adapter.healthcare.cdc.CdcSchemaFactory'},"
      + "  {name: 'nih', factory: 'org.apache.calcite.adapter.healthcare.nih.NihSchemaFactory'}"
      + "]}";
  }
}
```

This approach provides:
- **Clean separation**: Each data source gets its own schema
- **Code reuse**: Shared infrastructure within each adapter module
- **Natural namespacing**: `sec.companies`, `uspto.patents`, `fed.interest_rates`
- **Flexible configuration**: Enable/disable individual schemas as needed

### Cross-Domain Query Examples

#### Economic Impact on Business Performance
```sql
SELECT
  e.quarter,
  e.gdp_growth,
  e.inflation_rate,
  e.unemployment_rate,
  AVG(b.revenue_growth) as avg_sp500_revenue_growth,
  AVG(b.profit_margin) as avg_sp500_profit_margin
FROM economy.indicators e
JOIN business.sp500_financials b ON e.quarter = b.fiscal_quarter
WHERE e.date >= '2020-01-01'
GROUP BY e.quarter, e.gdp_growth, e.inflation_rate, e.unemployment_rate
ORDER BY e.quarter;
```

#### Innovation Clusters by Region
```sql
SELECT
  d.msa_name as metro_area,
  d.population,
  d.median_income,
  COUNT(DISTINCT p.assignee) as unique_innovators,
  COUNT(p.patent_number) as total_patents,
  COUNT(DISTINCT p.patent_number) FILTER (WHERE p.cpc_codes && ARRAY['G06N']) as ai_patents,
  COUNT(DISTINCT c.cik) as public_companies
FROM demographics.metro_areas d
LEFT JOIN business.patents p ON d.msa_code = p.inventor_msa
LEFT JOIN business.companies c ON d.msa_code = c.hq_msa
WHERE d.population > 1000000
GROUP BY d.msa_name, d.population, d.median_income
ORDER BY total_patents DESC;
```

#### Healthcare Disparities Analysis
```sql
SELECT
  d.income_quintile,
  d.racial_majority,
  AVG(h.provider_density) as providers_per_10k,
  AVG(h.hospital_quality_score) as avg_hospital_quality,
  AVG(c.life_expectancy) as avg_life_expectancy,
  AVG(c.preventable_death_rate) as preventable_deaths_per_100k
FROM demographics.counties d
JOIN healthcare.provider_metrics h ON d.fips = h.county_fips
JOIN healthcare.health_outcomes c ON d.fips = c.fips
GROUP BY d.income_quintile, d.racial_majority
ORDER BY d.income_quintile;
```

#### Political Influence on Business Regulation
```sql
SELECT
  g.committee_name,
  g.chair_party,
  COUNT(DISTINCT b.bill_id) as bills_introduced,
  COUNT(DISTINCT b.bill_id) FILTER (WHERE b.became_law) as bills_enacted,
  AVG(s.stock_return) as avg_affected_sector_return
FROM government.committees g
JOIN government.bills b ON g.committee_id = b.committee_id
JOIN business.sector_returns s ON b.affected_industries && s.industries
WHERE b.topic IN ('Financial Regulation', 'Healthcare', 'Technology')
  AND b.congress >= 116
GROUP BY g.committee_name, g.chair_party;
```

### Technical Implementation Details

#### Shared Infrastructure
While each adapter is logically separate, they share common patterns:
- **Parquet Storage**: All adapters convert data to Parquet for the file adapter's caching
- **Identifier Normalization**: Consistent formatting (CIK padding, FIPS codes, etc.)
- **Rate Limiting**: Respecting API limits (SEC: 10/sec, USPTO: 4/sec, etc.)
- **Background Refresh**: Scheduled updates for time-series data

#### Entity Resolution
Calcite handles cross-schema joins naturally, but we ensure consistent identifiers:
- Company: CIK (zero-padded), DUNS, EIN
- Geographic: FIPS codes, ZIP/ZCTA, MSA codes
- Healthcare: NPI, Medicare Provider ID
- People: Standardized name formats

#### Configuration Example
```json
{
  "jdbc:url": "jdbc:usgov:",
  "schemas": {
    "economy": {
      "enabled": true,
      "sources": ["fred", "bls"],
      "cache_ttl": "1h"
    },
    "business": {
      "enabled": true,
      "sec_api_key": "${SEC_KEY}",
      "uspto_api_key": "${USPTO_KEY}"
    }
  }
}
```

### Benefits of Logical Organization

1. **Intuitive Queries**: Users think "economy" not "Federal Reserve"
2. **Natural Joins**: Related data is co-located in logical schemas
3. **Simplified Discovery**: Finding data doesn't require knowing agencies
4. **Better Performance**: Related data can share caching strategies
5. **Cleaner Code**: Each adapter has a focused domain model

### Migration Path

1. Rename `xbrl` adapter to `us-business`
2. Extract SEC-specific code to subpackage
3. Add USPTO, FDA, clinical trials to business adapter
4. Create new economy adapter with Fed/BLS data
5. Build demographics adapter with Census/IRS
6. Package all in us-gov-bundle JDBC driver

This architecture provides a powerful, intuitive interface to US government data while maintaining clean separation of concerns and leveraging Calcite's native multi-schema capabilities.

## Future Design Ideas

*This section will contain additional design proposals and architectural ideas as they are developed.*
