# Government Data Schema Relationships

## Schema Overview

The Government Data adapter provides three distinct schemas:

| Schema | Domain | Description | Primary Tables |
|--------|--------|-------------|----------------|
| **SEC** | Financial Data | SEC filings, XBRL data, stock prices | filing_metadata, financial_line_items, stock_prices |
| **ECON** | Economic Data | BLS, Treasury, FRED, BEA, World Bank data | employment_statistics, inflation_metrics, treasury_yields, fred_indicators |
| **GEO** | Geographic Data | Census boundaries, HUD ZIP mappings | tiger_states, tiger_counties, hud_zip_* tables |

### Cross-Schema Foreign Key Constraints (Implemented)

**Geographic Relationships (Format-Compatible FKs)**:
- **SEC → GEO**: `filing_metadata.state_of_incorporation` → `tiger_states.state_code` (2-letter state codes)
- **ECON → GEO**: `regional_employment.state_code` → `tiger_states.state_code` (2-letter state codes)
- **ECON → GEO**: `regional_income.geo_fips` → `tiger_states.state_fips` (partial - state-level FIPS only)
- **ECON → GEO**: `state_gdp.geo_fips` → `tiger_states.state_fips` (state-level FIPS codes)

**Within-Schema Relationships**:
- **ECON → ECON**: Series overlap between data sources (metadata-only, not enforced)
- **GEO → GEO**: Geographic hierarchy FKs (counties → states, places → states)

**Temporal Relationships (NOT Foreign Keys)**:
- **SEC ↔ ECON**: Economic indicators and stock prices (use temporal joins in queries)
- Different reporting cycles prevent direct FK constraints
- Handled through application-level temporal correlation queries

## Entity Relationship Diagram

```mermaid
erDiagram
    %% ===========================
    %% SEC SCHEMA (Financial Data)
    %% ===========================
    
    %% SEC.financial_line_items
    financial_line_items {
        string cik PK
        string filing_type PK
        int year PK
        string accession_number PK
        string concept PK
        string period PK
        string context_ref FK
        decimal value
        string unit
        string segment
        date filing_date
    }
    
    %% SEC.filing_contexts
    filing_contexts {
        string cik PK
        string filing_type PK
        int year PK
        string accession_number PK
        string context_id PK
        date start_date
        date end_date
        boolean instant
        string segment
        date filing_date
    }
    
    %% SEC.filing_metadata (central reference table)
    filing_metadata {
        string cik PK
        string filing_type PK
        int year PK
        string accession_number PK
        string company_name
        string sic_code
        string state_of_incorporation FK
        string fiscal_year_end
        string business_address
        string business_phone
        date filing_date
    }
    
    %% SEC.mda_sections
    mda_sections {
        string cik PK,FK
        string filing_type PK,FK
        int year PK,FK
        string accession_number PK,FK
        string section_id PK
        string section_text
        int section_order
        date filing_date
    }
    
    %% SEC.footnotes
    footnotes {
        string cik PK,FK
        string filing_type PK,FK
        int year PK,FK
        string accession_number PK,FK
        string footnote_id PK
        string footnote_text
        string referenced_concept
        int footnote_number
        date filing_date
    }
    
    %% SEC.xbrl_relationships
    xbrl_relationships {
        string cik PK
        string filing_type PK
        int year PK
        string accession_number PK
        string relationship_id PK
        string from_concept
        string to_concept
        string arc_role
        date filing_date
    }
    
    %% SEC.insider_transactions
    insider_transactions {
        string cik PK
        string filing_type PK
        int year PK
        string accession_number PK
        string transaction_id PK
        string insider_cik
        string insider_name
        date transaction_date
        string transaction_type
        decimal shares
        decimal price_per_share
        date filing_date
    }
    
    %% SEC.earnings_transcripts
    earnings_transcripts {
        string cik PK,FK
        string filing_type PK,FK
        int year PK,FK
        string accession_number PK,FK
        string speaker
        string text
        int sequence
        date filing_date
    }
    
    %% SEC.stock_prices
    stock_prices {
        string ticker PK
        date date PK
        string cik
        decimal open
        decimal high
        decimal low
        decimal close
        bigint volume
        decimal adjusted_close
    }
    
    %% SEC.vectorized_blobs
    vectorized_blobs {
        string cik PK,FK
        string filing_type PK,FK
        int year PK,FK
        string accession_number PK,FK
        string blob_id PK
        string blob_type
        string source_table
        string source_id FK
        string content
        array embedding
        int start_offset
        int end_offset
        date filing_date
    }
    
    %% =============================
    %% ECON SCHEMA (Economic Data)
    %% =============================
    
    %% ECON.employment_statistics
    employment_statistics {
        date date PK
        string series_id PK,FK
        string series_name
        decimal value
        string unit
        boolean seasonally_adjusted
        decimal percent_change_month
        decimal percent_change_year
        string category
        string subcategory
    }
    
    %% ECON.inflation_metrics
    inflation_metrics {
        date date PK
        string index_type PK
        string item_code PK
        string area_code PK,FK
        string item_name
        decimal index_value
        decimal percent_change_month
        decimal percent_change_year
        string area_name
        boolean seasonally_adjusted
    }
    
    %% ECON.wage_growth
    wage_growth {
        date date PK
        string series_id PK
        string industry_code PK
        string occupation_code PK
        string industry_name
        string occupation_name
        decimal average_hourly_earnings
        decimal average_weekly_earnings
        decimal employment_cost_index
        decimal percent_change_year
    }
    
    %% ECON.regional_employment
    regional_employment {
        date date PK
        string area_code PK
        string area_name
        string area_type
        string state_code FK
        decimal unemployment_rate
        bigint employment_level
        bigint labor_force
        decimal participation_rate
        decimal employment_population_ratio
    }
    
    %% ECON.treasury_yields
    treasury_yields {
        date date PK
        int maturity_months PK
        decimal yield_rate
        string treasury_type
        decimal bid_to_cover_ratio
        decimal high_yield
        decimal low_yield
        bigint accepted_amount
        bigint tendered_amount
    }
    
    %% ECON.federal_debt
    federal_debt {
        date date PK
        string debt_type PK
        decimal debt_amount
        decimal interest_rate
        string debt_category
        decimal percent_of_total
        decimal year_over_year_change
    }
    
    %% ECON.world_indicators
    world_indicators {
        string country_code PK
        string indicator_code PK
        int year PK
        string country_name
        string indicator_name
        decimal value
        string unit
        string region
        string income_group
    }
    
    %% ECON.fred_indicators
    fred_indicators {
        string series_id PK
        date date PK
        string series_title
        decimal value
        string unit
        string frequency
        boolean seasonally_adjusted
        date last_updated
    }
    
    %% ECON.gdp_components
    gdp_components {
        string table_id PK,FK
        int line_number PK
        int year PK
        string component_name
        decimal value
        string unit
        decimal percent_change
        decimal contribution_to_growth
        string data_source
    }
    
    %% ECON.regional_income
    regional_income {
        string geo_fips PK,FK
        string metric PK
        int year PK
        string geo_name
        decimal value
        string unit
        string geo_type
        decimal per_capita_value
        decimal percent_change_year
    }
    
    %% ECON.state_gdp
    state_gdp {
        string geo_fips PK,FK
        string line_code PK
        int year PK
        string geo_name
        string line_description
        decimal value
        string units
    }
    
    %% =============================
    %% GEO SCHEMA (Geographic Data)
    %% =============================
    
    %% GEO.tiger_states (bridges FIPS and 2-letter codes)
    tiger_states {
        string state_fips PK
        string state_code UK
        string state_name
        geometry boundary
        decimal land_area
        decimal water_area
    }
    
    %% GEO.tiger_counties
    tiger_counties {
        string county_fips PK
        string state_fips FK
        string county_name
        geometry boundary
        decimal land_area
        decimal water_area
    }
    
    %% GEO.census_places
    census_places {
        string place_code PK
        string state_code PK,FK
        string place_name
        int population
        decimal median_income
        geometry boundary
    }
    
    %% GEO.hud_zip_county
    hud_zip_county {
        string zip PK
        string state_fips FK
        string county_fips FK
        decimal res_ratio
        decimal bus_ratio
        decimal oth_ratio
        decimal tot_ratio
    }
    
    %% GEO.hud_zip_tract
    hud_zip_tract {
        string zip PK
        string state_fips FK
        string tract PK
        string county_fips FK
        decimal res_ratio
        decimal bus_ratio
        decimal oth_ratio
        decimal tot_ratio
    }
    
    %% GEO.hud_zip_cbsa
    hud_zip_cbsa {
        string zip PK
        string state_fips FK
        string cbsa_code PK
        string cbsa_title
        decimal res_ratio
        decimal bus_ratio
        decimal oth_ratio
        decimal tot_ratio
    }
    
    %% Relationships within SEC domain
    financial_line_items ||--o{ filing_contexts : "has context"
    financial_line_items }o--|| filing_metadata : "belongs to filing"
    mda_sections }o--|| filing_metadata : "extracted from filing"
    footnotes }o--|| filing_metadata : "part of filing"
    footnotes }o--|| financial_line_items : "explains line item"
    footnotes }o--|| xbrl_relationships : "references relationships"
    earnings_transcripts }o--|| filing_metadata : "from 8-K filing"
    insider_transactions }o--|| filing_metadata : "reported in filing"
    
    %% Vectorized blobs relationships (contains embeddings of text content)
    vectorized_blobs }o--|| filing_metadata : "text from filing"
    vectorized_blobs }o--|| mda_sections : "vectorizes MD&A text"
    vectorized_blobs }o--|| footnotes : "vectorizes footnote text"
    vectorized_blobs }o--|| earnings_transcripts : "vectorizes transcript text"
    
    %% Relationships within ECON domain
    employment_statistics }o--|| fred_indicators : "series_id → series_id (BLS to FRED overlap)"
    inflation_metrics }o--|| regional_employment : "area_code → area_code (geographic overlap)"
    gdp_components }o--|| fred_indicators : "table_id → series_id (GDP series temporal)"
    
    %% Relationships within GEO domain
    tiger_counties }o--|| tiger_states : "belongs to"
    census_places }o--|| tiger_states : "located in"
    hud_zip_county }o--|| tiger_counties : "maps to"
    hud_zip_county }o--|| tiger_states : "zip in state"
    hud_zip_tract }o--|| tiger_counties : "within"
    hud_zip_tract }o--|| tiger_states : "zip in state"
    hud_zip_cbsa }o--|| tiger_states : "zip in state"
    
    %% Cross-schema relationships (using tiger_states.state_code)
    filing_metadata }o--|| tiger_states : "state_of_incorporation → state_code"
    regional_employment }o--|| tiger_states : "state_code → state_code"
    regional_income }o--|| tiger_states : "geo_fips → state_fips (2-digit state FIPS)"
    state_gdp }o--|| tiger_states : "geo_fips → state_fips"
    
    %% Other cross-schema relationships
    stock_prices }o--|| filing_metadata : "belongs to company"
    inflation_metrics }o--o{ tiger_counties : "regional inflation"
    
    %% Cross-domain relationships (SEC to ECON)
    financial_line_items ||--o{ employment_statistics : "correlates with economy"
    stock_prices ||--o{ inflation_metrics : "affected by inflation"
```

## Cross-Schema Relationships

### State Code Bridge Solution
The **GEO.tiger_states** table serves as the bridge between different state code formats:
- `state_fips`: FIPS codes (e.g., "06" for California) - used internally in GEO schema
- `state_code`: 2-letter codes (e.g., "CA") - enables FKs from SEC and ECON schemas
- Both columns exist in the same table, providing the mapping

This allows true referential integrity without requiring data transformation.

### Foreign Key Implementation Status

#### Implemented Cross-Schema FKs (in GovDataSchemaFactory)
1. **filing_metadata.state_of_incorporation → tiger_states.state_code**
   - Format: 2-letter state codes (e.g., "CA", "TX")
   - Implementation: `defineCrossDomainConstraintsForSec()`
   
2. **regional_employment.state_code → tiger_states.state_code**
   - Format: 2-letter state codes
   - Implementation: `defineCrossDomainConstraintsForEcon()`
   
3. **regional_income.geo_fips → tiger_states.state_fips** 
   - Format: FIPS codes (partial - state-level only)
   - Note: Only works for 2-digit state FIPS, not 5-digit county FIPS
   - Implementation: `defineCrossDomainConstraintsForEcon()`

4. **state_gdp.geo_fips → tiger_states.state_fips**
   - Format: FIPS codes (state-level, e.g., "06" for California)
   - Implementation: `defineCrossDomainConstraintsForEcon()`

#### Implemented Within-Schema FKs

**SEC Schema Internal FKs (to filing_metadata)**:
1. **financial_line_items** → **filing_metadata** (cik, filing_type, year, accession_number)
2. **footnotes** → **filing_metadata** (cik, filing_type, year, accession_number)
3. **insider_transactions** → **filing_metadata** (cik, filing_type, year, accession_number)
4. **earnings_transcripts** → **filing_metadata** (cik, filing_type, year, accession_number)
5. **vectorized_blobs** → **filing_metadata** (cik, filing_type, year, accession_number)
6. **mda_sections** → **filing_metadata** (cik, filing_type, year, accession_number)
7. **xbrl_relationships** → **filing_metadata** (cik, filing_type, year, accession_number)
8. **filing_contexts** → **filing_metadata** (cik, filing_type, year, accession_number)

**ECON Schema Internal FKs**:
1. **employment_statistics.series_id** → **fred_indicators.series_id** (partial overlap)
2. **inflation_metrics.area_code** → **regional_employment.area_code** (geographic overlap)
3. **gdp_components.table_id** → **fred_indicators.series_id** (GDP series temporal)

**GEO Schema Internal FKs (Geographic Hierarchy)**:
1. **tiger_counties.state_fips** → **tiger_states.state_fips**
2. **census_places.state_code** → **tiger_states.state_code**
3. **hud_zip_county.county_fips** → **tiger_counties.county_fips**
4. **hud_zip_county.state_fips** → **tiger_states.state_fips**
5. **hud_zip_tract.county_fips** → **tiger_counties.county_fips**
6. **hud_zip_tract.state_fips** → **tiger_states.state_fips**
7. **hud_zip_cbsa.state_fips** → **tiger_states.state_fips**
8. **tiger_census_tracts.county_fips** → **tiger_counties.county_fips**
9. **tiger_block_groups.tract_code** → **tiger_census_tracts.tract_code**
10. **hud_zip_cbsa_div.cbsa** → **tiger_cbsa.cbsa_code**
11. **hud_zip_congressional.state_code** → **tiger_states.state_code**

### Conceptual Relationships (Require data extraction/parsing)

1. **filing_metadata.business_address → hud_zip_county.zip**
   - Business addresses contain ZIP codes
   - Would require parsing address field to extract ZIP
   - Enables geographic analysis of company headquarters

2. **insider_transactions → census_places**
   - If insider addresses were captured, could link to cities
   - Enables analysis of insider trading patterns by geography

3. **stock_prices.ticker → geographic market data**
   - Stock exchanges have geographic locations
   - Could analyze trading patterns by exchange location

## Table Categories

### SEC Tables (Partitioned by cik/filing_type/year)
- **financial_line_items**: XBRL financial statement data
- **filing_contexts**: XBRL context definitions
- **filing_metadata**: Company and filing information (central reference table)
- **mda_sections**: Management Discussion & Analysis text (FK to filing_metadata)
- **footnotes**: Financial statement footnotes (FK to filing_metadata)
- **xbrl_relationships**: Concept relationships and calculations
- **insider_transactions**: Forms 3, 4, 5 insider trading data (FK to filing_metadata)
- **earnings_transcripts**: 8-K earnings call transcripts (FK to filing_metadata)
- **stock_prices**: Daily stock price data (partitioned by ticker/year)
- **vectorized_blobs**: Text embeddings for semantic search (vectorizes content from mda_sections, footnotes, and earnings_transcripts)

### GEO Tables (Static or slowly changing)
- **tiger_states**: State boundaries and metadata
- **tiger_counties**: County boundaries and metadata
- **census_places**: City/town data with demographics
- **hud_zip_county**: ZIP to county mapping
- **hud_zip_tract**: ZIP to census tract mapping
- **hud_zip_cbsa**: ZIP to metro area mapping

### ECON Tables (Multi-source - Partitioned by date/series)
- **employment_statistics**: BLS national employment and unemployment data
- **inflation_metrics**: BLS CPI and PPI inflation indicators
- **wage_growth**: BLS earnings by industry and occupation
- **regional_employment**: BLS state and metro area employment statistics
- **treasury_yields**: Treasury Direct yield curves and auction data
- **federal_debt**: Treasury Direct federal debt statistics and interest rates
- **world_indicators**: World Bank international economic indicators (GDP, inflation, unemployment)
- **fred_indicators**: Federal Reserve economic time series data (800K+ indicators)
- **gdp_components**: BEA GDP components and detailed economic accounts
- **regional_income**: BEA state and regional personal income statistics
- **state_gdp**: BEA state-level GDP statistics by NAICS industry (total and per capita)

## Query Examples Using Cross-Schema Relationships

```sql
-- Companies incorporated in California with their stock performance
-- Joins across SEC and GEO schemas
SELECT 
    m.company_name,
    m.state_of_incorporation,
    s.state_name,
    AVG(p.close) as avg_stock_price
FROM sec.filing_metadata m
JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code
JOIN sec.stock_prices p ON m.cik = p.cik
WHERE s.state_name = 'California'
GROUP BY m.company_name, m.state_of_incorporation, s.state_name;

-- Financial performance by state of incorporation
SELECT 
    s.state_name,
    COUNT(DISTINCT f.cik) as company_count,
    AVG(f.value) as avg_net_income
FROM financial_line_items f
JOIN filing_metadata m ON f.cik = m.cik 
    AND f.filing_type = m.filing_type 
    AND f.year = m.year
JOIN tiger_states s ON m.state_of_incorporation = s.state_code
WHERE f.concept = 'NetIncomeLoss'
GROUP BY s.state_name
ORDER BY avg_net_income DESC;

-- Geographic concentration of tech companies (using SIC codes)
SELECT 
    s.state_name,
    COUNT(DISTINCT m.cik) as tech_company_count
FROM filing_metadata m
JOIN tiger_states s ON m.state_of_incorporation = s.state_code
WHERE m.sic_code BETWEEN '7370' AND '7379' -- Computer services
GROUP BY s.state_name
ORDER BY tech_company_count DESC;

-- Company performance vs. economic indicators (SEC + ECON)
SELECT 
    f.year,
    f.cik,
    m.company_name,
    f.value as revenue,
    e.value as unemployment_rate,
    i.percent_change_year as inflation_rate,
    LAG(f.value) OVER (PARTITION BY f.cik ORDER BY f.year) as prev_revenue,
    (f.value - LAG(f.value) OVER (PARTITION BY f.cik ORDER BY f.year)) / 
        LAG(f.value) OVER (PARTITION BY f.cik ORDER BY f.year) * 100 as revenue_growth
FROM financial_line_items f
JOIN filing_metadata m ON f.cik = m.cik 
    AND f.filing_type = m.filing_type 
    AND f.year = m.year
JOIN employment_statistics e ON YEAR(e.date) = f.year 
    AND e.series_id = 'UNRATE'
    AND MONTH(e.date) = 12  -- December data
JOIN inflation_metrics i ON YEAR(i.date) = f.year
    AND i.index_type = 'CPI-U'
    AND i.item_code = 'All Items'
    AND MONTH(i.date) = 12
WHERE f.concept = 'Revenues'
    AND f.filing_type = '10-K'
ORDER BY f.year, revenue_growth DESC;

-- Regional employment impact on local companies (ECON + GEO + SEC)
SELECT 
    re.state_code,
    s.state_name,
    re.unemployment_rate,
    re.employment_level,
    COUNT(DISTINCT m.cik) as company_count,
    AVG(sp.close) as avg_stock_price
FROM regional_employment re
JOIN tiger_states s ON re.state_code = s.state_code
JOIN filing_metadata m ON m.state_of_incorporation = s.state_code
LEFT JOIN stock_prices sp ON m.cik = sp.cik 
    AND YEAR(sp.trade_date) = YEAR(re.date)
WHERE re.area_type = 'state'
    AND re.date = (SELECT MAX(date) FROM regional_employment)
GROUP BY re.state_code, s.state_name, re.unemployment_rate, re.employment_level
ORDER BY re.unemployment_rate ASC;

-- Wage growth vs. company compensation expenses (ECON + SEC)
SELECT 
    w.industry_name,
    AVG(w.average_hourly_earnings) as avg_hourly_wage,
    AVG(w.percent_change_year) as wage_growth_rate,
    COUNT(DISTINCT f.cik) as companies_in_industry,
    AVG(f.value) as avg_compensation_expense
FROM wage_growth w
JOIN filing_metadata m ON m.sic_code BETWEEN '2000' AND '3999'  -- Manufacturing
JOIN financial_line_items f ON f.cik = m.cik
    AND f.concept = 'CompensationCosts'
    AND YEAR(w.date) = f.year
WHERE w.industry_code LIKE '31-33%'  -- Manufacturing NAICS
GROUP BY w.industry_name
ORDER BY wage_growth_rate DESC;
```

## Implementation Notes

### Primary Keys
- All SEC tables use composite PKs including partition columns (cik, filing_type, year)
- GEO tables use natural keys (FIPS codes, ZIP codes, etc.)
- ECON tables use composite PKs of date + series/area identifiers

### Foreign Keys
- Within-domain FKs are strongly typed and enforced via metadata
- Cross-domain FKs require data transformation or parsing
- State codes need standardization (2-letter vs FIPS)
- ECON area codes can map to both FIPS (counties) and MSA codes

### Data Freshness
- SEC data is continuously updated via RSS feeds
- GEO data is updated annually (Census/TIGER releases)
- Stock prices updated daily
- ECON data updated monthly (BLS releases on fixed schedule)
  - Employment data: First Friday of each month
  - CPI: Mid-month for prior month
  - PPI: Mid-month for prior month

### Temporal Relationships (Why Not Foreign Keys)

Temporal relationships between schemas are NOT implemented as foreign key constraints because:

1. **Different Reporting Cycles**:
   - SEC filings: Quarterly (10-Q) and Annual (10-K)
   - Economic data: Daily, Weekly, Monthly, or Quarterly
   - Stock prices: Daily trading days only
   - Dates rarely align exactly across domains

2. **Business Logic Required**:
   - "As of" date matching (e.g., find economic data closest to filing date)
   - Period overlap analysis (e.g., quarterly GDP vs fiscal quarter)
   - Lag/lead relationships (e.g., economic indicators predict future earnings)

3. **Solution: Temporal Joins**:
   ```sql
   -- Example: Join SEC filing with economic data from same quarter
   SELECT f.*, e.*
   FROM filing_metadata f
   JOIN employment_statistics e 
     ON YEAR(e.date) = f.year 
     AND QUARTER(e.date) = QUARTER(f.filing_date)
   WHERE e.series_id = 'UNRATE'
   ```

These relationships are better handled through:
- Application-level temporal join logic
- Window functions with date ranges
- Specialized temporal operators (if available)

### Performance Considerations
- Partition pruning critical for SEC queries
- Geographic joins benefit from spatial indexes
- Cross-domain joins should filter early to reduce data movement
- Temporal joins should use date indexes and partition pruning