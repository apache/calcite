# Apache Calcite Government Data Adapter

[![Build Status](https://github.com/apache/calcite/actions/workflows/gradle.yml/badge.svg)](https://github.com/apache/calcite/actions/workflows/gradle.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The Government Data Adapter provides unified SQL access to various government data sources through Apache Calcite. Currently supports SEC financial data and HUD/Census geographic data, with plans for additional Census demographics, IRS, Treasury, and other government datasets.

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

#### SEC Financial Data
```json
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "directory": "/path/to/cache",
      "ciks": ["AAPL", "MSFT", "GOOGL"],
      "startYear": 2020,
      "endYear": 2024
    }
  }]
}
```

#### Economic Data
```json
{
  "version": "1.0",
  "defaultSchema": "econ",
  "schemas": [{
    "name": "econ",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "econ",
      "blsApiKey": "${BLS_API_KEY}",
      "fredApiKey": "${FRED_API_KEY}",
      "updateFrequency": "daily",
      "historicalDepth": "10 years",
      "enabledSources": ["bls", "fred", "treasury"]
    }
  }]
}
```

#### Public Safety Data
```json
{
  "version": "1.0",
  "defaultSchema": "safety",
  "schemas": [{
    "name": "safety",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "safety",
      "fbiApiKey": "${FBI_API_KEY}",
      "femaApiKey": "${FEMA_API_KEY}",
      "nhtsaApiKey": "${NHTSA_API_KEY}",
      "updateFrequency": "monthly",
      "historicalDepth": "5 years",
      "enabledSources": ["fbi", "nhtsa", "fema"],
      "spatialAnalysis": {"enabled": true, "radiusAnalysis": ["1mi", "5mi"]}
    }
  }]
}
```

#### Public Data
```json
{
  "version": "1.0",
  "defaultSchema": "pub",
  "schemas": [{
    "name": "pub",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "pub",
      "wikipediaLanguages": ["en", "es", "fr"],
      "osmRegions": ["us", "canada", "mexico"],
      "wikidataEndpoint": "https://query.wikidata.org/sparql",
      "openalexApiKey": "${OPENALEX_API_KEY}",
      "updateFrequency": "daily",
      "entityLinking": {
        "enabled": true,
        "confidenceThreshold": 0.8
      },
      "spatialAnalysis": {
        "enabled": true,
        "bufferAnalysis": ["100m", "500m", "1km"]
      },
      "cacheDirectory": "${PUB_CACHE_DIR:/tmp/pub-cache}"
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

### Geographic and Census Data (NEW)
- **HUD/Census Integration**: Access to geographic and demographic datasets
- **Fair Market Rent (FMR)**: Housing cost data by metropolitan area
- **Income Limits**: Area median income and affordability metrics
- **Geographic Boundaries**: County, state, and metropolitan area definitions
- **Unified Schema**: Consistent interface across government data sources

### Data Processing
- **Smart Caching**: Intelligent local storage and refresh
- **Partitioned Storage**: Efficient Parquet organization by CIK/filing type/year  
- **Multiple Engines**: DuckDB, Arrow, LINQ4J, Parquet execution support
- **Text Similarity**: Vector embeddings for semantic search
- **Constraint Metadata**: Universal support for key constraints and relationships

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
- **HUD/Census Geographic Data**: Fair Market Rent, Income Limits, Geographic boundaries
- **Economic Data (ECON)**: BLS employment statistics, Federal Reserve indicators, Treasury yields
- **Public Safety (SAFETY)**: FBI crime statistics, traffic safety data, emergency services, disasters
- **Public Data (PUB)**: Wikipedia encyclopedia content, OpenStreetMap geographic data, Wikidata structured knowledge, academic research

### Economic Data Sources (NEW)
The `econ` schema provides unified access to U.S. economic indicators:
- **Bureau of Labor Statistics (BLS)**: Employment data, CPI/PPI inflation metrics, wage growth
- **Federal Reserve (FRED)**: 800,000+ economic time series, interest rates, GDP components
- **U.S. Treasury**: Daily yield curves, auction results, federal debt statistics
- **Bureau of Economic Analysis (BEA)**: GDP by state/industry, trade data

### Public Safety Data Sources (NEW)
The `safety` schema provides comprehensive public safety and risk assessment data:
- **FBI Crime Data Explorer**: NIBRS incident data, UCR summary statistics, hate crimes, arrest data
- **NHTSA Traffic Safety**: Fatal crash analysis (FARS), vehicle safety data, traffic violations
- **FEMA Emergency Management**: Disaster declarations, public assistance projects, hazard mitigation
- **Local Emergency Services**: 911 call data, fire incidents, EMS responses (where available)

### Public Data Sources (NEW)
The `pub` schema provides ubiquitous public data for comprehensive knowledge intelligence:
- **Wikipedia**: Encyclopedia articles, entity data, knowledge extraction across 300+ languages
- **OpenStreetMap**: Buildings, transportation, amenities, detailed geographic data worldwide
- **Wikidata**: Structured knowledge base with 45+ million entities and relationship data
- **Academic Research**: Publications, patents, institutional data from OpenAlex and other sources
- **Entity Resolution**: Cross-reference linking between all data sources with confidence scoring

### Planned Support
- **US Census**: Demographics, economic indicators
- **IRS**: Tax statistics, exempt organizations

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

### Geographic Data Tables
- `fair_market_rent` - HUD Fair Market Rent by area and bedroom count
- `income_limits` - HUD Income Limits by area and household size
- `geographic_boundaries` - County, state, and metropolitan area definitions
- `cbsa_definitions` - Core-Based Statistical Area metadata

### Public Knowledge Tables
- `wikipedia_articles` - Encyclopedia articles with text content and metadata
- `wikipedia_entities` - Entity information extracted from Wikipedia pages
- `openstreetmap_buildings` - Building footprints and property information
- `openstreetmap_transportation` - Roads, transit, and transportation infrastructure
- `openstreetmap_amenities` - Points of interest, services, and facilities
- `wikidata_entities` - Structured entity database with properties and relationships
- `academic_publications` - Research papers and academic literature
- `patent_applications` - Innovation and technology transfer data
- `entity_relationships` - Cross-source entity linking and resolution data

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

### Geographic Data Analysis
```sql
-- Fair Market Rent by metropolitan area
SELECT 
    area_name,
    state,
    efficiency,
    one_bedroom,
    two_bedroom,
    three_bedroom,
    four_bedroom
FROM fair_market_rent
WHERE metro = 1  -- Metropolitan areas only
  AND state = 'CA'
ORDER BY two_bedroom DESC
LIMIT 10;

-- Income limits for California counties
SELECT 
    area_name,
    median_income,
    l50_1,  -- 50% AMI for 1-person household
    l80_4   -- 80% AMI for 4-person household
FROM income_limits
WHERE state_alpha = 'CA'
ORDER BY median_income DESC;
```

### Economic Data Analysis
```sql
-- Current economic dashboard
SELECT 
    'Unemployment Rate' as indicator,
    value as current_value,
    percent_change_year as yoy_change
FROM econ.employment_statistics
WHERE series_id = 'UNRATE'
  AND date = (SELECT MAX(date) FROM econ.employment_statistics)
UNION ALL
SELECT 
    'CPI Inflation',
    percent_change_year,
    percent_change_year - LAG(percent_change_year, 12) OVER (ORDER BY date)
FROM econ.inflation_metrics
WHERE index_type = 'CPI-U' AND item_code = 'All Items';

-- Yield curve analysis
SELECT 
    date,
    MAX(CASE WHEN maturity_months = 3 THEN yield_percent END) as "3M",
    MAX(CASE WHEN maturity_months = 24 THEN yield_percent END) as "2Y",
    MAX(CASE WHEN maturity_months = 120 THEN yield_percent END) as "10Y",
    MAX(CASE WHEN maturity_months = 120 THEN yield_percent END) - 
    MAX(CASE WHEN maturity_months = 24 THEN yield_percent END) as "10Y-2Y_spread"
FROM econ.treasury_yields
WHERE date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY date
ORDER BY date DESC;

-- Company performance vs. economic conditions
SELECT 
    s.company_name,
    s.fiscal_year,
    s.revenue_growth_yoy,
    e.gdp_growth_yoy,
    e.unemployment_rate,
    CORR(s.revenue_growth_yoy, e.gdp_growth_yoy) OVER (
        PARTITION BY s.cik ORDER BY s.fiscal_year
    ) as revenue_gdp_correlation
FROM sec.financial_metrics s
JOIN econ.economic_indicators e ON s.fiscal_year = e.year;
```

### Public Safety Risk Assessment
```sql
-- Comprehensive safety analysis for business locations
SELECT 
    g.county_name,
    g.state_name,
    s.violent_crime_rate_per_100k,
    s.property_crime_rate_per_100k,
    s.traffic_fatality_rate_per_100k,
    s.disaster_risk_score,
    s.overall_safety_score,
    s.safety_rank_in_state,
    e.unemployment_rate,
    CASE 
        WHEN s.overall_safety_score >= 8.0 THEN 'LOW RISK'
        WHEN s.overall_safety_score >= 6.0 THEN 'MODERATE RISK'
        ELSE 'HIGH RISK'
    END as risk_category
FROM safety.public_safety_index s
JOIN geo.counties g ON s.area_code = g.county_fips
JOIN econ.regional_employment e ON g.county_fips = e.area_code
WHERE s.area_type = 'county' AND s.year = 2023
ORDER BY s.overall_safety_score DESC;

-- Crime trend analysis by metropolitan area
SELECT 
    l.agency_name,
    l.state_code,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN c.offense_name LIKE '%THEFT%' THEN 1 END) as theft_incidents,
    AVG(CASE WHEN c.cleared_flag = 'Y' THEN 1.0 ELSE 0.0 END) as clearance_rate,
    STRING_AGG(DISTINCT c.location_type, ', ') as common_locations
FROM safety.crime_incidents c
JOIN safety.law_enforcement_agencies l ON c.agency_ori = l.agency_ori
WHERE c.incident_date >= '2023-01-01' AND l.population_served > 50000
GROUP BY l.agency_ori, l.agency_name, l.state_code
ORDER BY total_incidents DESC LIMIT 20;

-- Cross-domain business risk analysis
SELECT 
    cf.company_name,
    cf.facility_city,
    s.violent_crime_rate_per_100k,
    t.fatal_crashes_per_year,
    d.disaster_count_5year,
    e.unemployment_rate,
    CASE 
        WHEN s.overall_safety_score >= 7.5 AND e.unemployment_rate <= 5.0 THEN 'PREFERRED'
        WHEN s.overall_safety_score >= 6.0 THEN 'ACCEPTABLE'
        ELSE 'HIGH_RISK'
    END as location_recommendation
FROM sec.company_facilities cf
JOIN safety.public_safety_index s ON cf.county_fips = s.area_code
JOIN safety.traffic_fatalities t ON cf.county_fips = t.county_fips
JOIN safety.disaster_declarations d ON cf.county_fips = d.county_fips  
JOIN econ.regional_employment e ON cf.county_fips = e.area_code
WHERE cf.cik = '0000320193' AND s.year = 2023;
```

### Public Data Intelligence Analysis
```sql
-- Enhanced company intelligence with public data
SELECT 
    s.company_name,
    s.cik,
    w.founded_date,
    w.founder_names,
    w.headquarters_location,
    w.industry_description,
    w.notable_events,
    STRING_AGG(DISTINCT wr.related_entity, ', ') as business_relationships,
    COUNT(DISTINCT p.patent_id) as patent_count,
    AVG(a.citation_count) as avg_citation_impact
FROM sec.company_metadata s
JOIN pub.wikipedia_entities w ON s.cik = w.linked_cik
LEFT JOIN pub.entity_relationships wr ON w.entity_id = wr.source_entity_id
LEFT JOIN pub.patent_applications p ON w.entity_id = p.assignee_entity_id
LEFT JOIN pub.academic_publications a ON w.entity_id = a.affiliation_entity_id
WHERE s.cik IN ('0000320193', '0000789019')  -- Apple, Microsoft
GROUP BY s.cik, s.company_name, w.founded_date, w.founder_names, 
         w.headquarters_location, w.industry_description, w.notable_events;

-- Geographic business environment analysis
SELECT 
    cf.company_name,
    cf.facility_address,
    osm.building_type,
    osm.nearby_amenities,
    osm.transportation_access,
    STRING_AGG(DISTINCT osm.amenity_type, ', ') as local_services,
    COUNT(DISTINCT osm.restaurant_count) as dining_options,
    AVG(osm.walkability_score) as walkability
FROM sec.company_facilities cf
JOIN pub.openstreetmap_buildings osm ON ST_Contains(osm.geometry, cf.location_point)
JOIN pub.openstreetmap_amenities amen ON ST_DWithin(cf.location_point, amen.location, 500)
WHERE cf.cik = '0000320193'
GROUP BY cf.company_name, cf.facility_address, osm.building_type, 
         osm.nearby_amenities, osm.transportation_access, osm.walkability_score;

-- Cross-domain knowledge discovery
SELECT 
    w.entity_name,
    w.entity_type,
    w.description,
    COUNT(DISTINCT er.target_entity_id) as relationship_count,
    STRING_AGG(DISTINCT wd.property_value, '; ') as structured_facts,
    COSINE_SIMILARITY(w.content_embedding, target_concept.embedding) as concept_similarity
FROM pub.wikipedia_entities w
JOIN pub.entity_relationships er ON w.entity_id = er.source_entity_id
JOIN pub.wikidata_entities wd ON w.wikidata_id = wd.entity_id
CROSS JOIN (SELECT embedding FROM pub.concept_embeddings WHERE concept = 'artificial intelligence') target_concept
WHERE w.entity_type = 'organization' 
  AND COSINE_SIMILARITY(w.content_embedding, target_concept.embedding) > 0.8
GROUP BY w.entity_id, w.entity_name, w.entity_type, w.description, 
         w.content_embedding, target_concept.embedding
ORDER BY concept_similarity DESC
LIMIT 20;
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
5. **Constraint Metadata Support**: Universal constraint and key relationship metadata
6. **Unified Data Architecture**: Consistent schema patterns across all government sources

### Component Structure
```
govdata/
├── GovDataSchemaFactory       # Main entry point and data source router
├── GovDataDriver             # JDBC driver for connection string parsing
├── sec/                      # SEC-specific implementation
│   ├── SecSchemaFactory      # SEC data schema and table management
│   ├── CikRegistry           # Ticker-to-CIK resolution
│   ├── XbrlToParquetConverter # XBRL processing pipeline
│   └── SecHttpStorageProvider # EDGAR-compliant HTTP client
├── geo/                      # Geographic data implementation
│   ├── GeoSchemaFactory      # HUD/Census data schema management
│   ├── HudDataProvider       # HUD Fair Market Rent and Income Limits
│   └── GeographicBoundaries  # County, state, and metro area definitions
├── econ/                     # Economic data implementation
│   ├── EconSchemaFactory     # Economic data schema management
│   ├── BlsApiClient          # Bureau of Labor Statistics API integration
│   ├── FredApiClient         # Federal Reserve Economic Data API
│   └── TreasuryDataProvider  # Treasury yields and auction data
├── safety/                   # Public safety data implementation
│   ├── SafetySchemaFactory   # Public safety data schema management
│   ├── FbiApiClient          # FBI Crime Data Explorer API integration
│   ├── NhtsaApiClient        # NHTSA traffic safety data API
│   ├── FemaApiClient         # FEMA disaster and emergency data
│   └── LocalDataPortalClient # Municipal crime and emergency data
├── pub/                      # Public data implementation
│   ├── PubSchemaFactory      # Public data schema management
│   ├── WikipediaApiClient    # Wikipedia REST API integration
│   ├── OpenStreetMapClient   # OSM Overpass API and data processing
│   ├── WikidataClient        # Wikidata SPARQL endpoint integration
│   └── AcademicDataProvider  # OpenAlex and research database access
└── common/                   # Shared utilities for government data
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