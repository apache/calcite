# Economic Data Schema Design

## Overview

Create a new `econ` (economic) schema within the govdata adapter to provide SQL access to U.S. economic data from the Bureau of Labor Statistics (BLS), Federal Reserve, U.S. Treasury, and other economic data sources.

## API Access Requirements and Authentication

### Bureau of Labor Statistics (BLS)
- **Base API URL**: `https://api.bls.gov/publicAPI/v2/`
- **Documentation**: https://www.bls.gov/developers/
- **Registration**: https://data.bls.gov/registrationEngine/
- **Authentication**: 
  - **Optional but recommended**: API key increases rate limits
  - **Without key**: 25 requests/day, 10 years of data
  - **With key**: 500 requests/day, 20 years of data
- **Key Generation**:
  1. Register at https://data.bls.gov/registrationEngine/
  2. Verify email address
  3. API key sent via email immediately
  4. No approval process - instant access
- **Rate Limits**: 
  - 25 queries/day without key
  - 500 queries/day with key
  - No per-second rate limit
- **Example Request**:
  ```bash
  curl -X POST 'https://api.bls.gov/publicAPI/v2/timeseries/data/' \
    -H 'Content-Type: application/json' \
    -d '{"seriesid":["CUUR0000SA0"],"startyear":"2020","endyear":"2024","registrationkey":"YOUR_KEY"}'
  ```

### Federal Reserve Economic Data (FRED)
- **Base API URL**: `https://api.stlouisfed.org/fred/`
- **Documentation**: https://fred.stlouisfed.org/docs/api/fred/
- **Registration**: https://fred.stlouisfed.org/docs/api/api_key.html
- **Authentication**:
  - **Required**: API key mandatory for all requests
  - **Free tier**: Generous limits for most use cases
- **Key Generation**:
  1. Create account at https://fred.stlouisfed.org/useraccount/register
  2. Request API key at https://fredaccount.stlouisfed.org/apikeys
  3. Key generated instantly upon request
  4. No approval process - immediate access
- **Rate Limits**:
  - 120 requests/minute
  - 40,000 requests/day
- **Example Request**:
  ```bash
  curl 'https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key=YOUR_KEY&file_type=json'
  ```
- **Popular Series IDs**:
  - `GDP`: Gross Domestic Product
  - `UNRATE`: Unemployment Rate
  - `DFF`: Federal Funds Rate
  - `CPIAUCSL`: Consumer Price Index
  - `DGS10`: 10-Year Treasury Rate

### U.S. Treasury
- **Base URL**: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/`
- **Documentation**: https://fiscaldata.treasury.gov/api-documentation/
- **Authentication**: 
  - **None required**: Completely open API
  - **No registration needed**: Direct access
- **Rate Limits**:
  - No published rate limits
  - Reasonable use expected
- **Example Request**:
  ```bash
  curl 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange?filter=record_date:gte:2024-01-01'
  ```
- **Key Endpoints**:
  - `/v1/accounting/od/rates_of_exchange`: Exchange rates
  - `/v1/accounting/od/debt_to_penny`: National debt
  - `/v1/accounting/od/avg_interest_rates`: Treasury interest rates
  - `/v1/accounting/od/treasury_offset_program`: Treasury yields

### Bureau of Economic Analysis (BEA)
- **Base API URL**: `https://apps.bea.gov/api/`
- **Documentation**: https://apps.bea.gov/API/docs/index.htm
- **Registration**: https://apps.bea.gov/API/signup/
- **Authentication**:
  - **Required**: API key mandatory
  - **Free**: No cost for API access
- **Key Generation**:
  1. Register at https://apps.bea.gov/API/signup/
  2. Provide name, email, and affiliation
  3. Key emailed within minutes
  4. Manual review process (usually same day)
- **Rate Limits**:
  - 1000 requests/hour
  - 30,000 requests/day
- **Example Request**:
  ```bash
  curl 'https://apps.bea.gov/api/data/?UserID=YOUR_KEY&method=GetData&datasetname=NIPA&TableName=T10101&Frequency=Q&Year=2024&ResultFormat=JSON'
  ```

### World Bank (for international context)
- **Base API URL**: `https://api.worldbank.org/v2/`
- **Documentation**: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
- **Authentication**: 
  - **None required**: Open access
  - **No registration needed**
- **Rate Limits**:
  - No explicit limits
  - Requests may be throttled if excessive
- **Example Request**:
  ```bash
  curl 'https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&date=2020:2024'
  ```

## Data Sources

### Bureau of Labor Statistics (BLS)
1. **BLS Public Data API v2** (Free, requires registration)
   - Employment and unemployment data
   - Consumer Price Index (CPI) for inflation
   - Producer Price Index (PPI)
   - Employment Cost Index
   - Productivity statistics
   - API Key required (free registration)
   - Rate limit: 500 requests/day with key, 25 without

2. **BLS Data Series**
   - Current Employment Statistics (CES)
   - Local Area Unemployment Statistics (LAUS)
   - Occupational Employment and Wage Statistics (OEWS)
   - Consumer Expenditure Survey (CE)

### Federal Reserve Economic Data (FRED)
1. **FRED API** (Free, requires API key)
   - 800,000+ economic time series
   - Interest rates (Fed Funds, SOFR, Prime Rate)
   - GDP and economic indicators
   - Exchange rates
   - Financial market data
   - Rate limit: 120 requests/minute

2. **Federal Reserve Board Data**
   - FOMC meeting minutes and decisions
   - Beige Book reports
   - H.15 Selected Interest Rates
   - Z.1 Financial Accounts

### U.S. Treasury
1. **Treasury Direct API** (Free, public)
   - Treasury securities auction results
   - Daily Treasury yield curve rates
   - Monthly statement of public debt
   - Treasury bill, note, and bond data

2. **FiscalData.Treasury.gov API** (Free, public)
   - Federal revenue and spending
   - National debt data
   - Treasury securities outstanding
   - Daily Treasury statements

### Additional Sources
1. **Bureau of Economic Analysis (BEA)**
   - GDP by state and industry
   - Personal income and outlays
   - International trade and investment

2. **Congressional Budget Office (CBO)**
   - Economic projections
   - Budget and deficit forecasts

## Implemented Tables

### Core Economic Indicators

1. **`employment_statistics`** - U.S. employment and unemployment statistics
   - **Description**: Monthly employment and unemployment statistics from BLS including national unemployment rate, labor force participation, job openings, and employment by sector. Updated monthly with seasonal adjustments.
   - **Columns**:
     - `date` (DATE): Observation date (first day of month for monthly data)
     - `series_id` (VARCHAR): BLS series identifier (e.g., 'UNRATE' for unemployment rate)
     - `series_name` (VARCHAR): Human-readable series description
     - `value` (DECIMAL): Metric value (rate as percentage or count in thousands)
     - `unit` (VARCHAR): Unit of measurement (percent, thousands, index)
     - `seasonally_adjusted` (BOOLEAN): Whether data is seasonally adjusted
     - `percent_change_month` (DECIMAL): Percent change from previous month
     - `percent_change_year` (DECIMAL): Percent change from same month previous year
     - `category` (VARCHAR): Major category (e.g., 'Employment', 'Unemployment', 'Labor Force')
     - `subcategory` (VARCHAR): Detailed subcategory (e.g., 'Manufacturing', 'Services')
   - **Primary key**: (date, series_id)
   - **Data source**: BLS API series including LNS14000000 (unemployment), CES0000000001 (employment level), LNS11300000 (labor force participation)

2. **`inflation_metrics`** - Consumer and Producer Price Index data
   - **Description**: CPI and PPI data tracking inflation across different categories of goods and services. Includes urban, regional, and sector-specific inflation rates.
   - **Columns**:
     - `date` (DATE): Observation date for the index value
     - `index_type` (VARCHAR): Type of price index (CPI-U, CPI-W, PPI, etc.)
     - `item_code` (VARCHAR): BLS item code for specific good/service category
     - `item_name` (VARCHAR): Description of item or category (e.g., 'All items', 'Food', 'Energy')
     - `index_value` (DECIMAL): Index value (base period = 100)
     - `percent_change_month` (DECIMAL): Percent change from previous month
     - `percent_change_year` (DECIMAL): Year-over-year percent change (inflation rate)
     - `area_code` (VARCHAR): Geographic area code (U.S., regions, or metro areas)
     - `area_name` (VARCHAR): Geographic area name
     - `seasonally_adjusted` (BOOLEAN): Whether data is seasonally adjusted
   - **Primary key**: (date, index_type, item_code, area_code)
   - **Data source**: BLS API series including CUUR0000SA0 (CPI-U All Urban), CUUR0000SA0L1E (Core CPI), WPUFD4 (PPI Final Demand)

3. **`wage_growth`** - Average earnings and compensation data
   - **Description**: Average hourly earnings, weekly earnings, and employment cost index by industry and occupation. Tracks wage growth trends and labor cost pressures.
   - **Columns**:
     - `date` (DATE): Observation date
     - `series_id` (VARCHAR): BLS series identifier
     - `industry_code` (VARCHAR): NAICS industry code
     - `industry_name` (VARCHAR): Industry description
     - `occupation_code` (VARCHAR): SOC occupation code
     - `occupation_name` (VARCHAR): Occupation description
     - `average_hourly_earnings` (DECIMAL): Average hourly earnings in dollars
     - `average_weekly_earnings` (DECIMAL): Average weekly earnings in dollars
     - `employment_cost_index` (DECIMAL): Employment cost index (base = 100)
     - `percent_change_year` (DECIMAL): Year-over-year percent change in earnings
   - **Primary key**: (date, series_id, industry_code, occupation_code)
   - **Data source**: BLS API series including CES0500000003 (average hourly earnings), CIU1010000000000A (employment cost index)

4. **`regional_employment`** - State and metropolitan area employment statistics
   - **Description**: State and metropolitan area employment statistics including unemployment rates, job growth, and labor force participation by geographic region.
   - **Columns**:
     - `date` (DATE): Observation date
     - `area_code` (VARCHAR): Geographic area code (FIPS or MSA code)
     - `area_name` (VARCHAR): Geographic area name
     - `area_type` (VARCHAR): Type of area (state, MSA, county)
     - `state_code` (VARCHAR): Two-letter state code
     - `unemployment_rate` (DECIMAL): Unemployment rate as percentage
     - `employment_level` (BIGINT): Number of employed persons
     - `labor_force` (BIGINT): Total labor force size
     - `participation_rate` (DECIMAL): Labor force participation rate as percentage
     - `employment_population_ratio` (DECIMAL): Employment to population ratio
   - **Primary key**: (date, area_code)
   - **Foreign key**: state_code → geo.tiger_states.state_code
   - **Data source**: BLS Local Area Unemployment Statistics (LAUS) series

### Financial Market Indicators

5. **`treasury_yields`** - Daily Treasury yield curve rates
   - **Description**: Daily U.S. Treasury yield curve data including rates for various maturities from 1-month to 30-year securities. Tracks government borrowing costs and serves as risk-free rate benchmark.
   - **Columns**:
     - `date` (DATE): Observation date
     - `maturity_months` (INTEGER): Maturity period in months (1, 3, 6, 12, 24, 60, 120, 360)
     - `maturity_label` (VARCHAR): Human-readable maturity label (e.g., '1M', '10Y', '30Y')
     - `yield_percent` (DECIMAL): Yield rate as percentage
     - `yield_type` (VARCHAR): Type of yield (nominal, real/TIPS, average)
     - `source` (VARCHAR): Data source (Treasury Direct)
   - **Primary key**: (date, maturity_months)
   - **Data source**: Treasury Fiscal Data API - Average Interest Rates endpoint

6. **`federal_debt`** - U.S. federal debt statistics
   - **Description**: Daily federal debt levels including debt held by public, intragovernmental holdings, and total outstanding debt. Tracks national debt trends and composition.
   - **Columns**:
     - `date` (DATE): Observation date
     - `debt_type` (VARCHAR): Type of debt measurement (Total, Held by Public, Intragovernmental)
     - `amount_billions` (DECIMAL): Debt amount in billions of dollars
     - `percent_of_gdp` (DECIMAL): Debt as percentage of GDP (when available)
     - `holder_category` (VARCHAR): Category of debt holder
     - `debt_held_by_public` (DECIMAL): Portion held by public in billions
     - `intragovernmental_holdings` (DECIMAL): Portion held by government accounts in billions
   - **Primary key**: (date, debt_type)
   - **Data source**: Treasury Fiscal Data API - Debt to the Penny endpoint

7. **`world_indicators`** - International economic indicators for comparison
   - **Description**: Key economic indicators for major world economies including GDP, inflation, unemployment, and government debt. Enables international comparisons and global economic analysis.
   - **Columns**:
     - `country_code` (VARCHAR): ISO 3-letter country code
     - `country_name` (VARCHAR): Country name
     - `indicator_code` (VARCHAR): World Bank indicator code
     - `indicator_name` (VARCHAR): Indicator description
     - `year` (INTEGER): Observation year
     - `value` (DECIMAL): Indicator value
     - `unit` (VARCHAR): Unit of measurement
     - `scale` (VARCHAR): Scale factor if applicable
   - **Primary key**: (country_code, indicator_code, year)
   - **Data source**: World Bank API - World Development Indicators

### Federal Reserve Economic Data (FRED)

8. **`fred_indicators`** - Federal Reserve economic time series data
   - **Description**: Comprehensive economic time series from FRED including interest rates, monetary aggregates, exchange rates, and thousands of other economic indicators.
   - **Columns**:
     - `series_id` (VARCHAR): FRED series identifier (e.g., 'DFF', 'GDP', 'UNRATE')
     - `series_name` (VARCHAR): Human-readable series description
     - `date` (DATE): Observation date
     - `value` (DECIMAL): Observation value
     - `units` (VARCHAR): Unit of measurement
     - `frequency` (VARCHAR): Data frequency (D, W, M, Q, A)
     - `seasonal_adjustment` (VARCHAR): Seasonal adjustment method if applicable
     - `last_updated` (TIMESTAMP): When series was last updated
   - **Primary key**: (series_id, date)
   - **Data source**: FRED API (requires API key)

### Bureau of Economic Analysis (BEA)

9. **`gdp_components`** - GDP breakdown by component
   - **Description**: Detailed GDP components including personal consumption, investment, government spending, and net exports. Provides granular view of economic activity drivers.
   - **Columns**:
     - `table_id` (VARCHAR): BEA NIPA table identifier
     - `line_number` (INTEGER): Line number within table
     - `line_description` (VARCHAR): Component description
     - `series_code` (VARCHAR): BEA series code
     - `year` (INTEGER): Observation year
     - `value` (DECIMAL): Component value in billions
     - `units` (VARCHAR): Unit of measurement
     - `frequency` (VARCHAR): Data frequency (A, Q, M)
   - **Primary key**: (table_id, line_number, year)
   - **Data source**: BEA API - NIPA tables (requires API key)

10. **`regional_income`** - Personal income by state and region
    - **Description**: State and regional personal income statistics including per capita income, population, and total personal income. Tracks regional economic disparities and trends.
    - **Columns**:
      - `geo_fips` (VARCHAR): Geographic FIPS code
      - `geo_name` (VARCHAR): Geographic area name
      - `metric` (VARCHAR): Metric type (Total Income, Per Capita, Population)
      - `line_code` (VARCHAR): BEA line code
      - `line_description` (VARCHAR): Detailed description
      - `year` (INTEGER): Observation year
      - `value` (DECIMAL): Metric value
      - `units` (VARCHAR): Unit of measurement
    - **Primary key**: (geo_fips, metric, year)
    - **Data source**: BEA API - Regional Economic Accounts (requires API key)

## Planned Future Tables

The following tables are planned for future implementation:

- **`fomc_decisions`** - Federal Open Market Committee decisions
- **`housing_indicators`** - Housing market indicators
- **`trade_statistics`** - International trade data
- **`productivity_metrics`** - Labor productivity statistics

## Configuration and Environment Variables

### Setting Up API Keys

#### Environment Variables (Recommended)
```bash
# Add to ~/.bashrc, ~/.zshrc, or .env file
export BLS_API_KEY="your-bls-api-key-here"
export FRED_API_KEY="your-fred-api-key-here"
export BEA_API_KEY="your-bea-api-key-here"

# Unified date range configuration (applies to all government data)
export GOVDATA_START_YEAR=2020
export GOVDATA_END_YEAR=2024

# Optional: ECON-specific date range override
# export ECON_START_YEAR=2015
# export ECON_END_YEAR=2024

# No keys needed for Treasury or World Bank
```

#### Model Configuration
```json
{
  "schemas": [{
    "name": "econ",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "econ",
      "blsApiKey": "${BLS_API_KEY}",
      "fredApiKey": "${FRED_API_KEY}",
      "beaApiKey": "${BEA_API_KEY}",
      "enabledSources": ["bls", "fred", "treasury", "bea"],
      "cacheDirectory": "${ECON_CACHE_DIR:/tmp/econ-cache}",
      "updateFrequency": "daily",
      "historicalDepth": "10 years"
    }
  }]
}
```

### API Key Priority Order
1. **Explicit in model**: `"blsApiKey": "abc123"`
2. **Environment variable**: `${BLS_API_KEY}`
3. **System property**: `-Dbls.api.key=abc123`
4. **Default behavior**: Run without key (limited functionality)

### Initial Data Download

#### Estimated Data Volumes
- **BLS**: ~500MB for 10 years of major series
- **FRED**: ~2GB for popular 1000 series
- **Treasury**: ~100MB for yield curves and debt data
- **BEA**: ~1GB for GDP and trade data
- **Total Initial**: ~4GB compressed Parquet

#### Download Strategy
```sql
-- System will automatically fetch data on first query
-- Or pre-populate cache with common series:
CALL econ.refresh_data('UNRATE', '2014-01-01', '2024-12-31');
CALL econ.refresh_data('GDP', '2014-01-01', '2024-12-31');
CALL econ.refresh_data('CPIAUCSL', '2014-01-01', '2024-12-31');
```

### Testing API Access

#### Quick API Verification Script
```bash
#!/bin/bash
# test_econ_apis.sh - Verify all economic API keys work

echo "Testing BLS API..."
curl -s -X POST 'https://api.bls.gov/publicAPI/v2/timeseries/data/' \
  -H 'Content-Type: application/json' \
  -d "{\"seriesid\":[\"UNRATE\"],\"startyear\":\"2024\",\"endyear\":\"2024\",\"registrationkey\":\"$BLS_API_KEY\"}" \
  | grep -q "Results" && echo "✓ BLS API working" || echo "✗ BLS API failed"

echo "Testing FRED API..."
curl -s "https://api.stlouisfed.org/fred/series?series_id=GDP&api_key=$FRED_API_KEY&file_type=json" \
  | grep -q "seriess" && echo "✓ FRED API working" || echo "✗ FRED API failed"

echo "Testing Treasury API (no key needed)..."
curl -s 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/debt_to_penny?limit=1' \
  | grep -q "data" && echo "✓ Treasury API working" || echo "✗ Treasury API failed"

echo "Testing BEA API..."
curl -s "https://apps.bea.gov/api/data/?UserID=$BEA_API_KEY&method=GetParameterList&datasetname=NIPA&ResultFormat=JSON" \
  | grep -q "Parameter" && echo "✓ BEA API working" || echo "✗ BEA API failed"
```

## Implementation Status

### ✅ Completed (Phase 1-3)
1. **BLS API integration** - Full implementation with rate limiting
   - employment_statistics table with unemployment and jobs data
   - inflation_metrics table with CPI/PPI data
   - wage_growth table with earnings data
   - regional_employment table with state/metro data

2. **Treasury Direct API** - Complete integration (no auth required)
   - treasury_yields table with daily yield curve
   - federal_debt table with debt statistics

3. **World Bank API** - International comparison data
   - world_indicators table for G20 economic metrics
   - Global GDP tracking for all countries

4. **FRED API** - Federal Reserve data (requires API key)
   - fred_indicators table with 800K+ time series
   - Support for all major economic indicators

5. **BEA API** - Bureau of Economic Analysis (requires API key)
   - gdp_components table with detailed GDP breakdown
   - regional_income table with state-level income data

### Phase 4: Regional and Sector Integration
1. Link regional data to geo schema
2. Add industry sector mappings to SEC company data
3. Create cross-schema analytical views
4. Implement housing and real estate indicators

## Common Economic Series Reference

### Key BLS Series IDs
- **UNRATE**: Unemployment Rate (monthly)
- **CUUR0000SA0**: CPI-U All Items (monthly inflation)
- **WPUFD4**: PPI Final Demand (producer prices)
- **CES0000000001**: Total Nonfarm Employment
- **LNS14000000**: Labor Force Participation Rate
- **CES0500000003**: Average Hourly Earnings

### Key FRED Series IDs
- **GDP**: Gross Domestic Product (quarterly)
- **GDPC1**: Real GDP (inflation-adjusted)
- **DFF**: Federal Funds Rate (daily)
- **DGS10**: 10-Year Treasury Rate (daily)
- **DGS2**: 2-Year Treasury Rate (daily)
- **SOFR**: Secured Overnight Financing Rate
- **DEXUSEU**: USD/EUR Exchange Rate
- **VIXCLS**: VIX Volatility Index
- **HOUST**: Housing Starts
- **INDPRO**: Industrial Production Index
- **UMCSENT**: Consumer Sentiment Index

### Treasury Data Endpoints
- **debt_to_penny**: Daily national debt
- **avg_interest_rates**: Average interest rates on Treasury securities
- **treasury_offset_program**: Collections data
- **rates_of_exchange**: Foreign exchange rates

### BEA Table IDs
- **T10101**: GDP and Personal Income
- **T10201**: Personal Consumption Expenditures
- **T20301**: Government Current Receipts and Expenditures
- **T70101**: Foreign Trade in Goods and Services

## Cross-Schema Relationships

### Links to SEC Schema
- Company performance vs. economic indicators
- Industry employment vs. sector financial performance
- Interest rates impact on company debt costs
- Inflation effects on revenue and margins

### Links to GEO Schema
- Regional unemployment by county/MSA
- State-level GDP and economic growth
- Metropolitan area wage differentials
- Geographic inflation variations

## Query Examples

```sql
-- Current economic dashboard
SELECT 
    'Unemployment Rate' as indicator,
    value as current_value,
    percent_change_year as yoy_change
FROM econ.employment_statistics
WHERE series_id = 'LNS14000000'  -- Unemployment rate series
  AND date = (SELECT MAX(date) FROM econ.employment_statistics)
UNION ALL
SELECT 
    'CPI Inflation' as indicator,
    index_value as current_value,
    percent_change_year as yoy_change
FROM econ.inflation_metrics
WHERE index_type = 'CPI-U'
  AND item_name = 'All items'
  AND date = (SELECT MAX(date) FROM econ.inflation_metrics)
UNION ALL
SELECT 
    'Average Hourly Earnings' as indicator,
    average_hourly_earnings as current_value,
    percent_change_year as yoy_change
FROM econ.wage_growth
WHERE series_id = 'CES0500000003'
  AND date = (SELECT MAX(date) FROM econ.wage_growth);

-- Company performance vs. economic conditions
SELECT 
    s.company_name,
    s.fiscal_year,
    s.revenue_growth_yoy,
    e.gdp_growth_yoy,
    e.unemployment_rate,
    e.cpi_inflation_yoy,
    CORR(s.revenue_growth_yoy, e.gdp_growth_yoy) OVER (
        PARTITION BY s.cik 
        ORDER BY s.fiscal_year 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as revenue_gdp_correlation
FROM (
    SELECT 
        cik,
        company_name,
        fiscal_year,
        (revenue - LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year)) 
            / LAG(revenue) OVER (PARTITION BY cik ORDER BY fiscal_year) * 100 as revenue_growth_yoy
    FROM sec.financial_metrics
) s
JOIN (
    SELECT 
        EXTRACT(YEAR FROM date) as year,
        AVG(CASE WHEN series_id = 'GDP' THEN percent_change_year END) as gdp_growth_yoy,
        AVG(CASE WHEN series_id = 'UNRATE' THEN value END) as unemployment_rate,
        AVG(CASE WHEN index_type = 'CPI-U' THEN percent_change_year END) as cpi_inflation_yoy
    FROM econ.employment_statistics
    GROUP BY EXTRACT(YEAR FROM date)
) e ON s.fiscal_year = e.year;

-- Regional unemployment analysis
SELECT 
    g.state_name,
    g.county_name,
    r.unemployment_rate,
    r.unemployment_rate - s.state_avg_unemployment as diff_from_state_avg,
    RANK() OVER (PARTITION BY g.state_code ORDER BY r.unemployment_rate DESC) as unemployment_rank_in_state
FROM econ.regional_employment r
JOIN geo.counties g ON r.area_code = g.county_fips
JOIN (
    SELECT 
        LEFT(area_code, 2) as state_fips,
        AVG(unemployment_rate) as state_avg_unemployment
    FROM econ.regional_employment
    WHERE area_type = 'county'
      AND date = (SELECT MAX(date) FROM econ.regional_employment)
    GROUP BY LEFT(area_code, 2)
) s ON LEFT(g.county_fips, 2) = s.state_fips
WHERE r.date = (SELECT MAX(date) FROM econ.regional_employment)
  AND r.area_type = 'county';

-- Wage growth analysis by industry
SELECT 
    industry_name,
    date,
    average_hourly_earnings,
    percent_change_year as wage_growth_yoy,
    AVG(percent_change_year) OVER (
        PARTITION BY industry_code 
        ORDER BY date 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as rolling_12m_avg_growth,
    RANK() OVER (
        PARTITION BY date 
        ORDER BY percent_change_year DESC
    ) as wage_growth_rank
FROM econ.wage_growth
WHERE date >= CURRENT_DATE - INTERVAL '2 years'
  AND industry_code IS NOT NULL
ORDER BY date DESC, wage_growth_yoy DESC;
```

## Performance Considerations

### Data Volume Estimates
- **Employment data**: ~10GB (detailed series back to 1948)
- **FRED series**: ~50GB (800,000+ series with history)
- **Treasury data**: ~5GB (daily yields, auction results)
- **Regional data**: ~20GB (county-level employment for 3,000+ counties)

### Optimization Strategies
1. **Time-based partitioning**: Partition by year/month for efficient queries
2. **Materialized views**: Pre-aggregate common economic indicators
3. **Incremental updates**: Only fetch new/revised data points
4. **Series compression**: Use columnar storage for time series data
5. **Smart caching**: Cache frequently accessed series locally

## Update Frequency

- **Employment data**: Monthly (first Friday of month)
- **CPI/PPI**: Monthly (mid-month release)
- **GDP**: Quarterly with revisions
- **Interest rates**: Daily (Fed Funds), Weekly (H.15 report)
- **Treasury yields**: Daily at 3:00 PM ET
- **FOMC decisions**: 8 times per year

## API Configuration

```json
{
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
      "enabledSources": ["bls", "fred", "treasury"],
      "cacheDirectory": "${ECON_CACHE_DIR:/tmp/econ-cache}"
    }
  }]
}
```

## Dependencies

- BLS API v2 client library
- FRED API client (consider using fred-api-java)
- Treasury Direct API integration
- Time series database optimizations
- Statistical functions for economic calculations

## Security and Compliance

- All data sources are public government data
- API keys are free but required for higher rate limits
- No PII or sensitive information
- Terms of use compliance for each data source
- Proper attribution required for some sources

## Future Enhancements

1. **Machine Learning Integration**
   - Economic forecasting models
   - Anomaly detection for economic indicators
   - Correlation analysis across indicators

2. **International Data**
   - Foreign exchange rates
   - International trade statistics
   - Comparative economic indicators

3. **Alternative Data**
   - Google Trends economic indicators
   - Satellite data for economic activity
   - Social media sentiment indicators

4. **Real-time Streaming**
   - WebSocket feeds for market data
   - Real-time Treasury auction results
   - Live FOMC announcement processing

This creates a comprehensive economic data schema that complements the existing SEC financial and GEO demographic data, enabling powerful economic analysis and cross-domain insights.