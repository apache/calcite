# Economic Data Schema Design

## Overview

Create a new `econ` (economic) schema within the govdata adapter to provide SQL access to U.S. economic data from the Bureau of Labor Statistics (BLS), Federal Reserve, U.S. Treasury, and other economic data sources.

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

## Proposed Tables

### Core Economic Indicators

1. **`employment_statistics`** - Monthly employment data
   - Columns: date, series_id, series_name, value, seasonally_adjusted, industry_code, area_code, area_name, period_type, source
   - Primary key: (date, series_id)
   - Partitioned by: year, month
   - Includes: Non-farm payrolls, unemployment rate, labor force participation

2. **`inflation_metrics`** - CPI and PPI data
   - Columns: date, index_type, item_code, item_name, index_value, percent_change_month, percent_change_year, seasonally_adjusted, area_code, source
   - Primary key: (date, index_type, item_code, area_code)
   - Partitioned by: year, month
   - Includes: CPI-U, CPI-W, Core CPI, PPI by commodity

3. **`interest_rates`** - Federal Reserve and market interest rates
   - Columns: date, rate_type, rate_name, rate_value, maturity, frequency, source
   - Primary key: (date, rate_type)
   - Partitioned by: year, month
   - Includes: Fed Funds, SOFR, Prime Rate, LIBOR replacements

4. **`treasury_yields`** - Daily Treasury yield curve
   - Columns: date, maturity_months, maturity_label, yield_percent, yield_type, source
   - Primary key: (date, maturity_months)
   - Partitioned by: year, month
   - Includes: 1M to 30Y yields, TIPS yields

### Economic Time Series

5. **`fred_series`** - FRED economic time series metadata
   - Columns: series_id, title, units, frequency, seasonal_adjustment, popularity, observation_start, observation_end, last_updated
   - Primary key: series_id
   - Enables discovery of available series

6. **`fred_observations`** - FRED time series data points
   - Columns: series_id, date, value, revision_date
   - Primary key: (series_id, date)
   - Foreign key: series_id -> fred_series.series_id
   - Partitioned by: year

7. **`gdp_components`** - GDP breakdown by component
   - Columns: date, component_code, component_name, value_billions, real_nominal, percent_of_gdp, percent_change_quarter, percent_change_year
   - Primary key: (date, component_code)
   - Includes: Personal consumption, investment, government spending, net exports

### Labor Market Details

8. **`jobs_report`** - Monthly employment situation report
   - Columns: date, industry_sector, jobs_added, total_employment, average_hourly_earnings, average_weekly_hours, revision_prior_month
   - Primary key: (date, industry_sector)
   - Detailed breakdown by NAICS industry codes

9. **`unemployment_demographics`** - Unemployment by demographics
   - Columns: date, demographic_group, unemployment_rate, employment_population_ratio, labor_force_participation_rate, not_in_labor_force
   - Primary key: (date, demographic_group)
   - Breakdown by age, gender, race, education level

10. **`wage_growth`** - Wage and compensation metrics
    - Columns: date, series_type, industry_sector, wage_growth_yoy, wage_growth_qoq, median_weekly_earnings, benefits_cost_index
    - Primary key: (date, series_type, industry_sector)

### Financial Markets and Treasury

11. **`treasury_auctions`** - Treasury securities auction results
    - Columns: auction_date, security_type, maturity_date, high_yield, median_yield, low_yield, bid_to_cover_ratio, indirect_bidders_pct
    - Primary key: (auction_date, security_type, maturity_date)

12. **`federal_debt`** - Public debt statistics
    - Columns: date, debt_type, amount_billions, percent_of_gdp, holder_category
    - Primary key: (date, debt_type, holder_category)
    - Tracks debt by holder type (foreign, domestic, Fed)

13. **`fomc_decisions`** - Federal Open Market Committee decisions
    - Columns: meeting_date, decision_type, target_rate_lower, target_rate_upper, vote_count, dissents, statement_sentiment
    - Primary key: meeting_date
    - Links to statement text and minutes

### Regional and Sector Data

14. **`regional_employment`** - State and MSA employment data
    - Columns: date, area_code, area_name, area_type, employment_total, unemployment_rate, labor_force, employment_population_ratio
    - Primary key: (date, area_code)
    - Foreign key: area_code references geo.geographic_areas

15. **`housing_indicators`** - Housing market indicators
    - Columns: date, area_code, indicator_type, value, period_type
    - Primary key: (date, area_code, indicator_type)
    - Includes: Housing starts, building permits, home prices

## Implementation Approach

### Phase 1: Core Economic Data
1. Set up BLS API integration with rate limiting
2. Implement FRED API data fetcher
3. Create employment_statistics, inflation_metrics, interest_rates tables
4. Add real-time data refresh capabilities

### Phase 2: Treasury and Fed Data
1. Integrate Treasury Direct API
2. Add treasury_yields and treasury_auctions tables
3. Parse FOMC meeting calendar and decisions
4. Implement federal_debt tracking

### Phase 3: Time Series and Advanced Analytics
1. Build comprehensive FRED series catalog
2. Implement efficient time series storage
3. Add GDP components and detailed breakdowns
4. Create materialized views for common queries

### Phase 4: Regional and Sector Integration
1. Link regional data to geo schema
2. Add industry sector mappings to SEC company data
3. Create cross-schema analytical views
4. Implement housing and real estate indicators

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
WHERE series_id = 'UNRATE'
  AND date = (SELECT MAX(date) FROM econ.employment_statistics)
UNION ALL
SELECT 
    'CPI Inflation',
    percent_change_year,
    percent_change_year - LAG(percent_change_year, 12) OVER (ORDER BY date)
FROM econ.inflation_metrics
WHERE index_type = 'CPI-U'
  AND item_code = 'All Items'
  AND date = (SELECT MAX(date) FROM econ.inflation_metrics)
UNION ALL
SELECT 
    'Fed Funds Rate',
    rate_value,
    rate_value - LAG(rate_value, 12) OVER (ORDER BY date)
FROM econ.interest_rates
WHERE rate_type = 'FEDFUNDS'
  AND date = (SELECT MAX(date) FROM econ.interest_rates);

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

-- Yield curve analysis
SELECT 
    date,
    MAX(CASE WHEN maturity_months = 3 THEN yield_percent END) as "3M",
    MAX(CASE WHEN maturity_months = 24 THEN yield_percent END) as "2Y",
    MAX(CASE WHEN maturity_months = 60 THEN yield_percent END) as "5Y",
    MAX(CASE WHEN maturity_months = 120 THEN yield_percent END) as "10Y",
    MAX(CASE WHEN maturity_months = 360 THEN yield_percent END) as "30Y",
    MAX(CASE WHEN maturity_months = 120 THEN yield_percent END) - 
    MAX(CASE WHEN maturity_months = 24 THEN yield_percent END) as "10Y-2Y_spread",
    CASE 
        WHEN MAX(CASE WHEN maturity_months = 120 THEN yield_percent END) < 
             MAX(CASE WHEN maturity_months = 24 THEN yield_percent END) 
        THEN 'INVERTED'
        ELSE 'NORMAL'
    END as curve_shape
FROM econ.treasury_yields
WHERE date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY date
ORDER BY date DESC;
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