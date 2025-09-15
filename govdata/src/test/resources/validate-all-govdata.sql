-- ======================================================================
-- COMPREHENSIVE GOVERNMENT DATA VALIDATION
-- ======================================================================
-- Tests all three schemas: SEC, ECON, GEO
-- Validates: table existence, data presence, queryability, relationships

-- ======================================================================
-- SECTION 1: SEC SCHEMA VALIDATION
-- ======================================================================

-- 1.1 SEC Table Existence
SELECT 'SEC.FILING_METADATA' as test_name, COUNT(*) as row_count 
FROM sec.filing_metadata 
LIMIT 1;

SELECT 'SEC.FINANCIAL_LINE_ITEMS' as test_name, COUNT(*) as row_count 
FROM sec.financial_line_items 
LIMIT 1;

SELECT 'SEC.FILING_EXHIBITS' as test_name, COUNT(*) as row_count 
FROM sec.filing_exhibits 
LIMIT 1;

SELECT 'SEC.INSIDER_TRANSACTIONS' as test_name, COUNT(*) as row_count 
FROM sec.insider_transactions 
LIMIT 1;

SELECT 'SEC.STOCK_PRICES' as test_name, COUNT(*) as row_count 
FROM sec.stock_prices 
LIMIT 1;

-- 1.2 SEC Data Quality Check
SELECT 'SEC Apple Data' as test_name, 
       cik, company_name, state_of_incorporation 
FROM sec.filing_metadata 
WHERE cik = '0000320193' 
LIMIT 1;

SELECT 'SEC Microsoft Data' as test_name, 
       cik, company_name, state_of_incorporation 
FROM sec.filing_metadata 
WHERE cik = '0000789019' 
LIMIT 1;

-- 1.3 SEC Financial Data
SELECT 'SEC Net Income' as test_name,
       cik, "year", concept, "value", unit 
FROM sec.financial_line_items 
WHERE concept = 'NetIncomeLoss' 
  AND cik IN ('0000320193', '0000789019')
LIMIT 5;

-- 1.4 SEC Insider Trading
SELECT 'SEC Form 4 Data' as test_name,
       cik, transaction_date, transaction_type, shares, price_per_share
FROM sec.insider_transactions
WHERE cik IN ('0000320193', '0000789019')
LIMIT 5;

-- ======================================================================
-- SECTION 2: ECON SCHEMA VALIDATION  
-- ======================================================================

-- 2.1 ECON Table Existence
SELECT 'ECON.EMPLOYMENT_STATISTICS' as test_name, COUNT(*) as row_count 
FROM econ.employment_statistics 
LIMIT 1;

SELECT 'ECON.TREASURY_YIELDS' as test_name, COUNT(*) as row_count 
FROM econ.treasury_yields 
LIMIT 1;

SELECT 'ECON.FRED_INDICATORS' as test_name, COUNT(*) as row_count 
FROM econ.fred_indicators 
LIMIT 1;

SELECT 'ECON.REGIONAL_EMPLOYMENT' as test_name, COUNT(*) as row_count 
FROM econ.regional_employment 
LIMIT 1;

SELECT 'ECON.CONSUMER_PRICE_INDEX' as test_name, COUNT(*) as row_count 
FROM econ.consumer_price_index 
LIMIT 1;

SELECT 'ECON.GDP_STATISTICS' as test_name, COUNT(*) as row_count 
FROM econ.gdp_statistics 
LIMIT 1;

-- 2.2 ECON Data Quality
SELECT 'ECON Unemployment Rate' as test_name,
       "date", series_id, "value", unit 
FROM econ.employment_statistics 
WHERE series_id = 'UNRATE' 
ORDER BY "date" DESC 
LIMIT 5;

SELECT 'ECON Treasury 10Y' as test_name,
       "date", maturity_months, yield_rate 
FROM econ.treasury_yields 
WHERE maturity_months = 120 
ORDER BY "date" DESC 
LIMIT 5;

SELECT 'ECON CPI Data' as test_name,
       "date", cpi_value, year_over_year_change 
FROM econ.consumer_price_index 
ORDER BY "date" DESC 
LIMIT 5;

-- ======================================================================
-- SECTION 3: GEO SCHEMA VALIDATION
-- ======================================================================

-- 3.1 GEO Table Existence
SELECT 'GEO.TIGER_STATES' as test_name, COUNT(*) as row_count 
FROM geo.tiger_states;

SELECT 'GEO.TIGER_COUNTIES' as test_name, COUNT(*) as row_count 
FROM geo.tiger_counties 
LIMIT 1;

SELECT 'GEO.TIGER_ZCTAS' as test_name, COUNT(*) as row_count 
FROM geo.tiger_zctas 
LIMIT 1;

SELECT 'GEO.TIGER_CBSA' as test_name, COUNT(*) as row_count 
FROM geo.tiger_cbsa 
LIMIT 1;

SELECT 'GEO.TIGER_CENSUS_TRACTS' as test_name, COUNT(*) as row_count 
FROM geo.tiger_census_tracts 
LIMIT 1;

SELECT 'GEO.TIGER_BLOCK_GROUPS' as test_name, COUNT(*) as row_count 
FROM geo.tiger_block_groups 
LIMIT 1;

-- 3.2 GEO Data Quality
SELECT 'GEO California Data' as test_name,
       state_code, state_name, state_fips 
FROM geo.tiger_states 
WHERE state_code = 'CA';

SELECT 'GEO New York Data' as test_name,
       state_code, state_name, state_fips 
FROM geo.tiger_states 
WHERE state_code = 'NY';

-- ======================================================================
-- SECTION 4: CROSS-SCHEMA RELATIONSHIPS
-- ======================================================================

-- 4.1 SEC + GEO: Companies by State
SELECT 'SEC-GEO Join' as test_name,
       m.company_name,
       m.state_of_incorporation,
       s.state_name
FROM sec.filing_metadata m
JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code
WHERE m.cik IN ('0000320193', '0000789019')
LIMIT 5;

-- 4.2 SEC + ECON: Company Performance vs Economy
SELECT 'SEC-ECON Join' as test_name,
       f."year",
       m.company_name,
       f."value" as net_income,
       e."value" as unemployment_rate
FROM sec.financial_line_items f
JOIN sec.filing_metadata m ON f.cik = m.cik 
    AND f.filing_type = m.filing_type 
    AND f."year" = m."year"
JOIN econ.employment_statistics e ON f."year" = EXTRACT(YEAR FROM e."date")
WHERE f.concept = 'NetIncomeLoss'
    AND f.cik IN ('0000320193', '0000789019')
    AND e.series_id = 'UNRATE'
    AND EXTRACT(MONTH FROM e."date") = 12
LIMIT 5;

-- 4.3 ECON + GEO: Regional Economic Data
SELECT 'ECON-GEO Join' as test_name,
       s.state_name,
       s.state_code,
       re.unemployment_rate,
       re."date"
FROM geo.tiger_states s
LEFT JOIN econ.regional_employment re ON s.state_code = re.state_code
WHERE s.state_code IN ('CA', 'NY')
    AND re.area_type = 'state'
ORDER BY re."date" DESC
LIMIT 5;

-- 4.4 Triple Join: SEC + ECON + GEO
SELECT 'Triple Schema Join' as test_name,
       m.company_name,
       s.state_name as incorporated_in,
       f."value" as revenue_millions,
       e."value" as national_unemployment
FROM sec.filing_metadata m
JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code
JOIN sec.financial_line_items f ON m.cik = f.cik 
    AND m."year" = f."year" 
    AND m.filing_type = f.filing_type
JOIN econ.employment_statistics e ON f."year" = EXTRACT(YEAR FROM e."date")
WHERE f.concept = 'Revenues'
    AND m.cik IN ('0000320193', '0000789019')
    AND e.series_id = 'UNRATE'
    AND EXTRACT(MONTH FROM e."date") = 12
LIMIT 5;

-- ======================================================================
-- SECTION 5: CONSTRAINT VALIDATION
-- ======================================================================

-- 5.1 Primary Key Uniqueness
SELECT 'SEC PK Check' as test_name,
       COUNT(*) as total_rows,
       COUNT(DISTINCT (cik || '|' || filing_type || '|' || "year" || '|' || accession_number)) as unique_keys
FROM sec.filing_metadata;

SELECT 'ECON PK Check' as test_name,
       COUNT(*) as total_rows,
       COUNT(DISTINCT ("date" || '|' || series_id)) as unique_keys
FROM econ.employment_statistics
LIMIT 100;

-- 5.2 Foreign Key Integrity
SELECT 'SEC-GEO FK Check' as test_name,
       COUNT(*) as total_companies,
       COUNT(s.state_code) as valid_states,
       COUNT(*) - COUNT(s.state_code) as orphan_records
FROM sec.filing_metadata m
LEFT JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code;

-- ======================================================================
-- SECTION 6: AGGREGATION QUERIES
-- ======================================================================

-- 6.1 SEC Aggregations
SELECT 'SEC Revenue by Company' as test_name,
       m.company_name,
       f."year",
       SUM(f."value") as total_revenue
FROM sec.financial_line_items f
JOIN sec.filing_metadata m ON f.cik = m.cik
WHERE f.concept = 'Revenues'
    AND f.cik IN ('0000320193', '0000789019')
GROUP BY m.company_name, f."year"
ORDER BY f."year" DESC
LIMIT 5;

-- 6.2 ECON Aggregations  
SELECT 'ECON Avg Unemployment by Year' as test_name,
       EXTRACT(YEAR FROM "date") as "year",
       AVG("value") as avg_unemployment
FROM econ.employment_statistics
WHERE series_id = 'UNRATE'
GROUP BY EXTRACT(YEAR FROM "date")
ORDER BY "year" DESC
LIMIT 5;

-- 6.3 GEO Aggregations
SELECT 'GEO Counties per State' as test_name,
       s.state_name,
       COUNT(c.county_fips) as county_count
FROM geo.tiger_states s
LEFT JOIN geo.tiger_counties c ON s.state_fips = c.state_fips
WHERE s.state_code IN ('CA', 'NY')
GROUP BY s.state_name;

-- ======================================================================
-- VALIDATION SUMMARY
-- ======================================================================
SELECT 'FINAL VALIDATION' as test_name,
    (SELECT COUNT(*) FROM sec.filing_metadata) as sec_filings,
    (SELECT COUNT(*) FROM econ.employment_statistics LIMIT 1000) as econ_records,
    (SELECT COUNT(*) FROM geo.tiger_states) as geo_states;