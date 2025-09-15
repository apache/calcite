-- ======================================================================
-- COMPREHENSIVE GOVERNMENT DATA VALIDATION SCRIPT
-- ======================================================================
-- This script validates all schemas, tables, data presence, and relationships
-- Run with: jdbc:calcite:model=govdata-simple-test-model.json

-- ======================================================================
-- PHASE 1: VALIDATE TABLE EXISTENCE
-- ======================================================================

-- SEC Schema Tables (should return 7+ tables)
SELECT 'SEC TABLE CHECK' as phase, COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'SEC';

-- List SEC tables
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'SEC' 
ORDER BY table_name;

-- ECON Schema Tables (should return 10 tables)
SELECT 'ECON TABLE CHECK' as phase, COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'ECON';

-- List ECON tables  
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'ECON' 
ORDER BY table_name;

-- GEO Schema Tables (should return 6+ tables)
SELECT 'GEO TABLE CHECK' as phase, COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'GEO';

-- List GEO tables
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'GEO' 
ORDER BY table_name;

-- ======================================================================
-- PHASE 2: VALIDATE DATA PRESENCE
-- ======================================================================

-- Check SEC core tables have data
SELECT 'SEC.FILING_METADATA' as table_name, COUNT(*) as row_count 
FROM sec.filing_metadata;

SELECT 'SEC.FINANCIAL_LINE_ITEMS' as table_name, COUNT(*) as row_count 
FROM sec.financial_line_items 
LIMIT 1;

-- Check ECON core tables have data
SELECT 'ECON.EMPLOYMENT_STATISTICS' as table_name, COUNT(*) as row_count 
FROM econ.employment_statistics 
LIMIT 1;

SELECT 'ECON.TREASURY_YIELDS' as table_name, COUNT(*) as row_count 
FROM econ.treasury_yields 
LIMIT 1;

SELECT 'ECON.FRED_INDICATORS' as table_name, COUNT(*) as row_count 
FROM econ.fred_indicators 
LIMIT 1;

-- Check GEO core tables have data (should always have states)
SELECT 'GEO.TIGER_STATES' as table_name, COUNT(*) as row_count 
FROM geo.tiger_states;

SELECT 'GEO.TIGER_COUNTIES' as table_name, COUNT(*) as row_count 
FROM geo.tiger_counties 
LIMIT 1;

-- ======================================================================
-- PHASE 3: VALIDATE QUERYABILITY (Sample queries from each table)
-- ======================================================================

-- SEC: Get sample company data
SELECT cik, company_name, state_of_incorporation 
FROM sec.filing_metadata 
LIMIT 5;

-- SEC: Get sample financial data
SELECT cik, concept, value, unit 
FROM sec.financial_line_items 
WHERE concept = 'NetIncomeLoss' 
LIMIT 5;

-- ECON: Get sample employment data
SELECT date, series_id, value, unit 
FROM econ.employment_statistics 
WHERE series_id = 'UNRATE' 
LIMIT 5;

-- ECON: Get sample treasury data
SELECT date, maturity_months, yield_rate 
FROM econ.treasury_yields 
WHERE maturity_months = 120 
LIMIT 5;

-- GEO: Get state data
SELECT state_code, state_name, state_fips 
FROM geo.tiger_states 
WHERE state_code IN ('CA', 'NY', 'TX');

-- ======================================================================
-- PHASE 4: VALIDATE FOREIGN KEY RELATIONSHIPS
-- ======================================================================

-- Test SEC -> GEO relationship (state_of_incorporation -> state_code)
SELECT 
    'SEC->GEO FK TEST' as test_name,
    COUNT(*) as total_companies,
    COUNT(s.state_code) as matched_states,
    COUNT(*) - COUNT(s.state_code) as orphan_records
FROM sec.filing_metadata m
LEFT JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code;

-- Test ECON -> GEO relationship (regional_employment.state_code -> tiger_states.state_code)
SELECT 
    'ECON->GEO FK TEST' as test_name,
    COUNT(*) as total_employment_records,
    COUNT(s.state_code) as matched_states
FROM econ.regional_employment re
LEFT JOIN geo.tiger_states s ON re.state_code = s.state_code
WHERE re.area_type = 'state';

-- Test ECON -> ECON relationship (employment_statistics.series_id -> fred_indicators.series_id)
SELECT 
    'ECON->ECON FK TEST' as test_name,
    COUNT(DISTINCT es.series_id) as employment_series,
    COUNT(DISTINCT fi.series_id) as matching_fred_series
FROM econ.employment_statistics es
LEFT JOIN econ.fred_indicators fi ON es.series_id = fi.series_id;

-- ======================================================================
-- PHASE 5: CROSS-SCHEMA QUERIES
-- ======================================================================

-- Triple schema join: Companies by state with unemployment data
SELECT 
    m.company_name,
    m.state_of_incorporation,
    s.state_name,
    re.unemployment_rate
FROM sec.filing_metadata m
JOIN geo.tiger_states s ON m.state_of_incorporation = s.state_code
JOIN econ.regional_employment re ON s.state_code = re.state_code
WHERE re.area_type = 'state'
LIMIT 5;

-- SEC + ECON: Company revenue vs economic indicators
SELECT 
    f.year,
    m.company_name,
    f.value as revenue,
    e.value as unemployment_rate
FROM sec.financial_line_items f
JOIN sec.filing_metadata m ON f.cik = m.cik 
    AND f.filing_type = m.filing_type 
    AND f.year = m.year
JOIN econ.employment_statistics e ON f.year = EXTRACT(YEAR FROM e.date)
WHERE f.concept = 'Revenues'
    AND e.series_id = 'UNRATE'
    AND EXTRACT(MONTH FROM e.date) = 12
LIMIT 5;

-- GEO + ECON: States with economic data
SELECT 
    s.state_name,
    s.state_code,
    COUNT(DISTINCT re.date) as data_points,
    AVG(re.unemployment_rate) as avg_unemployment
FROM geo.tiger_states s
JOIN econ.regional_employment re ON s.state_code = re.state_code
WHERE re.area_type = 'state'
GROUP BY s.state_name, s.state_code
ORDER BY avg_unemployment DESC
LIMIT 10;

-- ======================================================================
-- PHASE 6: VALIDATE PRIMARY KEYS
-- ======================================================================

-- Test ECON primary key uniqueness
SELECT 
    'EMPLOYMENT_STATISTICS PK' as constraint_test,
    COUNT(*) as total_rows,
    COUNT(DISTINCT (date || '|' || series_id)) as unique_combinations
FROM econ.employment_statistics;

-- Test SEC primary key uniqueness  
SELECT 
    'FILING_METADATA PK' as constraint_test,
    COUNT(*) as total_rows,
    COUNT(DISTINCT (cik || '|' || filing_type || '|' || year || '|' || accession_number)) as unique_combinations
FROM sec.filing_metadata;

-- ======================================================================
-- SUMMARY: Count all validations
-- ======================================================================
SELECT 
    'VALIDATION SUMMARY' as report,
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('SEC', 'ECON', 'GEO')) as total_tables,
    (SELECT COUNT(*) FROM sec.filing_metadata) as sec_filings,
    (SELECT COUNT(*) FROM econ.employment_statistics LIMIT 100) as econ_data_points,
    (SELECT COUNT(*) FROM geo.tiger_states) as geo_states;