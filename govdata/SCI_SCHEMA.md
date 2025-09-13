# SCI Schema Implementation Plan

## Overview
Create a new `sci` (science/research/health) schema within the govdata adapter to provide SQL access to federal science, research, and health data from agencies like FDA, CDC, NIH, EPA, and NOAA.

## Data Sources & Tables

### Primary Data Sources

#### FDA (Food and Drug Administration)
1. **FDA OpenFDA API** (Free, Government)
   - Drug approvals, recalls, adverse events
   - Medical device data, food enforcement reports
   - JSON REST API with no authentication required

2. **FDA Orange Book** (Free, Government)
   - Approved drug products with therapeutic equivalence evaluations
   - Generic drug approvals and patent information
   - Downloadable datasets in various formats

#### CDC (Centers for Disease Control and Prevention)
3. **CDC Wonder** (Free, Government)
   - Mortality data, birth data, cancer incidence
   - Disease surveillance data
   - Web-based query system with API access

4. **CDC Data API** (Free, Government)  
   - Chronic disease indicators, behavioral risk factors
   - Vaccination data, disease outbreak information
   - JSON/XML formats

#### NIH (National Institutes of Health)
5. **NIH RePORTER API** (Free, Government)
   - Research grants, publications, clinical trials
   - Principal investigator information
   - JSON REST API

6. **ClinicalTrials.gov API** (Free, Government)
   - Clinical trial registrations and results
   - Study protocols, outcomes, recruitment status
   - XML/JSON formats

#### EPA (Environmental Protection Agency)
7. **EPA APIs** (Free, Government)
   - Air quality data, water quality monitoring
   - Toxic release inventory, superfund sites
   - Multiple APIs with JSON/XML support

#### NOAA (National Oceanic and Atmospheric Administration)
8. **NOAA Climate Data API** (Free, Government)
   - Weather observations, climate normals
   - Historical weather data, forecasts
   - JSON REST API

## Proposed Table Schema

### FDA Tables
1. **`fda_drug_approvals`** - FDA approved drugs and biologics
   - Columns: application_number, product_name, active_ingredient, approval_date, company_name, indication, dosage_form, route, strength

2. **`fda_adverse_events`** - Drug adverse event reports
   - Columns: report_id, drug_name, reaction, outcome, patient_age, patient_sex, report_date, serious

3. **`fda_recalls`** - Drug and food recalls
   - Columns: recall_number, product_description, reason, classification, recall_date, company_name, status

4. **`fda_medical_devices`** - Medical device approvals and clearances
   - Columns: device_name, classification, approval_type, approval_date, company_name, indication

### CDC Tables
5. **`cdc_mortality`** - Mortality statistics
   - Columns: year, state, county, cause_of_death, age_group, gender, race, death_count, population

6. **`cdc_disease_surveillance`** - Notifiable disease data
   - Columns: disease, state, county, report_date, case_count, incidence_rate, year

7. **`cdc_vaccination_rates`** - Vaccination coverage data
   - Columns: vaccine, state, county, age_group, coverage_percentage, year, survey_year

### NIH Tables
8. **`nih_research_grants`** - Research grant awards
   - Columns: grant_id, project_title, principal_investigator, institution, award_amount, start_date, end_date, agency, activity_code

9. **`clinical_trials`** - Clinical trial information
   - Columns: nct_id, title, phase, status, condition, intervention, sponsor, enrollment, start_date, completion_date

10. **`nih_publications`** - Research publications from grants
    - Columns: pmid, title, authors, journal, publication_date, grant_ids, citation_count

### EPA Tables
11. **`epa_air_quality`** - Air quality monitoring data
    - Columns: site_id, state, county, parameter, date, value, units, aqi, measurement_method

12. **`epa_superfund_sites`** - Superfund contaminated sites
    - Columns: site_id, site_name, address, city, state, zip, contaminants, status, listing_date

13. **`epa_toxic_releases`** - Toxic Release Inventory data
    - Columns: facility_name, chemical, industry, state, county, release_amount, year, media

### NOAA Tables
14. **`noaa_weather_observations`** - Weather station data
    - Columns: station_id, date, temperature_max, temperature_min, precipitation, snow, wind_speed, location

15. **`noaa_climate_data`** - Climate normals and extremes
    - Columns: station_id, parameter, period, normal_value, record_value, record_date, location

## Implementation Steps

### Phase 1: FDA Core Data
1. Create SCI schema factory class extending existing pattern
2. Implement FDA OpenFDA API data providers
3. Create Parquet conversion pipeline for drug and device data  
4. Implement core tables: fda_drug_approvals, fda_adverse_events, fda_recalls
5. Add constraint metadata for relationships between tables
6. Create model files and integration tests

### Phase 2: CDC Health Data
1. Add CDC Wonder and Data API integration
2. Implement mortality, disease surveillance, and vaccination tables
3. Create geographic linkages to GEO schema data
4. Add health outcome analysis capabilities

### Phase 3: NIH Research Data
1. Add NIH RePORTER and ClinicalTrials.gov APIs
2. Implement research grants and publications tables
3. Create research funding and outcome tracking
4. Link clinical trials to drug approvals

### Phase 4: EPA Environmental Data
1. Add EPA air quality and toxic release APIs
2. Implement environmental monitoring tables
3. Create geographic correlation with health outcomes
4. Add superfund site tracking

### Phase 5: NOAA Climate Data
1. Add NOAA Climate Data API integration
2. Implement weather and climate tables
3. Enable climate-health correlation analysis
4. Create long-term trend analysis capabilities

## Technical Architecture
- Follow existing govdata pattern with dedicated `sci/` package
- Implement `SciSchemaFactory` similar to `SecSchemaFactory` and `GeoSchemaFactory`
- Use same caching and Parquet conversion strategies as other schemas
- Support same execution engines (DuckDB, Parquet, etc.)
- Maintain constraint metadata for query optimization
- Handle large time-series datasets efficiently

## Data Refresh Strategy
- **FDA data**: Daily updates for adverse events, weekly for approvals
- **CDC data**: Weekly updates for surveillance, monthly for statistics  
- **NIH data**: Weekly updates for grants, monthly for publications
- **EPA data**: Daily for air quality, quarterly for toxic releases
- **NOAA data**: Daily for weather, annual for climate normals

## Example Queries

### Cross-Schema Analysis
```sql
-- Drug approvals vs adverse events by geographic region
SELECT 
    fda.fda_drug_approvals.product_name,
    COUNT(fda.fda_adverse_events.report_id) as adverse_event_count,
    geo.income_limits.state_alpha,
    geo.income_limits.median_income
FROM sci.fda_drug_approvals fda
LEFT JOIN sci.fda_adverse_events ae ON fda.product_name = ae.drug_name
JOIN geo.income_limits geo ON fda.company_state = geo.state_alpha
WHERE fda.approval_date >= '2020-01-01'
GROUP BY fda.product_name, geo.state_alpha, geo.median_income
ORDER BY adverse_event_count DESC;

-- Research funding vs health outcomes by congressional district  
SELECT 
    pol.congress_members.full_name,
    pol.congress_members.state,
    SUM(sci.nih_research_grants.award_amount) as total_funding,
    AVG(sci.cdc_mortality.death_count) as avg_mortality
FROM pol.congress_members pol
JOIN sci.nih_research_grants nih ON pol.state = nih.institution_state
JOIN sci.cdc_mortality cdc ON pol.state = cdc.state
WHERE pol.chamber = 'house' AND pol.current = true
GROUP BY pol.full_name, pol.state
ORDER BY total_funding DESC;

-- Environmental factors vs health outcomes
SELECT 
    sci.epa_air_quality.state,
    sci.epa_air_quality.county,
    AVG(sci.epa_air_quality.aqi) as avg_air_quality,
    AVG(sci.cdc_mortality.death_count) as avg_mortality,
    sci.noaa_weather_observations.temperature_max
FROM sci.epa_air_quality
JOIN sci.cdc_mortality ON sci.epa_air_quality.state = sci.cdc_mortality.state 
    AND sci.epa_air_quality.county = sci.cdc_mortality.county
JOIN sci.noaa_weather_observations ON sci.epa_air_quality.date = sci.noaa_weather_observations.date
WHERE sci.epa_air_quality.date >= '2023-01-01'
GROUP BY sci.epa_air_quality.state, sci.epa_air_quality.county
ORDER BY avg_air_quality DESC;
```

## Storage Estimates
- **FDA tables**: ~2GB (drug approvals, adverse events, recalls, devices)
- **CDC tables**: ~5GB (mortality, disease surveillance, vaccination data)
- **NIH tables**: ~1GB (grants, trials, publications)
- **EPA tables**: ~3GB (air quality, superfund, toxic releases)
- **NOAA tables**: ~4GB (weather observations, climate data)

**Total Phase 1-5**: ~15GB compressed Parquet

## Data Quality Considerations
- Handle missing values and data inconsistencies across agencies
- Standardize geographic identifiers (FIPS codes, state abbreviations)
- Validate date formats and ranges across different data sources
- Implement data lineage tracking for audit purposes
- Create data quality metrics and monitoring

## Privacy and Compliance
- Ensure PHI (Protected Health Information) compliance for health data
- Implement appropriate data masking for sensitive information
- Follow FDA, CDC, and other agency data use guidelines
- Document data source restrictions and usage requirements

This creates a comprehensive science/research/health schema that enables powerful analysis of public health, environmental factors, research funding, and health outcomes across federal data sources.