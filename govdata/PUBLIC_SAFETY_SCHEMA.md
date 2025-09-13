# Public Safety Schema Design

## Overview

Create a new `safety` (public safety) schema within the govdata adapter to provide SQL access to U.S. public safety data including crime statistics, emergency services, traffic safety, natural disasters, and fire incidents from multiple government data sources.

## Data Sources

### FBI Crime Data (Primary)
1. **FBI Crime Data Explorer API v2** (Free, public access)
   - National Incident-Based Reporting System (NIBRS) - detailed incident data
   - Uniform Crime Reporting (UCR) - summary statistics
   - Hate Crime Statistics
   - Law Enforcement Officers Killed and Assaulted (LEOKA)
   - National Use-of-Force Data Collection
   - API Key optional but recommended for higher rate limits
   - Rate limit: 1000 requests/hour with key, 100 without

2. **FBI Crime Data Series**
   - Property crime (burglary, theft, motor vehicle theft, arson)
   - Violent crime (murder, rape, robbery, aggravated assault)
   - Arrests by age, sex, race, and ethnicity
   - Clearance rates by offense type
   - Law enforcement officer employment data

### National Highway Traffic Safety Administration (NHTSA)
1. **FARS API** (Fatality Analysis Reporting System) - Free, public
   - Fatal traffic crash data with 100+ variables per incident
   - Vehicle, driver, passenger, and environmental factors
   - Geographic coordinates and road characteristics
   - Drug and alcohol involvement
   - Rate limit: 1000 requests/hour

2. **NCSA Data** (National Center for Statistics and Analysis)
   - Traffic safety statistics and trends
   - Vehicle recalls and safety defects
   - Crash test ratings and vehicle safety data

### Federal Emergency Management Agency (FEMA)
1. **OpenFEMA API** (Free, public access)
   - Presidential disaster declarations
   - Public Assistance funded projects
   - Individual Assistance program data
   - Hazard Mitigation Grant Program
   - Rate limit: 1000 requests/minute

2. **National Flood Insurance Program**
   - Flood insurance claims data
   - Community flood risk assessments

### Additional Sources
1. **Bureau of Alcohol, Tobacco, Firearms and Explosives (ATF)**
   - Federal firearms licenses and trace data
   - Arson and explosives incidents
   - Firearms commerce statistics

2. **CDC WISQARS** (Web-based Injury Statistics Query and Reporting System)
   - Injury mortality and morbidity data
   - Cost of injuries and violence
   - Surveillance data

3. **National Fire Protection Association (NFPA)**
   - National Fire Incident Reporting System (NFIRS) data
   - Fire loss statistics
   - Firefighter injury and fatality data

4. **Local Data Portals**
   - City crime incident data via Socrata/CKAN APIs
   - 911 call data and emergency response metrics
   - Police stops, searches, and traffic enforcement

## Proposed Tables

### Core Crime Statistics

1. **`crime_incidents`** - NIBRS incident-level data
   - Columns: incident_id, agency_ori, incident_date, incident_hour, offense_code, offense_name, crime_against, location_type, cleared_flag, victim_count, offender_count, latitude, longitude, county_fips, state_code, weapon_force_involved, premise_type
   - Primary key: (incident_id, agency_ori)
   - Partitioned by: year, month, state_code
   - Volume: ~50M records annually from 10,000+ agencies
   - Includes detailed offense circumstances and victim/offender relationships

2. **`crime_summary`** - UCR summary statistics by agency/month
   - Columns: agency_ori, agency_name, state_code, county_fips, population_served, year, month, violent_crime, murder, rape_legacy, rape_revised, robbery, aggravated_assault, property_crime, burglary, larceny_theft, motor_vehicle_theft, arson, clearance_rate_violent, clearance_rate_property
   - Primary key: (agency_ori, year, month)
   - Partitioned by: year, state_code
   - Historical data available back to 1960

3. **`arrests`** - Arrest statistics by offense and demographics
   - Columns: agency_ori, year, month, offense_code, offense_category, age_group, sex, race, ethnicity, arrest_count, juvenile_flag, adult_flag
   - Primary key: (agency_ori, year, month, offense_code, age_group, sex, race, ethnicity)
   - Detailed demographic breakdown by detailed offense categories

4. **`hate_crimes`** - Bias-motivated criminal incidents
   - Columns: incident_id, incident_date, agency_ori, bias_motivation, bias_category, offense_type, victim_type, victim_count, offender_race, offender_ethnicity, location_type, state_code, county_fips
   - Primary key: incident_id
   - Tracks 30+ bias categories and 8 victim types

### Law Enforcement Data

5. **`law_enforcement_agencies`** - Agency reference information
   - Columns: agency_ori, agency_name, agency_type, state_code, county_fips, city_name, population_served, officers_total, officers_per_1000_population, civilian_employees, total_employees, agency_latitude, agency_longitude, nibrs_participant, ucr_participant
   - Primary key: agency_ori
   - Links to all crime statistics and enforcement data

6. **`officer_involved_incidents`** - Use of force and officer safety
   - Columns: incident_id, incident_date, agency_ori, incident_type, use_of_force_type, subject_injury_level, officer_injury_level, subject_armed_with, subject_resistance, officer_years_service, officer_assignment, disposition, investigation_finding
   - Primary key: incident_id
   - Includes police shootings, injuries, and assaults on officers

7. **`police_employment`** - Historical law enforcement staffing
   - Columns: agency_ori, year, sworn_officers_total, sworn_officers_male, sworn_officers_female, sworn_officers_by_race, civilian_staff_total, civilian_staff_male, civilian_staff_female, total_budget_dollars, officers_per_1000_pop
   - Primary key: (agency_ori, year)
   - Annual staffing and budget data

### Traffic Safety

8. **`traffic_fatalities`** - FARS fatal crash incident data
   - Columns: crash_id, crash_date, crash_time, day_of_week, state_code, county_fips, city_name, latitude, longitude, weather_condition, light_condition, road_surface, first_harmful_event, fatality_count, vehicle_count, drunk_driver_involved, speeding_involved, distracted_driving, roadway_function_class
   - Primary key: crash_id
   - Partitioned by: year, state_code
   - Detailed crash circumstances and contributing factors

9. **`traffic_violations`** - Citation and traffic enforcement
   - Columns: citation_id, citation_date, citation_time, agency_ori, violation_type, violation_code, vehicle_type, driver_age, driver_sex, driver_race, driver_ethnicity, fine_amount, court_disposition, stop_reason, search_conducted
   - Primary key: citation_id
   - Available from state/local sources, varies by jurisdiction

10. **`vehicle_crashes`** - Non-fatal crash summary statistics
    - Columns: state_code, county_fips, year, month, total_crashes, fatal_crashes, injury_crashes, property_damage_only, vehicles_involved, persons_killed, persons_injured, economic_cost_millions
    - Primary key: (state_code, county_fips, year, month)
    - State-reported crash summaries

### Emergency Services

11. **`emergency_calls`** - 911 call volumes and response metrics
    - Columns: call_id, call_date, call_time, call_type_code, call_type_description, priority_level, agency_ori, response_time_seconds, on_scene_time_seconds, incident_closed_time, disposition_code, latitude, longitude, beat_district
    - Primary key: call_id
    - Partitioned by: year, month, agency_ori
    - From local data portals where available

12. **`fire_incidents`** - Fire department emergency responses
    - Columns: incident_id, incident_date, incident_time, agency_id, incident_type, alarm_time, arrival_time, controlled_time, last_unit_cleared_time, property_use, ignition_source, fire_cause, civilian_deaths, civilian_injuries, fire_service_deaths, fire_service_injuries, property_loss_dollars, contents_loss_dollars
    - Primary key: incident_id
    - NFIRS (National Fire Incident Reporting System) data

13. **`ems_incidents`** - Emergency medical service responses
    - Columns: incident_id, incident_date, incident_time, agency_id, call_type, patient_age, patient_sex, injury_type, illness_type, transport_flag, receiving_hospital, response_time_seconds, scene_time_minutes, transport_time_minutes
    - Primary key: incident_id
    - Where available from local EMS systems

### Natural Disasters and Hazards

14. **`disaster_declarations`** - Presidential and major disaster declarations
    - Columns: disaster_number, declaration_date, declaration_title, incident_type, disaster_type, state_code, county_fips, declaration_type, incident_begin_date, incident_end_date, close_out_date, federal_share_obligated, total_amount_eligible, damage_category_code
    - Primary key: (disaster_number, county_fips)
    - Links to FEMA assistance and mitigation programs

15. **`public_assistance_projects`** - FEMA disaster recovery funding
    - Columns: project_id, disaster_number, state_code, county_fips, applicant_name, damage_category, project_amount, federal_share, applicant_share, project_description, dcc_code, completion_date
    - Primary key: project_id
    - Infrastructure repair and replacement projects

16. **`hazard_mitigation_projects`** - Risk reduction and preparedness
    - Columns: project_id, disaster_number, state_code, county_fips, project_type, hazard_type, project_cost, benefit_cost_ratio, properties_protected, lives_saved, project_status, completion_date
    - Primary key: project_id
    - Pre-disaster mitigation and post-disaster risk reduction

### Composite and Analytics Tables

17. **`public_safety_index`** - Pre-calculated safety metrics and rankings
    - Columns: area_code, area_type, area_name, state_code, year, month, population, violent_crime_rate_per_100k, property_crime_rate_per_100k, traffic_fatality_rate_per_100k, disaster_risk_score, fire_risk_score, overall_safety_score, safety_rank_in_state, safety_rank_national, crime_trend_12month
    - Primary key: (area_code, area_type, year, month)
    - Composite scores for counties, cities, metro areas

18. **`crime_trends`** - Year-over-year crime pattern analysis
    - Columns: area_code, area_type, year, crime_category, current_rate_per_100k, prior_year_rate, three_year_avg_rate, five_year_avg_rate, percent_change_yoy, percent_change_5year, trend_direction, volatility_index
    - Primary key: (area_code, area_type, year, crime_category)
    - Statistical trend analysis with confidence intervals

## Cross-Schema Relationships

### Links to GEO Schema
- All incident tables use county_fips and state_code from geographic reference tables
- Spatial joins with census tracts, zip code areas, and metropolitan boundaries
- Crime density analysis by geographic subdivisions
- Regional aggregations and comparative analysis

### Links to ECON Schema
- Crime rates correlated with unemployment, poverty, and economic indicators
- Economic impact assessment of disasters and emergency events
- Public safety spending analysis vs. economic development investment
- Tourism and business impact of crime and safety perceptions

### Links to SEC Schema
- Business location risk assessments for corporate facility planning
- Retail shrinkage and security cost analysis impacting company earnings
- Disaster business interruption and insurance claim impacts
- Supply chain disruption risk from transportation safety data

### Links to CENSUS Schema (future)
- Per capita crime rate calculations using demographic denominators
- Socioeconomic correlation analysis with crime and safety patterns
- Educational attainment vs. crime involvement statistics
- Income inequality impact on property and violent crime rates

## Implementation Approach

### Phase 1: Core Crime Data Foundation (Months 1-3)
1. Set up FBI Crime Data Explorer API integration with rate limiting and caching
2. Implement crime_incidents table with NIBRS detailed incident data
3. Create crime_summary table with UCR aggregated statistics
4. Add arrests table with demographic breakdowns
5. Build law_enforcement_agencies reference table
6. Create hate_crimes specialized table

### Phase 2: Traffic Safety and Emergency Services (Months 4-5)
1. Integrate NHTSA FARS API for comprehensive traffic fatality data
2. Add traffic_fatalities table with crash details and contributing factors
3. Implement fire_incidents table using NFIRS data feeds
4. Create emergency_calls table framework for local data integration
5. Build vehicle_crashes summary statistics table

### Phase 3: Disasters and Officer Safety (Months 6-7)
1. Connect to OpenFEMA API for disaster declarations and assistance data
2. Add disaster_declarations and public_assistance_projects tables
3. Implement hazard_mitigation_projects for risk reduction tracking
4. Create officer_involved_incidents for use-of-force transparency
5. Add police_employment for staffing and budget analysis

### Phase 4: Analytics and Local Integration (Months 8-12)
1. Build public_safety_index with pre-calculated composite scores
2. Create crime_trends table with statistical trend analysis
3. Develop framework for city-specific data portal integration
4. Add Socrata/CKAN API adapters for local government data
5. Implement traffic_violations and ems_incidents where data available
6. Create materialized views for common analytical queries

## Query Examples

### Public Safety Risk Assessment
```sql
-- Comprehensive safety analysis for business location
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
WHERE s.area_type = 'county' 
  AND s.year = 2023
  AND g.state_code IN ('CA', 'TX', 'FL', 'NY')
ORDER BY s.overall_safety_score DESC;
```

### Crime Pattern Analysis
```sql
-- Identify crime trends and hotspots
SELECT 
    l.agency_name,
    l.state_code,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN c.offense_name LIKE '%THEFT%' THEN 1 END) as theft_incidents,
    COUNT(CASE WHEN c.offense_name LIKE '%ASSAULT%' THEN 1 END) as assault_incidents,
    AVG(CASE WHEN c.cleared_flag = 'Y' THEN 1.0 ELSE 0.0 END) as clearance_rate,
    EXTRACT(HOUR FROM c.incident_hour) as peak_hour
FROM safety.crime_incidents c
JOIN safety.law_enforcement_agencies l ON c.agency_ori = l.agency_ori
WHERE c.incident_date >= '2023-01-01'
  AND c.incident_date <= '2023-12-31'
  AND l.population_served > 50000
GROUP BY l.agency_ori, l.agency_name, l.state_code
HAVING COUNT(*) > 1000
ORDER BY total_incidents DESC
LIMIT 20;
```

### Disaster Impact Assessment
```sql
-- Analyze disaster frequency and costs by region
SELECT 
    g.state_name,
    COUNT(DISTINCT d.disaster_number) as disaster_count,
    STRING_AGG(DISTINCT d.incident_type, ', ') as disaster_types,
    SUM(d.federal_share_obligated) / 1000000 as federal_cost_millions,
    SUM(p.project_amount) / 1000000 as total_projects_millions,
    AVG(m.benefit_cost_ratio) as avg_benefit_cost_ratio
FROM safety.disaster_declarations d
JOIN geo.counties g ON d.county_fips = g.county_fips
LEFT JOIN safety.public_assistance_projects p ON d.disaster_number = p.disaster_number
LEFT JOIN safety.hazard_mitigation_projects m ON d.disaster_number = m.disaster_number
WHERE d.declaration_date >= '2020-01-01'
GROUP BY g.state_code, g.state_name
HAVING disaster_count >= 5
ORDER BY federal_cost_millions DESC;
```

### Traffic Safety Corridor Analysis
```sql
-- Identify high-risk traffic corridors for transportation planning
SELECT 
    t.county_fips,
    g.county_name,
    g.state_name,
    COUNT(*) as fatal_crashes,
    SUM(t.fatality_count) as total_fatalities,
    COUNT(CASE WHEN t.drunk_driver_involved = 'Y' THEN 1 END) as alcohol_involved,
    COUNT(CASE WHEN t.speeding_involved = 'Y' THEN 1 END) as speed_involved,
    COUNT(CASE WHEN t.distracted_driving = 'Y' THEN 1 END) as distraction_involved,
    ROUND(AVG(t.fatality_count), 2) as avg_fatalities_per_crash
FROM safety.traffic_fatalities t
JOIN geo.counties g ON t.county_fips = g.county_fips
WHERE t.crash_date >= '2022-01-01'
  AND t.crash_date <= '2023-12-31'
GROUP BY t.county_fips, g.county_name, g.state_name
HAVING fatal_crashes >= 50
ORDER BY total_fatalities DESC
LIMIT 25;
```

### Cross-Domain Business Risk Analysis
```sql
-- Comprehensive business location risk combining safety, economic, and demographic factors
SELECT 
    c.company_name,
    c.facility_city,
    c.facility_state,
    s.violent_crime_rate_per_100k,
    s.property_crime_rate_per_100k,
    d.disaster_count_5year,
    t.traffic_fatality_rate_per_100k,
    e.unemployment_rate,
    e.median_household_income,
    census.poverty_rate,
    CASE 
        WHEN s.overall_safety_score >= 7.5 AND e.unemployment_rate <= 5.0 THEN 'PREFERRED'
        WHEN s.overall_safety_score >= 6.0 AND e.unemployment_rate <= 7.5 THEN 'ACCEPTABLE'
        ELSE 'HIGH_RISK'
    END as location_recommendation
FROM sec.company_facilities c
JOIN safety.public_safety_index s ON c.county_fips = s.area_code
JOIN (
    SELECT county_fips, COUNT(*) as disaster_count_5year
    FROM safety.disaster_declarations 
    WHERE declaration_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY county_fips
) d ON c.county_fips = d.county_fips
JOIN safety.traffic_fatalities t ON c.county_fips = t.county_fips
JOIN econ.regional_employment e ON c.county_fips = e.area_code
JOIN census.demographic_profile census ON c.county_fips = census.county_fips
WHERE c.cik IN ('0000320193', '0000789019', '0001018724')  -- Apple, Microsoft, Amazon
  AND s.year = 2023
ORDER BY s.overall_safety_score DESC, e.unemployment_rate;
```

## Performance Considerations

### Data Volume Estimates
- **Crime incidents**: ~100GB (detailed NIBRS data 2016-present, growing 15GB annually)
- **Traffic fatalities**: ~25GB (FARS detailed crash data 1975-present)
- **Emergency calls**: ~200GB (major city 911 data where available)
- **Disaster data**: ~15GB (FEMA declarations and projects 1950-present)
- **Fire incidents**: ~50GB (NFIRS data 1999-present)
- **Law enforcement**: ~10GB (agency and employment metadata)

### Optimization Strategies
1. **Hierarchical aggregation**: Pre-compute statistics at city, county, state, and national levels
2. **Time-based partitioning**: Partition large tables by year and month for query performance
3. **Spatial indexing**: R-tree and PostGIS-style spatial indices for location-based queries
4. **Materialized views**: Pre-calculate common metrics like crime rates, clearance rates, trends
5. **Incremental updates**: Delta processing for new incidents and revised statistics
6. **Hot/cold storage**: Recent data in memory, historical data in compressed columnar format
7. **Smart caching**: Cache frequently accessed aggregations and geographic joins

### Query Performance Optimization
- Use covering indices on (agency_ori, incident_date, offense_code) for crime queries
- Spatial indices on (latitude, longitude) for geographic analysis
- Composite indices on (area_code, area_type, year) for trend analysis
- Partition elimination for date-range queries
- Push down predicates to storage layer for large scans

## Update Frequency and Data Freshness

- **FBI Crime Data**: Monthly updates (typically 3-month lag for incident data, 1-year for summary)
- **Traffic Fatalities**: Annual FARS data release (6-month lag), quarterly preliminary estimates
- **Disaster Declarations**: Real-time during active events, daily batch updates for project data
- **Emergency Calls**: Daily updates where municipal APIs available
- **Fire Incidents**: Monthly NFIRS submissions (varies by jurisdiction)
- **Law Enforcement**: Annual staffing and employment updates
- **Local Data**: Daily to weekly depending on city data portal refresh schedules

## API Configuration

```json
{
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
      "enabledSources": ["fbi", "nhtsa", "fema", "atf", "cdc"],
      "localDataPortals": {
        "chicago": {"endpoint": "data.cityofchicago.org", "apiKey": "${CHICAGO_API_KEY}"},
        "nyc": {"endpoint": "data.cityofnewyork.us", "apiKey": "${NYC_OPEN_DATA_KEY}"},
        "seattle": {"endpoint": "data.seattle.gov"}
      },
      "spatialAnalysis": {
        "enabled": true,
        "radiusAnalysis": ["0.5mi", "1mi", "5mi"],
        "bufferAnalysis": true
      },
      "cacheDirectory": "${SAFETY_CACHE_DIR:/tmp/safety-cache}",
      "compressionLevel": "high"
    }
  }]
}
```

## Dependencies and Integration Requirements

### External Libraries
- FBI Crime Data Explorer API client
- NHTSA FARS API integration
- OpenFEMA API client  
- Socrata/CKAN API adapters for local data
- Spatial analysis libraries (JTS, GeoTools)
- Time series analysis for trend calculation

### Data Processing Pipeline
- ETL framework for multi-source data harmonization  
- Geocoding services for address standardization
- Statistical libraries for trend analysis and scoring
- Caching layer with geographic indexing
- Data quality validation and anomaly detection

## Security and Compliance

### Data Access and Privacy
- All data sources are public government information
- No personally identifiable information (PII) in aggregated datasets
- Individual incident data may require additional privacy protections
- Compliance with agency terms of service and rate limiting requirements

### Rate Limiting and Ethical Use
- Respect FBI, NHTSA, and FEMA API rate limits
- Implement exponential backoff for failed requests
- Cache frequently accessed data to minimize API calls
- Proper attribution required for all data sources
- No commercial resale without agency permission

## Future Enhancements

### Advanced Analytics
1. **Predictive Crime Mapping**: Machine learning models for crime risk forecasting
2. **Emergency Response Optimization**: Response time analysis and resource allocation
3. **Disaster Preparedness Scoring**: Community resilience and vulnerability assessment
4. **Traffic Safety Interventions**: Identify high-risk locations for infrastructure improvement

### Enhanced Data Sources
1. **Court and Justice System**: Case outcomes, sentencing data, recidivism tracking
2. **Corrections Data**: Prison populations, parole/probation statistics
3. **Emergency Management**: Shelter capacity, evacuation route planning
4. **Public Health Integration**: Injury surveillance, substance abuse treatment data

### Real-Time Capabilities  
1. **Streaming Crime Data**: Real-time incident feeds from participating agencies
2. **Emergency Alert Integration**: Active disaster monitoring and business impact alerts
3. **Traffic Incident Feeds**: Real-time crash data for supply chain disruption analysis
4. **Social Media Sentiment**: Public safety perception analysis

This comprehensive public safety schema creates a powerful analytical foundation for risk assessment, emergency planning, and evidence-based public policy decisions while maintaining strong cross-domain relationships with financial, economic, and geographic data sources.