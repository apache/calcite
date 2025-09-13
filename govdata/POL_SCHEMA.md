# POL Schema Implementation Plan

## Overview
Create a new `pol` (political) schema within the govdata adapter to provide SQL access to federal elected and appointed officials data.

## Data Sources & Tables

### Primary Data Sources
1. **GitHub unitedstates/congress-legislators** (Free, Public Domain)
   - Current and historical Congress members (1789-Present)
   - Available in CSV, JSON, YAML formats
   - Updated regularly by public commons project

2. **Congress.gov API** (Official Government)
   - Congressional data, committees, leadership
   - Requires free API key registration
   - JSON/XML REST API

3. **Federal Register API** (Official Government)
   - Executive appointments and nominations
   - Daily publication data
   - JSON/CSV format

## Proposed Table Schema

### Core Tables
1. **`congress_members`** - Current and historical Congress members
   - Columns: bioguide_id, first_name, last_name, full_name, state, district, party, chamber (house/senate), terms, birth_date, etc.

2. **`congress_terms`** - Individual terms served
   - Columns: bioguide_id, chamber, state, district, party, start_date, end_date, congress_number

3. **`committees`** - Congressional committees and subcommittees  
   - Columns: committee_id, name, chamber, parent_committee_id, jurisdiction

4. **`committee_memberships`** - Member committee assignments
   - Columns: bioguide_id, committee_id, role (chair, ranking_member, member), start_date, end_date

5. **`leadership_positions`** - Congressional leadership roles
   - Columns: bioguide_id, position (speaker, majority_leader, etc.), chamber, start_date, end_date

6. **`district_offices`** - Congressional district office locations
   - Columns: bioguide_id, office_type, address, phone, city, state, zip

### Future Enhancement Tables
7. **`executive_appointees`** - Federal executive appointments
   - Columns: name, position, agency, appointment_date, confirmation_date, status

8. **`nominations`** - Presidential nominations
   - Columns: nominee_name, position, nominating_president, nomination_date, confirmation_status

## Implementation Steps

### Phase 1: Core Legislative Data
1. Create POL schema factory class extending existing pattern
2. Implement data providers for GitHub congress-legislators repository  
3. Create Parquet conversion pipeline for Congress member data
4. Implement core tables: congress_members, congress_terms, committees
5. Add constraint metadata for relationships between tables
6. Create model files and integration tests

### Phase 2: Extended Legislative Data  
1. Add committee_memberships and leadership_positions tables
2. Implement district_offices table for constituent services
3. Add Congress.gov API integration for real-time updates

### Phase 3: Executive Branch Data
1. Add Federal Register API integration
2. Implement executive_appointees and nominations tables
3. Create cross-schema relationships with GEO congressional districts

## Technical Architecture
- Follow existing govdata pattern with dedicated `pol/` package
- Implement `PolSchemaFactory` similar to `SecSchemaFactory` and `GeoSchemaFactory`
- Use same caching and Parquet conversion strategies as other schemas
- Support same execution engines (DuckDB, Parquet, etc.)
- Maintain constraint metadata for query optimization

## Data Refresh Strategy
- Daily incremental updates from GitHub repository
- Weekly full refresh for committee assignments
- Event-driven updates for new appointments via Federal Register API

## Example Queries

### Cross-Schema Analysis
```sql
-- Congressional representatives by district economic indicators
SELECT 
    pol.congress_members.full_name,
    pol.congress_members.state,
    pol.congress_members.district,
    geo.income_limits.median_income,
    geo.fair_market_rent.two_bedroom
FROM pol.congress_members
JOIN geo.income_limits ON pol.congress_members.state = geo.income_limits.state_alpha
JOIN geo.fair_market_rent ON pol.congress_members.state = geo.fair_market_rent.state
WHERE pol.congress_members.chamber = 'house'
  AND pol.congress_members.current = true;

-- Committee leadership and tenure analysis
SELECT 
    cm.full_name,
    c.name as committee_name,
    cm_mb.role,
    DATEDIFF(CURRENT_DATE, cm_mb.start_date) as days_in_role
FROM pol.congress_members cm
JOIN pol.committee_memberships cm_mb ON cm.bioguide_id = cm_mb.bioguide_id
JOIN pol.committees c ON cm_mb.committee_id = c.committee_id
WHERE cm_mb.role IN ('chair', 'ranking_member')
ORDER BY days_in_role DESC;
```

## Storage Estimates
- **congress_members**: ~50MB (all historical members)
- **congress_terms**: ~100MB (detailed term data)
- **committees**: ~1MB (committee metadata)
- **committee_memberships**: ~20MB (historical assignments)
- **leadership_positions**: ~5MB (leadership history)
- **district_offices**: ~10MB (office locations)

**Total Phase 1**: ~200MB compressed Parquet

This creates a comprehensive political officials schema that integrates seamlessly with existing SEC financial and GEO demographic data for powerful cross-domain analysis.