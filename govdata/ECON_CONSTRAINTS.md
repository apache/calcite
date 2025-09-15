# ECON Schema Constraint Definitions

## Overview

This document defines all primary key (PK) and foreign key (FK) constraints for the ECON schema tables. Constraints are implemented in `EconSchemaFactory.defineEconTableConstraints()` and enforce referential integrity within the ECON schema and across schemas where data formats match exactly.

## Primary Key Constraints

All ECON tables have composite primary keys that ensure unique identification of each record:

| Table | Primary Key Columns | Description |
|-------|-------------------|-------------|
| **employment_statistics** | `date`, `series_id` | Uniquely identifies each employment metric by observation date and BLS series |
| **inflation_metrics** | `date`, `index_type`, `item_code`, `area_code` | Composite key for multi-dimensional inflation data |
| **wage_growth** | `date`, `series_id`, `industry_code`, `occupation_code` | Identifies wage data by time, series, industry, and occupation |
| **regional_employment** | `date`, `area_code` | Identifies employment data by time and geographic area |
| **treasury_yields** | `date`, `maturity_months` | Uniquely identifies yield curve points by date and maturity |
| **federal_debt** | `date`, `debt_type` | Identifies debt levels by date and debt category |
| **world_indicators** | `country_code`, `indicator_code`, `year` | Composite key for international economic indicators |
| **fred_indicators** | `series_id`, `date` | Identifies FRED time series observations |
| **gdp_components** | `table_id`, `line_number`, `year` | Identifies BEA GDP component data |
| **regional_income** | `geo_fips`, `metric`, `year` | Identifies regional income by geography, metric type, and year |

## Foreign Key Constraints

### Implemented Foreign Keys

Foreign keys where data formats match exactly or have acceptable partial overlap are implemented:

#### Cross-Schema Foreign Keys (ECON → GEO)

| Source Table | Source Column(s) | Target Schema | Target Table | Target Column(s) | Description |
|--------------|-----------------|---------------|--------------|-----------------|-------------|
| **regional_employment** | `state_code` | geo | tiger_states | `state_code` | Links regional employment to state geographic data. Both use 2-letter state codes (e.g., 'CA', 'NY') |
| **regional_income** | `geo_fips` | geo | tiger_states | `state_fips` | Links state-level regional income to geographic data. Partial FK - works for 2-digit state FIPS codes |

#### Intra-Schema Foreign Keys (ECON → ECON)

| Source Table | Source Column(s) | Target Schema | Target Table | Target Column(s) | Description |
|--------------|-----------------|---------------|--------------|-----------------|-------------|
| **employment_statistics** | `series_id` | econ | fred_indicators | `series_id` | Links BLS employment series to FRED indicators where series overlap |
| **inflation_metrics** | `area_code` | econ | regional_employment | `area_code` | Links inflation data to regional employment for overlapping geographic areas |
| **gdp_components** | `table_id` | econ | fred_indicators | `series_id` | Links BEA GDP components to corresponding FRED GDP time series |

#### Cross-Schema Foreign Keys (SEC → GEO)

Note: This FK is implemented in the SEC schema factory, documented here for completeness.

| Source Table | Source Column(s) | Target Schema | Target Table | Target Column(s) | Description |
|--------------|-----------------|---------------|--------------|-----------------|-------------|
| **filing_metadata** | `state_of_incorporation` | geo | tiger_states | `state_code` | Links company incorporation state to geographic data using 2-letter state codes |

### Relationships NOT Implemented as Foreign Keys

The following relationships exist conceptually but cannot be enforced as FK constraints due to format mismatches or partial data overlap:

#### Format Mismatch Issues

1. **regional_employment.area_code → geo.tiger_counties.county_fips**
   - **Issue**: `area_code` format varies by `area_type` (state/MSA/county)
   - **Reason**: When area_type='county', area_code is county FIPS, but other values use different formats

2. **inflation_metrics.area_code → geo.tiger_cbsa.cbsa_code**
   - **Issue**: `area_code` can be MSA code, state code, or custom region
   - **Reason**: Multiple possible target tables depending on area type

#### Data Granularity Mismatches

3. **ECON tables → SEC tables**
   - **Issue**: No CIK identifiers in ECON tables
   - **Reason**: Economic data is macro/aggregate level while SEC is company-specific
   - **Example**: Cannot link unemployment rates to specific companies

#### Industry Classification Mismatches

4. **wage_growth.industry_code → sec.sic_code or naics_code**
   - **Issue**: Different industry classification systems
   - **Reason**: NAICS codes in wage data don't directly map to SEC's SIC codes

## Intra-Schema Relationships

Within the ECON schema, tables share common dimensions but don't have strict FK relationships:

### Shared Dimensions (Informational Only)

- **Time Dimension**: Most tables have `date` or `year` columns but at different granularities
- **Geographic Dimension**: `area_code`, `state_code`, `geo_fips` appear in multiple tables
- **Series Identifiers**: `series_id` appears in both BLS and FRED tables but with different namespaces

## Constraint Validation Rules

### Primary Key Rules
1. All PK columns must be NOT NULL
2. Combination of PK columns must be unique
3. PK should represent the natural business key

### Foreign Key Rules
1. **No Format Conversion**: FK relationships require exact format match
2. **Complete Coverage**: Target table must contain all possible source values
3. **Stable References**: Target data must be relatively static (e.g., state codes)

## Query Examples Using Constraints

### Joining ECON with GEO data (using FK)
```sql
-- Regional employment with full state information
SELECT 
    re.date,
    re.state_code,
    ts.state_name,
    ts.state_fips,
    re.unemployment_rate,
    re.employment_level,
    ts.land_area / 2589988.11 as land_area_sq_miles
FROM econ.regional_employment re
JOIN geo.tiger_states ts ON re.state_code = ts.state_code
WHERE re.area_type = 'state'
  AND re.date >= '2023-01-01';
```

### Attempting conceptual join (no FK, requires careful filtering)
```sql
-- Regional income for states only (geo_fips must be 2-digit)
SELECT 
    ri.geo_name,
    ri.year,
    ri.value as personal_income,
    ts.state_name,
    ts.population
FROM econ.regional_income ri
LEFT JOIN geo.tiger_states ts 
    ON ri.geo_fips = ts.state_fips
    AND LENGTH(ri.geo_fips) = 2  -- Only state-level FIPS
WHERE ri.metric = 'Total Personal Income'
  AND ri.year = 2023;
```

## Constraint Metadata Access

Constraints can be accessed programmatically through:

1. **JDBC DatabaseMetaData API**
   - `getPrimaryKeys()` - Returns PK columns
   - `getImportedKeys()` - Returns FK relationships
   - `getExportedKeys()` - Returns tables that reference this table

2. **Calcite Schema API**
   - Through `ConstraintCapableSchemaFactory` interface
   - Via `EconSchemaFactory.defineEconTableConstraints()` method

## Future Considerations

### Potential Enhancements
1. **Composite Foreign Keys**: If geographic coding becomes standardized
2. **Temporal Foreign Keys**: For time-based relationships
3. **Conditional Foreign Keys**: Based on discriminator columns (e.g., area_type)

### Data Standardization Opportunities
1. Standardize geographic codes across all tables
2. Align industry classification systems
3. Create mapping tables for code conversions

## Summary

The ECON schema implements:
- **10 Primary Keys**: One for each table, ensuring data integrity
- **6 Foreign Keys**: 
  - 2 Cross-schema FKs to GEO schema (regional_employment, regional_income)
  - 3 Intra-schema FKs within ECON (employment_statistics, inflation_metrics, gdp_components)
  - 1 Cross-schema FK from SEC to GEO (documented for completeness)
- **4 Documented Non-FKs**: Relationships that cannot be enforced due to format/classification mismatches

This balanced approach implements all valid foreign key relationships while documenting why certain conceptual relationships cannot be enforced as constraints. The implementation includes both exact-match FKs and partial-overlap FKs where the business relationship is meaningful.