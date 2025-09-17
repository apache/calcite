# Foreign Key Design Principles for GovData Schemas

## Overview

This document describes the design principles and validation rules for foreign key constraints in the govdata adapters (SEC, GEO, ECON, SAFETY, PUB schemas).

## Core Principle: Format Compatibility

**Foreign keys can ONLY be defined where source and target columns use identical data formats.**

### Valid Format Matches ✅

| Source Schema | Source Column | Format | Target Schema | Target Column | Format | Status |
|---------------|---------------|---------|---------------|---------------|---------|---------|
| SEC | `state_of_incorporation` | 2-letter ("DE", "CA") | GEO | `state_code` | 2-letter ("DE", "CA") | ✅ VALID |
| ECON | `regional_employment.state_code` | 2-letter ("TX", "NY") | GEO | `tiger_states.state_code` | 2-letter ("TX", "NY") | ✅ VALID |
| ECON | `regional_income.geo_fips` | FIPS ("06", "48") | GEO | `tiger_states.state_fips` | FIPS ("06", "48") | ⚠️ PARTIAL |
| GEO | `tiger_counties.state_fips` | FIPS ("06") | GEO | `tiger_states.state_fips` | FIPS ("06") | ✅ VALID |

### Invalid Format Mismatches ❌

| Source | Target | Issue | Reason |
|--------|--------|-------|---------|
| SEC `state_of_incorporation` ("DE") | GEO `state_fips` ("10") | Format mismatch | 2-letter vs FIPS codes |
| ECON `area_code` (varies) | Any single target | Multi-format source | Can be state, MSA, or custom region codes |
| ECON `series_id` ("LNS14000000") | SEC `cik` ("0000320193") | Domain mismatch | BLS series vs SEC company IDs |
| ECON `fred_indicators.date` | SEC `filing_date` | Temporal mismatch | Different reporting cycles |

## Data Format Specifications

### State Identifiers

| Column | Format | Examples | Usage |
|--------|--------|----------|-------|
| `state_code` | 2-letter ANSI | "CA", "TX", "NY", "DE" | SEC incorporation, GEO states |
| `state_fips` | 2-digit FIPS | "06", "48", "36", "10" | GEO geographic hierarchy |

**Conversion Required**: 2-letter ↔ FIPS requires lookup table (CA ↔ 06, TX ↔ 48, etc.)

### Geographic Identifiers

| Column | Format | Examples | Notes |
|--------|--------|----------|-------|
| `geo_fips` | Variable FIPS | "06" (state), "06001" (county) | ⚠️ Mixed granularity |
| `area_code` | Variable | "US", "Northeast", "12345", "S49" | ⚠️ Multiple formats |
| `county_fips` | 5-digit FIPS | "06001", "48201" | County-level |
| `cbsa_code` | 5-digit MSA | "41860", "31080" | Metropolitan areas |

### Economic Series Identifiers

| Column | Source | Format | Examples |
|--------|--------|---------|----------|
| `series_id` | BLS | Alphanumeric | "LNS14000000", "CUUR0000SA0" |
| `series_id` | FRED | Alphanumeric | "UNRATE", "GDP", "CPIAUCSL" |
| `indicator_code` | World Bank | Alphanumeric | "NY.GDP.MKTP.CD", "SP.POP.TOTL" |

**Note**: BLS and FRED series IDs have partial overlap but are not identical systems.

## Constraint Implementation Rules

### 1. Mandatory Format Validation

Before creating any FK constraint:

```java
// ✅ GOOD: Verify format compatibility
// Both columns use 2-letter state codes
sourceColumn: "state_of_incorporation" // Format: 2-letter
targetColumn: "state_code"            // Format: 2-letter
// FK is valid

// ❌ BAD: Format mismatch
// sourceColumn: "state_of_incorporation" // Format: 2-letter ("DE")
// targetColumn: "state_fips"            // Format: FIPS ("10")
// FK would fail - requires lookup table
```

### 2. Partial FK Documentation

When FK only works for subset of data:

```java
// ⚠️ PARTIAL FK: Document limitations
regionalIncomeToStatesFk.put("columns", Arrays.asList("geo_fips"));
regionalIncomeToStatesFk.put("targetColumns", Arrays.asList("state_fips"));
// NOTE: geo_fips can be state FIPS (2-digit) or county FIPS (5-digit)
// This FK only works for state-level data (2-digit geo_fips)
// County-level data (5-digit geo_fips) will violate this constraint
```

### 3. Cross-Domain FK Guidelines

Cross-schema foreign keys should be:
- **Conservative**: Only create where relationship is clear and stable
- **Well-documented**: Explain business rationale
- **Format-compatible**: Source and target must use identical formats

```java
// ✅ GOOD: Clear business relationship with compatible formats
financial_line_items.state_of_incorporation → tiger_states.state_code
// Business rationale: SEC filings reference state of incorporation
// Format compatibility: Both use 2-letter state codes

// ❌ BAD: No clear business relationship
employment_statistics.series_id → financial_line_items.cik
// No business rationale for linking BLS employment data to SEC filings
// Format incompatibility: series_id vs cik are different identifier systems
```

## Invalid FK Patterns to Avoid

### 1. Format Conversion FKs

```java
// ❌ NEVER DO THIS
// This would require runtime format conversion
stateIncorpFK.put("columns", Arrays.asList("state_of_incorporation")); // "DE"
stateIncorpFK.put("targetColumns", Arrays.asList("state_fips"));       // "10"
// Solution: Use lookup table or choose compatible target column
```

### 2. Multi-Format Source Columns

```java
// ❌ NEVER DO THIS
// area_code can be: "US", "Northeast", "12345" (MSA), "S49" (state)
inflationFK.put("columns", Arrays.asList("area_code"));
inflationFK.put("targetColumns", Arrays.asList("state_code"));
// Solution: Create separate FKs for each area_type, or no FK constraint
```

### 3. Temporal Misalignment

```java
// ❌ NEVER DO THIS
// Economic indicators and SEC filings have different reporting cycles
economicFK.put("columns", Arrays.asList("date"));        // Daily economic data
economicFK.put("targetColumns", Arrays.asList("filing_date")); // Quarterly SEC filings
// Solution: Use temporal joins in queries, not FK constraints
```

## Testing and Validation

### Required Tests

1. **Format Compatibility Tests**: Verify source/target column formats match
2. **Data Validation Tests**: Check actual data conforms to expected formats  
3. **Constraint Violation Tests**: Ensure FKs fail appropriately for invalid data
4. **Cross-Schema Integration Tests**: Validate multi-schema FK relationships

### Test Categories

- `@Tag("unit")`: Format validation and constraint definition tests
- `@Tag("integration")`: Multi-schema FK relationship tests  
- `@Tag("data-validation")`: Tests with actual government data

## Future Enhancements

### Potential Valid FKs (Require Implementation)

1. **State Code Lookup Table**: Enable 2-letter ↔ FIPS conversion
2. **Industry Code Mapping**: Link SEC SIC codes to ECON NAICS codes
3. **Geographic Hierarchy**: Complete county→tract→block group FK chain
4. **Temporal Economic Context**: Link SEC filing dates to economic indicators by quarter

### Invalid FK Requests to Reject

1. **Direct Economic↔Financial**: No natural relationship between macro economics and individual company financials
2. **Geographic Detail Mismatch**: National economic data to local SEC company locations
3. **Identifier System Mixing**: BLS series IDs to SEC CIKs, FRED codes to GEO FIPS codes

## Summary

Foreign key constraints in govdata schemas must prioritize **data format compatibility** over relationship convenience. When formats don't match, use:

1. **Lookup tables** for systematic conversions (state codes ↔ FIPS)
2. **Application-level joins** for complex relationships  
3. **Documented partial FKs** for subset relationships
4. **No FK constraint** when relationship is unclear or data-dependent

This approach ensures FK constraints provide meaningful data quality validation rather than causing spurious constraint violations due to format mismatches.