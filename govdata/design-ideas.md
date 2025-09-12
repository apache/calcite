# SEC Adapter Design Ideas (future: US-Business Adapter)

This document captures design ideas and improvement opportunities for the SEC adapter (planned future expansion to US-Business adapter).

## Adapter Expansion Plan

### Future Expansion to US-Business Adapter
The SEC adapter is planned for future expansion to `us-business` as part of a broader logical reorganization of US government data sources. This adapter will serve as the comprehensive business and innovation data platform.

### Additional Data Sources Being Added

#### USPTO Integration
- **Patent Data**: Full patent grants, applications, citations
- **Trademark Data**: Registered marks, applications, status
- **Assignment Records**: Patent and trademark ownership transfers
- **PTAB Proceedings**: Patent trial and appeal board decisions

#### FDA Corporate Data
- **Drug Approvals**: NDAs, ANDAs, 510(k) clearances
- **Device Clearances**: Medical device approvals and registrations
- **Orange Book**: Patent and exclusivity data for drugs
- **Adverse Events**: FAERS data for post-market surveillance

#### Clinical Trials (ClinicalTrials.gov)
- **Trial Registry**: All registered clinical trials
- **Corporate Sponsors**: Industry-funded research
- **Results Database**: Trial outcomes and publications
- **Drug Development Pipeline**: Phase tracking for therapeutics

### Architecture: Multiple Schema Factories
Each sub-domain within the us-business adapter will have its own schema factory for clean separation:

```
us-business/
└── org.apache.calcite.adapter.business/
    ├── common/                          # Shared across all sub-schemas
    │   ├── EntityResolver.java
    │   └── BusinessRateLimiter.java
    ├── sec/
    │   └── SecSchemaFactory.java       # Creates 'sec' schema
    ├── uspto/
    │   └── UsptoSchemaFactory.java     # Creates 'uspto' schema
    ├── fda/
    │   └── FdaSchemaFactory.java       # Creates 'fda' schema
    └── clinical/
        └── ClinicalSchemaFactory.java  # Creates 'clinical_trials' schema
```

The JDBC driver will configure multiple schemas:
```java
{
  "schemas": [
    {"name": "sec", "factory": "org.apache.calcite.adapter.business.sec.SecSchemaFactory"},
    {"name": "uspto", "factory": "org.apache.calcite.adapter.business.uspto.UsptoSchemaFactory"},
    {"name": "fda", "factory": "org.apache.calcite.adapter.business.fda.FdaSchemaFactory"},
    {"name": "clinical_trials", "factory": "org.apache.calcite.adapter.business.clinical.ClinicalSchemaFactory"}
  ]
}
```

### Unified Business Intelligence
The expanded adapter will enable powerful cross-source queries across schemas:
```sql
-- Company innovation portfolio
SELECT
  c.ticker,
  c.company_name,
  COUNT(DISTINCT p.patent_number) as patents,
  COUNT(DISTINCT t.trademark_id) as trademarks,
  COUNT(DISTINCT d.nda_number) as drug_approvals,
  COUNT(DISTINCT ct.nct_number) as clinical_trials
FROM sec.companies c
LEFT JOIN uspto.patents p ON c.name = p.assignee
LEFT JOIN uspto.trademarks t ON c.name = t.owner
LEFT JOIN fda.approvals d ON c.name = d.sponsor
LEFT JOIN clinical_trials.trials ct ON c.name = ct.sponsor
GROUP BY c.ticker, c.company_name;
```

## 1. Code Reuse Opportunities - Leveraging File Adapter

### Current State
The SEC adapter currently operates somewhat independently of the file adapter, despite delegating to `FileSchemaFactory` for schema creation. This has led to duplicated functionality and missed opportunities to leverage the file adapter's comprehensive infrastructure.

### Identified Reuse Opportunities

#### 1.1 XML Processing & Data Type Fidelity (CRITICAL - NUANCED APPROACH)
**Current**: Custom DOM parsing in `SecToParquetConverter` with explicit Avro schema definitions
**File Adapter Has**: `XmlToJsonConverter` with automatic pattern detection, XPath support, nested element flattening

**Data Type Fidelity Consideration**:
SEC contains rich type information that standard JSON cannot preserve:
- **Monetary values**: Need DECIMAL(19,4) precision, not floating point
- **Share counts**: Should be BIGINT, not generic number
- **Percentages**: Require specific scale (e.g., DECIMAL(5,4))
- **Dates/Periods**: Have explicit temporal semantics
- **Units & Precision**: SEC includes decimals attribute for precision requirements

**Recommended Hybrid Approach**:
1. **Keep Direct SEC→Parquet for Financial Data** (preserves type fidelity)
   - Maintain current `SecToParquetConverter` logic for type-aware conversion
   - Preserve explicit Avro schema definitions with proper types
   - Critical for financial accuracy and regulatory compliance

2. **Wrap in File Adapter Infrastructure** (gain infrastructure benefits)
   - Implement `SecToParquetConverter` as a `FileConverter` interface
   - Use file adapter's `FileConversionManager` for pipeline management
   - Leverage `StorageCacheManager` for caching converted files
   - Benefit from refresh detection and metadata tracking

3. **Optional Future Enhancement**: Typed JSON Intermediate
   ```json
   {
     "_schema": {
       "revenue": {"type": "decimal", "precision": 19, "scale": 4, "unit": "USD"},
       "shares": {"type": "bigint", "unit": "shares"},
       "filing_date": {"type": "date", "format": "yyyy-MM-dd"}
     },
     "data": [...]
   }
   ```

**Benefits of Hybrid Approach**:
- ✅ Preserves SEC's rich type information
- ✅ Maintains financial data precision
- ✅ Leverages file adapter's infrastructure
- ✅ Allows gradual migration if needed
- ✅ No loss of critical metadata

#### 1.2 HTTP Data Fetching (MEDIUM PRIORITY)
**Current**: Direct HTTP calls in `SecDataFetcher` and `EdgarDownloader`
**File Adapter Has**: `HttpStorageProvider` with caching, retry logic, authentication
**Opportunity**:
- Use file adapter's HTTP storage provider for SEC API calls
- Create `SecHttpStorageProvider` extending `HttpStorageProvider` for SEC-specific headers/rate limiting
**Benefits**:
- Unified caching strategy
- Better error handling and retry logic
- Configuration consistency
- Built-in authentication support

#### 1.3 Caching Infrastructure (HIGH PRIORITY)
**Current**: Custom memory caching in `SecDataFetcher` with background refresh
**File Adapter Has**: `StorageCacheManager`, `PersistentStorageCache`, metadata-based refresh detection
**Opportunity**:
- Replace SEC's custom caching with file adapter's infrastructure
- Use `PersistentStorageCache` for SEC data caching
**Benefits**:
- Persistent cache across JVM restarts
- Better cache coordination
- Unified cache management
- Automatic cache expiration and refresh

#### 1.4 File Format Conversion (CRITICAL - WITH JSON SCHEMA SUPPORT)
**Current**: Direct SEC→Parquet conversion with custom Avro schema generation
**File Adapter Has**: `FileConversionManager` with metadata tracking, refresh detection
**File Adapter Will Have**: JSON Schema companion file support for type fidelity (see file adapter design-ideas.md)

**Opportunity with JSON Schema Companion Files** (contingent on file adapter implementation):
When the file adapter supports JSON Schema companion files, the SEC adapter can:
1. Generate both `.json` and `.schema.json` files from SEC
2. Map SEC data types to SQL types via JSON Schema
3. Use `x-sec-*` extensions for SEC-specific metadata
4. Enable lossless SEC→JSON→Parquet pipeline

**Example SEC-generated schema**:
```json
{
  "properties": {
    "us-gaap:Revenue": {
      "type": "number",
      "x-sql-type": "DECIMAL(19,4)",
      "x-sec-type": "monetaryItemType",
      "x-sec-unit": "USD",
      "x-sec-context": "instant"
    }
  }
}
```

**Benefits**:
- Solves type fidelity problem completely
- Leverages standard JSON Schema
- Enables JSON intermediate without type loss
- Self-documenting data with schemas

#### 1.5 Table Refresh Mechanisms for New Filing Detection (HIGH PRIORITY)
**Current**: No mechanism to detect and ingest new SEC filings
**File Adapter Has**: `AbstractRefreshableTable`, `RefreshableTable` interface, automatic refresh scheduling
**Important Note**: SEC files are immutable - SEC filings don't change once submitted. Amendments are filed as new documents with new accession numbers.
**Opportunity**:
- Create `SecRefreshableTable` extending `AbstractRefreshableTable`
- Implement automatic NEW SEC filing detection (not file modification detection)
- Track latest accession number/filing date processed
- Query SEC EDGAR API only for filings newer than last known
**Benefits**:
- Automatic ingestion of new SEC filings as they arrive
- No unnecessary re-processing of existing immutable files
- Efficient incremental updates to the data catalog
- Built-in scheduling for periodic new filing checks

### Implementation Strategy

#### Phase 1: Leverage Existing Infrastructure (Quick Wins)
1. Replace HTTP calls with `HttpStorageProvider`
   - Create `SecHttpStorageProvider` for SEC-specific logic
   - Use for both EdgarDownloader and SecDataFetcher

2. Replace custom caching with file adapter's cache
   - Use `StorageCacheManager` for CIK registry caching
   - Use `PersistentStorageCache` for SEC data

#### Phase 2: Integrate Conversion Pipeline (Type-Aware)
1. Wrap SEC→Parquet converter in file adapter interface
   - Implement `SecToParquetConverter` as `FileConverter`
   - Register with `FileConversionManager` for pipeline integration
   - Maintain direct type-aware conversion (no JSON intermediate initially)

2. Use file adapter's conversion infrastructure
   - Leverage metadata tracking for conversion state
   - Use refresh detection for updated SEC files
   - Benefit from conversion caching and coordination

3. Optional: Future typed JSON intermediate
   - Create `SecToTypedJsonConverter` that preserves type metadata
   - Implement `TypedJsonToParquetConverter` that reads type hints
   - Only if JSON intermediate proves necessary for debugging/flexibility

#### Phase 3: Advanced Features
1. Create refreshable SEC tables for new filing ingestion
   - Extend `AbstractRefreshableTable`
   - Implement NEW SEC filing detection (poll for filings newer than last processed)
   - Track watermark (latest accession number or filing date)
   - Only download and process genuinely new filings
   - Never re-process existing files (they're immutable)

2. Integrate with file adapter's statistics
   - Use HLL sketches for cardinality estimation
   - Leverage column statistics for optimization

### Benefits Summary
- **Eliminate Code Duplication**: Remove ~500 lines of duplicated caching/HTTP logic
- **Improved Reliability**: Leverage battle-tested file adapter components
- **Better Performance**: Use file adapter's optimized caching and conversion
- **Feature Parity**: Gain refresh mechanisms, statistics, and optimization for free
- **Maintainability**: Single codebase for common functionality
- **Configuration**: Unified configuration patterns across adapters

### Specific Classes to Reuse

From file adapter:
- `FileConversionManager` - Unified conversion pipeline
- `XmlToJsonConverter` - XML processing base
- `StorageCacheManager` - Caching infrastructure
- `HttpStorageProvider` - HTTP operations
- `AbstractRefreshableTable` - Refresh mechanisms
- `ExecutionEngineConfig` - Already used successfully

New SEC classes extending file adapter:
- `SecRefreshableTable extends AbstractRefreshableTable`
- `SecHttpStorageProvider extends HttpStorageProvider`
- `SecToJsonConverter implements FileConverter`
- `SecStorageProvider extends AbstractStorageProvider`

### Data Type Fidelity Decision Summary
After careful analysis, the recommendation is to **maintain direct SEC→Parquet conversion** while leveraging file adapter's infrastructure. This decision is based on:

1. **Financial Data Precision Requirements**
   - SEC monetary values must maintain exact decimal precision
   - Loss of type information through JSON could cause rounding errors
   - Regulatory compliance requires accurate representation

2. **SEC's Rich Type System**
   - Explicit data types (monetary, shares, percent, date)
   - Unit specifications (USD, EUR, shares, etc.)
   - Precision indicators (decimals attribute)
   - Context information (instant vs duration)

3. **Best of Both Worlds Approach**
   - Keep type-aware direct conversion for correctness
   - Wrap in file adapter interfaces for infrastructure benefits
   - Option to add typed JSON later if needed

### Important: SEC File Immutability
A critical characteristic of SEC/SEC filings that affects adapter design:

1. **SEC Files are Immutable**
   - Once filed with the SEC, SEC documents never change
   - Each filing has a unique accession number that permanently identifies it
   - Corrections or updates are filed as NEW documents (amendments) with NEW accession numbers
   - Example: A 10-K/A (amendment) doesn't modify the original 10-K, it's a separate filing

2. **Implications for Refresh Mechanisms**
   - Traditional file adapter refresh (detecting modified files) is unnecessary
   - Instead, focus on detecting and ingesting NEW filings
   - Existing Parquet conversions never need updating
   - Cache invalidation is not needed for processed files

3. **Proper Refresh Strategy**
   - Maintain a watermark (latest filing date or accession number processed)
   - Periodically query SEC EDGAR API for filings newer than watermark
   - Download and process only genuinely new filings
   - Update watermark after successful processing

This is fundamentally different from typical file adapter scenarios where source files might be updated in place.

### Next Steps
1. Prototype `SecHttpStorageProvider` for SEC API calls
2. Replace `SecDataFetcher` caching with `StorageCacheManager`
3. Wrap `SecToParquetConverter` as `FileConverter` interface
4. Create integration tests validating file adapter reuse while preserving type fidelity
