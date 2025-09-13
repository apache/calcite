# REG Schema Implementation Plan

## Overview
Create a new `reg` (regulatory/compliance) schema within the govdata adapter to provide SQL access to federal regulations, rules, enforcement actions, and compliance data across all government agencies.

## Data Sources & Tables

### Primary Data Sources

#### Federal Register
1. **Federal Register API** (Free, Government)
   - Daily publication of rules, proposed rules, notices
   - Presidential documents, executive orders
   - Public inspection documents
   - JSON/XML REST API, documents since 1994

2. **Code of Federal Regulations (CFR)**
   - Complete regulatory code organized by titles
   - Annual editions with quarterly updates
   - XML format from Government Publishing Office

#### Agency-Specific Regulatory Data
3. **SEC EDGAR Regulatory Filings** (Free, Government)
   - Enforcement actions, cease and desist orders
   - No-action letters, interpretive releases
   - Investment adviser/company registrations

4. **FCC APIs** (Free, Government)
   - Communications regulations, licensing
   - Enforcement actions, fines, forfeitures
   - Broadband availability and competition data

5. **DOT/FAA APIs** (Free, Government)  
   - Aviation regulations, safety directives
   - Transportation safety enforcement
   - Airline consumer complaints and enforcement

6. **OSHA APIs** (Free, Government)
   - Workplace safety regulations and standards
   - Inspection data, citations, penalties
   - Establishment inspection database

7. **FTC APIs** (Free, Government)
   - Consumer protection enforcement actions
   - Antitrust cases and settlements
   - Do Not Call Registry violations

#### Judicial and Enforcement Data
8. **PACER Case Data** (Fee-based, Court System)
   - Federal court cases and regulatory appeals
   - Administrative law judge decisions
   - Regulatory challenge outcomes

9. **GAO Reports API** (Free, Government)
   - Government accountability and oversight reports
   - Regulatory impact assessments
   - Fraud, waste, and abuse findings

## Proposed Table Schema

### Core Regulatory Tables
1. **`federal_register_documents`** - All Federal Register publications
   - Columns: document_number, title, abstract, agencies, publication_date, document_type, effective_date, cfr_references, page_count

2. **`proposed_rules`** - Proposed regulatory changes
   - Columns: document_number, title, proposing_agency, publication_date, comment_period_end, estimated_compliance_cost, regulatory_impact_analysis

3. **`final_rules`** - Enacted regulations
   - Columns: document_number, title, issuing_agency, effective_date, cfr_title, cfr_part, regulatory_impact, small_business_impact

4. **`executive_orders`** - Presidential directives
   - Columns: eo_number, title, president, signing_date, revoked_date, subject_area, agencies_affected

### Code of Federal Regulations
5. **`cfr_sections`** - Current regulatory text
   - Columns: cfr_title, cfr_part, cfr_section, section_title, regulatory_text, last_updated, authority_citation

6. **`cfr_changes`** - Historical regulatory changes
   - Columns: cfr_reference, change_type, effective_date, federal_register_citation, change_description, previous_text

### Enforcement and Compliance
7. **`enforcement_actions`** - Cross-agency enforcement
   - Columns: action_id, agency, action_type, respondent, violation_description, penalty_amount, action_date, settlement_terms

8. **`regulatory_violations`** - Compliance violations
   - Columns: violation_id, entity_name, agency, regulation_violated, violation_date, penalty_assessed, compliance_status

9. **`agency_inspections`** - Regulatory inspections
   - Columns: inspection_id, inspecting_agency, facility_name, inspection_date, violations_found, follow_up_required, inspector_notes

### Agency-Specific Tables
10. **`sec_enforcement_actions`** - SEC regulatory enforcement
    - Columns: release_number, action_type, respondent, violation_type, civil_penalty, disgorgement, administrative_proceeding

11. **`fcc_enforcement_actions`** - FCC violations and fines
    - Columns: enforcement_number, licensee, violation_type, forfeiture_amount, compliance_date, bureau

12. **`osha_inspections`** - Workplace safety inspections
    - Columns: inspection_number, establishment_name, inspection_date, inspection_type, violations, penalties, industry_code

13. **`dot_safety_actions`** - Transportation safety enforcement
    - Columns: case_number, carrier_name, violation_type, civil_penalty, safety_rating, compliance_date

### Regulatory Impact and Analysis
14. **`regulatory_impact_analyses`** - Economic impact assessments
    - Columns: ria_id, rule_title, agency, estimated_cost, estimated_benefit, small_business_impact, analysis_date

15. **`public_comments`** - Regulatory comment submissions
    - Columns: comment_id, document_number, commenter_name, organization, comment_date, comment_text, attachment_count

16. **`gao_reports`** - Government accountability reports
    - Columns: gao_number, title, publication_date, agencies_reviewed, recommendations, regulatory_implications, report_type

## Implementation Steps

### Phase 1: Core Federal Register Data
1. Create REG schema factory class extending existing pattern
2. Implement Federal Register API data providers
3. Create Parquet conversion pipeline for regulatory documents
4. Implement core tables: federal_register_documents, proposed_rules, final_rules
5. Add text search capabilities for regulatory content
6. Create model files and integration tests

### Phase 2: CFR Integration
1. Add Code of Federal Regulations data processing
2. Implement cfr_sections and cfr_changes tables
3. Create regulatory citation linking and cross-referencing
4. Add regulatory hierarchy navigation

### Phase 3: Enforcement Data
1. Implement cross-agency enforcement action aggregation
2. Add agency-specific enforcement tables (SEC, FCC, OSHA, DOT)
3. Create penalty and compliance tracking
4. Add violation pattern analysis capabilities

### Phase 4: Impact Analysis and Comments
1. Add regulatory impact analysis data
2. Implement public comment processing and analysis
3. Create stakeholder engagement tracking
4. Add cost-benefit analysis capabilities

### Phase 5: Judicial and Oversight Integration
1. Add GAO report integration for regulatory oversight
2. Implement regulatory challenge and court decision tracking
3. Create regulatory effectiveness measurement
4. Add compliance trend analysis

## Technical Architecture
- Follow existing govdata pattern with dedicated `reg/` package
- Implement `RegSchemaFactory` similar to other schema factories
- Use same caching and Parquet conversion strategies as other schemas
- Support same execution engines (DuckDB, Parquet, etc.)
- Implement full-text search capabilities for regulatory documents
- Handle large document processing and text extraction
- Maintain regulatory citation parsing and linking

## Data Refresh Strategy
- **Federal Register**: Daily updates for new publications
- **CFR updates**: Quarterly updates for regulatory changes
- **Enforcement actions**: Weekly updates from agency feeds
- **GAO reports**: Weekly updates for new publications
- **Public comments**: Daily during active comment periods

## Example Queries

### Cross-Schema Analysis
```sql
-- Regulatory burden vs economic outcomes by congressional district
SELECT 
    pol.congress_members.full_name,
    pol.congress_members.state,
    COUNT(reg.enforcement_actions.action_id) as enforcement_count,
    AVG(reg.enforcement_actions.penalty_amount) as avg_penalty,
    geo.income_limits.median_income
FROM pol.congress_members pol
JOIN reg.enforcement_actions reg ON pol.state = reg.respondent_state
JOIN geo.income_limits geo ON pol.state = geo.state_alpha
WHERE pol.chamber = 'house' AND pol.current = true
  AND reg.action_date >= '2023-01-01'
GROUP BY pol.full_name, pol.state, geo.median_income
ORDER BY enforcement_count DESC;

-- SEC enforcement vs company financial performance
SELECT 
    sec.company_metadata.company_name,
    COUNT(reg.sec_enforcement_actions.release_number) as sec_actions,
    SUM(reg.sec_enforcement_actions.civil_penalty) as total_penalties,
    AVG(sec.financial_line_items.value) as avg_net_income
FROM sec.company_metadata
LEFT JOIN reg.sec_enforcement_actions ON sec.company_metadata.cik = reg.sec_enforcement_actions.cik
LEFT JOIN sec.financial_line_items ON sec.company_metadata.cik = sec.financial_line_items.cik
WHERE sec.financial_line_items.concept = 'NetIncomeLoss'
  AND sec.financial_line_items.fiscal_year >= 2020
GROUP BY sec.company_metadata.company_name
HAVING sec_actions > 0
ORDER BY total_penalties DESC;

-- Regulatory impact vs health outcomes
SELECT 
    reg.regulatory_impact_analyses.agency,
    SUM(reg.regulatory_impact_analyses.estimated_cost) as total_reg_cost,
    AVG(sci.cdc_mortality.death_count) as avg_mortality,
    COUNT(reg.final_rules.document_number) as rules_enacted
FROM reg.regulatory_impact_analyses
JOIN reg.final_rules ON reg.regulatory_impact_analyses.rule_title = reg.final_rules.title
JOIN sci.cdc_mortality ON reg.final_rules.effective_date BETWEEN sci.cdc_mortality.year_start AND sci.cdc_mortality.year_end
WHERE reg.final_rules.effective_date >= '2020-01-01'
GROUP BY reg.regulatory_impact_analyses.agency
ORDER BY total_reg_cost DESC;

-- Public engagement in regulatory process
SELECT 
    reg.federal_register_documents.agencies[0] as agency,
    reg.proposed_rules.title,
    COUNT(reg.public_comments.comment_id) as comment_count,
    AVG(LENGTH(reg.public_comments.comment_text)) as avg_comment_length,
    reg.proposed_rules.estimated_compliance_cost
FROM reg.proposed_rules
JOIN reg.federal_register_documents ON reg.proposed_rules.document_number = reg.federal_register_documents.document_number
LEFT JOIN reg.public_comments ON reg.proposed_rules.document_number = reg.public_comments.document_number
WHERE reg.proposed_rules.publication_date >= '2023-01-01'
GROUP BY agency, reg.proposed_rules.title, reg.proposed_rules.estimated_compliance_cost
ORDER BY comment_count DESC;
```

## Storage Estimates
- **Federal Register documents**: ~15GB (full text since 1994)
- **CFR sections**: ~5GB (complete regulatory code)
- **Enforcement actions**: ~2GB (all agencies, historical)
- **Public comments**: ~10GB (comment text and metadata)
- **GAO reports**: ~3GB (full report text)
- **Agency-specific tables**: ~5GB (detailed enforcement data)

**Total Phase 1-5**: ~40GB compressed Parquet

## Data Processing Challenges
- **Document parsing**: Extract structured data from regulatory PDF/XML documents
- **Citation linking**: Parse and link CFR references across documents
- **Text processing**: Handle large document text for search and analysis
- **Entity resolution**: Match companies/entities across different regulatory systems
- **Date handling**: Manage effective dates, comment periods, compliance deadlines

## Compliance and Legal Considerations
- **Public domain content**: All federal regulatory content is public domain
- **Accuracy requirements**: Regulatory text must maintain legal accuracy
- **Update timeliness**: Critical for compliance-dependent users
- **Citation integrity**: Maintain proper regulatory citation formats
- **Version control**: Track regulatory changes and effective dates

## Advanced Features
- **Full-text search**: Enable natural language queries across all regulatory text
- **Citation network analysis**: Map relationships between regulations
- **Regulatory burden metrics**: Calculate cumulative compliance costs
- **Trend analysis**: Track regulatory activity patterns over time
- **Alert system**: Notify of relevant regulatory changes

This creates a comprehensive regulatory/compliance schema that enables analysis of the entire federal regulatory landscape, from rule-making through enforcement and compliance outcomes.