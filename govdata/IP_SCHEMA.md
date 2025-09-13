# IP Schema Implementation Plan

## Overview
Create a new `ip` (intellectual property) schema within the govdata adapter to provide SQL access to US and international intellectual property data, including patents, trademarks, copyrights, and innovation metrics from USPTO and other IP authorities.

## Data Sources & Tables

### Primary Data Sources

#### USPTO (United States Patent and Trademark Office)
1. **USPTO Patent Data API** (Free, Government)
   - Patent grants and applications (1976-present)
   - Full patent text, claims, citations, legal status
   - JSON/XML REST API with bulk download options

2. **USPTO Patent Assignment Database** (Free, Government)
   - Patent ownership transfers and assignments
   - Corporate acquisition tracking through IP
   - Searchable database with CSV export

3. **USPTO Trademark Database** (Free, Government)  
   - Federal trademark registrations and applications
   - Trademark status, classifications, renewal data
   - TESS (Trademark Electronic Search System) API

4. **Patent Trial and Appeal Board (PTAB)** (Free, Government)
   - Inter partes review (IPR) proceedings
   - Patent validity challenges and outcomes
   - Administrative trials and appeals data

5. **USPTO Global Dossier** (Free, Government)
   - International patent family information
   - Foreign filing data and prosecution history
   - Patent cooperation treaty (PCT) applications

#### International IP Data
6. **Google Patents Public Datasets** (Free, BigQuery)
   - Global patent data from multiple patent offices
   - Patent citations, classifications, legal events
   - Maintained partnership with patent offices worldwide

7. **WIPO Global Brand Database** (Free, International)
   - International trademark registrations
   - Madrid System trademark data
   - Industrial design registrations

#### Research and Innovation Linkage
8. **NSF Survey of Industrial Research** (Free, Government)
   - Corporate R&D spending by industry
   - Innovation investment metrics
   - Research personnel and facilities data

9. **SBIR/STTR Awards Database** (Free, Government)
   - Small business innovation research grants
   - Technology transfer and commercialization
   - Startup innovation pipeline tracking

## Proposed Table Schema

### Core Patent Tables
1. **`patent_grants`** - Issued US patents
   - Columns: patent_number, title, abstract, inventors, assignees, filing_date, grant_date, expiration_date, patent_type, technology_class

2. **`patent_applications`** - Published patent applications
   - Columns: application_number, publication_number, title, abstract, inventors, assignees, filing_date, publication_date, status, continuation_data

3. **`patent_claims`** - Individual patent claims
   - Columns: patent_number, claim_number, claim_text, claim_type, dependent_on, claim_status

4. **`patent_citations`** - Patent-to-patent citations
   - Columns: citing_patent, cited_patent, citation_type, examiner_citation, applicant_citation, citation_category

### Patent Classification and Technology
5. **`patent_classifications`** - Technology classification codes
   - Columns: patent_number, classification_system, primary_class, secondary_classes, ipc_codes, cpc_codes

6. **`technology_sectors`** - Technology sector mappings
   - Columns: classification_code, technology_sector, industry_group, innovation_category, research_intensity

### Patent Ownership and Legal Status
7. **`patent_assignments`** - Ownership transfers
   - Columns: patent_number, assignment_date, assignor, assignee, assignment_type, recorded_date, assignment_id

8. **`patent_legal_events`** - Legal status changes
   - Columns: patent_number, event_date, event_type, event_description, fee_paid, patent_status

9. **`patent_litigation`** - Patent disputes and lawsuits
   - Columns: case_id, patent_number, plaintiff, defendant, court, filing_date, case_status, outcome, damages_awarded

### USPTO Administrative Proceedings
10. **`ptab_proceedings`** - Patent Trial and Appeal Board cases
    - Columns: proceeding_number, patent_number, petitioner, patent_owner, proceeding_type, filing_date, decision_date, outcome

11. **`patent_reexaminations`** - Patent validity challenges
    - Columns: reexamination_number, patent_number, requester, filing_date, examiner, final_decision, claims_affected

### Trademark Tables
12. **`trademark_registrations`** - Federal trademark registrations
    - Columns: registration_number, serial_number, mark_text, owner, filing_date, registration_date, renewal_date, goods_services, status

13. **`trademark_applications`** - Trademark application pipeline
    - Columns: serial_number, mark_text, applicant, filing_date, publication_date, opposition_period, application_status

14. **`trademark_classifications`** - Goods and services classifications
    - Columns: serial_number, class_number, goods_services_description, international_class, status

### Innovation and Research Linkage
15. **`patent_inventor_profiles`** - Inventor analytics
    - Columns: inventor_id, inventor_name, patent_count, citation_count, technology_areas, geographic_location, career_span

16. **`corporate_patent_portfolios`** - Company IP analytics
    - Columns: assignee_name, patent_count, active_patents, technology_focus, portfolio_value, r_and_d_intensity

17. **`innovation_metrics`** - Technology innovation indicators
    - Columns: technology_sector, year, patent_applications, grant_rate, citation_intensity, international_filings, startup_activity

### International IP Integration
18. **`global_patent_families`** - International patent relationships
    - Columns: family_id, priority_patent, filing_countries, patent_numbers, technology_class, global_coverage

19. **`pct_applications`** - Patent Cooperation Treaty filings
    - Columns: pct_number, priority_date, applicant, title, technology_sector, designated_countries, national_phase_entries

20. **`international_trademarks`** - Global trademark registrations
    - Columns: registration_number, madrid_system, mark_text, owner, protected_countries, renewal_dates, status

## Implementation Steps

### Phase 1: Core US Patent Data
1. Create IP schema factory class extending existing pattern
2. Implement USPTO patent data API providers
3. Create Parquet conversion pipeline for patent grants and applications
4. Implement core tables: patent_grants, patent_applications, patent_citations
5. Add full-text search capabilities for patent documents
6. Create model files and integration tests

### Phase 2: Patent Analytics and Classification
1. Add patent classification and technology sector mapping
2. Implement patent assignment and legal status tracking
3. Create inventor and assignee analytics tables
4. Add patent citation network analysis capabilities

### Phase 3: USPTO Administrative Data
1. Implement PTAB proceedings and patent reexamination data
2. Add trademark registration and application tables
3. Create trademark classification and status tracking
4. Add IP litigation and dispute resolution data

### Phase 4: Innovation Metrics and Research Linkage
1. Connect patent data to research funding (NIH, NSF, SBIR)
2. Implement corporate patent portfolio analytics
3. Create innovation sector metrics and trend analysis
4. Add inventor career tracking and mobility analysis

### Phase 5: International IP Integration
1. Add global patent family mapping
2. Implement PCT and international trademark data
3. Create cross-border IP analysis capabilities
4. Add competitive intelligence and market analysis features

## Technical Architecture
- Follow existing govdata pattern with dedicated `ip/` package
- Implement `IpSchemaFactory` similar to other schema factories
- Use same caching and Parquet conversion strategies as other schemas
- Support same execution engines (DuckDB, Parquet, etc.)
- Implement specialized text search for patent and trademark content
- Handle large document processing and image/drawing extraction
- Create citation network analysis and graph processing capabilities

## Data Refresh Strategy
- **Patent grants**: Weekly updates from USPTO bulk data
- **Patent applications**: Weekly updates for new publications
- **Legal status changes**: Daily updates for patent maintenance
- **PTAB proceedings**: Daily updates for active cases
- **Trademark data**: Daily updates for new applications and registrations
- **International data**: Monthly synchronization with global databases

## Example Queries

### Cross-Schema Analysis
```sql
-- NIH research funding vs patent output by institution
SELECT 
    sci.nih_research_grants.institution,
    COUNT(DISTINCT sci.nih_research_grants.grant_id) as nih_grants,
    SUM(sci.nih_research_grants.award_amount) as total_funding,
    COUNT(ip.patent_grants.patent_number) as patents_generated,
    AVG(ip.patent_citations.citation_count) as avg_citations
FROM sci.nih_research_grants
LEFT JOIN ip.patent_grants ON sci.nih_research_grants.principal_investigator = ANY(ip.patent_grants.inventors)
LEFT JOIN (
    SELECT citing_patent, COUNT(*) as citation_count
    FROM ip.patent_citations 
    GROUP BY citing_patent
) ip.patent_citations ON ip.patent_grants.patent_number = ip.patent_citations.citing_patent
WHERE sci.nih_research_grants.start_date >= '2020-01-01'
GROUP BY sci.nih_research_grants.institution
ORDER BY patents_generated DESC;

-- Congressional district innovation activity vs economic outcomes
SELECT 
    pol.congress_members.full_name,
    pol.congress_members.state,
    pol.congress_members.district,
    COUNT(ip.patent_grants.patent_number) as district_patents,
    AVG(ip.innovation_metrics.patent_applications) as innovation_intensity,
    geo.income_limits.median_income,
    sec.company_metadata.industry_concentration
FROM pol.congress_members pol
JOIN ip.patent_grants ip ON pol.state = ip.inventor_state
JOIN ip.innovation_metrics metrics ON pol.district = metrics.congressional_district
JOIN geo.income_limits geo ON pol.state = geo.state_alpha
LEFT JOIN sec.company_metadata ON ip.assignee_name = sec.company_name
WHERE pol.chamber = 'house' AND pol.current = true
  AND ip.grant_date >= '2020-01-01'
GROUP BY pol.full_name, pol.state, pol.district, geo.median_income
ORDER BY district_patents DESC;

-- Pharmaceutical patent landscape vs FDA approvals
SELECT 
    ip.patent_grants.assignee_name as pharma_company,
    COUNT(ip.patent_grants.patent_number) as pharma_patents,
    COUNT(sci.fda_drug_approvals.application_number) as fda_approvals,
    AVG(ip.patent_legal_events.maintenance_fee) as avg_patent_value,
    SUM(sci.fda_adverse_events.serious_outcomes) as safety_events
FROM ip.patent_grants
JOIN ip.patent_classifications ON ip.patent_grants.patent_number = ip.patent_classifications.patent_number
LEFT JOIN sci.fda_drug_approvals ON ip.patent_grants.assignee_name = sci.fda_drug_approvals.company_name
LEFT JOIN sci.fda_adverse_events ON sci.fda_drug_approvals.product_name = sci.fda_adverse_events.drug_name
WHERE ip.patent_classifications.technology_sector = 'Pharmaceuticals'
  AND ip.patent_grants.grant_date >= '2015-01-01'
GROUP BY ip.patent_grants.assignee_name
ORDER BY pharma_patents DESC;

-- University technology transfer and startup creation
SELECT 
    ip.patent_grants.assignee_name as university,
    COUNT(ip.patent_grants.patent_number) as university_patents,
    COUNT(ip.patent_assignments.assignment_id) as tech_transfers,
    COUNT(DISTINCT startup.company_name) as spinoff_companies,
    AVG(ip.patent_citations.forward_citations) as innovation_impact
FROM ip.patent_grants
JOIN ip.patent_assignments ON ip.patent_grants.patent_number = ip.patent_assignments.patent_number
LEFT JOIN (
    SELECT assignee_name as company_name, MIN(filing_date) as founding_date
    FROM ip.patent_grants 
    WHERE assignee_type = 'startup'
    GROUP BY assignee_name
) startup ON ip.patent_assignments.assignee LIKE '%' || startup.company_name || '%'
WHERE ip.patent_grants.assignee_type = 'university'
  AND ip.patent_grants.grant_date >= '2010-01-01'
GROUP BY ip.patent_grants.assignee_name
ORDER BY tech_transfers DESC;
```

## Storage Estimates
- **Patent grants and applications**: ~50GB (full text, claims, images)
- **Patent citations and classifications**: ~15GB (citation networks)
- **Patent assignments and legal events**: ~5GB (ownership changes)
- **Trademark registrations and applications**: ~10GB (mark data and images)
- **PTAB and litigation data**: ~5GB (administrative proceedings)
- **International IP data**: ~20GB (global patent families)
- **Innovation analytics tables**: ~5GB (computed metrics)

**Total Phase 1-5**: ~110GB compressed Parquet

## Data Processing Challenges
- **Document parsing**: Extract structured data from patent PDF documents
- **Image processing**: Handle patent drawings and trademark logos
- **Entity resolution**: Match inventors, assignees across different formats
- **Citation extraction**: Parse patent citations from full text
- **Classification mapping**: Handle evolving patent classification systems
- **International harmonization**: Reconcile different patent office data formats

## Advanced Analytics Capabilities
- **Patent citation network analysis**: Map technology evolution and influence
- **Innovation cluster identification**: Identify technology hubs and ecosystems
- **Competitive intelligence**: Track competitor patent strategies
- **Technology landscape mapping**: Visualize patent landscapes by sector
- **Patent valuation models**: Estimate patent value based on citations and market factors
- **Inventor mobility tracking**: Analyze talent flow between organizations

## Compliance and Legal Considerations
- **Patent data accuracy**: Ensure legal status information is current
- **Fair use compliance**: Respect USPTO terms of service for bulk data
- **International data agreements**: Comply with foreign patent office data terms
- **Privacy protection**: Handle inventor personal information appropriately
- **Commercial use guidelines**: Understand restrictions on patent data commercialization

This creates the most comprehensive intellectual property analysis platform available, enabling unprecedented insights into innovation patterns, technology transfer, and the relationship between research funding, patent output, and economic outcomes.