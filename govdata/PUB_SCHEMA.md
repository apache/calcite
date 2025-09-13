# Public Data Schema Design

## Overview

Create a new `pub` (public data) schema within the govdata adapter to provide SQL access to ubiquitous public data sources including Wikipedia, OpenStreetMap, Wikidata, and academic/research databases. This schema complements government data with comprehensive public knowledge and geographic intelligence.

## Data Sources

### Wikipedia
1. **Wikipedia API** (Free, public access)
   - Article content, summaries, and extracts
   - Page metadata, categories, and links
   - Edit history and quality metrics
   - Search and navigation capabilities
   - Rate limit: 200 requests/second for anonymous users

2. **Wikipedia Dumps** (Complete datasets)
   - Full article content in multiple formats
   - Structured data extraction from infoboxes
   - Category hierarchies and page relationships
   - Historical versions and change tracking

### OpenStreetMap (OSM)
1. **Overpass API** (Real-time querying)
   - Buildings, roads, points of interest
   - Administrative boundaries and infrastructure
   - Real-time geographic data updates
   - Custom spatial queries and filtering

2. **Planet OSM** (Complete dataset)
   - Global geographic database (~60GB compressed)
   - Regional extracts for specific areas
   - Complete building footprints and addresses
   - Transportation networks and utility infrastructure

### Wikidata
1. **Wikidata Query Service** (SPARQL endpoint)
   - 45+ million structured entities
   - Property-value relationships with sources
   - Multilingual labels and descriptions
   - Temporal information with precision indicators

2. **Wikidata Dumps** (Complete knowledge base)
   - Entity relationships and properties
   - Cross-reference identifiers to external databases
   - Historical data with time-based qualifiers
   - Machine-readable structured knowledge

### Academic and Research Data
1. **OpenAlex** (Open academic database)
   - Research publications and citations
   - Author profiles and institutional affiliations
   - Research topics and concept mapping
   - Open access without API keys required

2. **arXiv** (Preprint repository)
   - Physics, mathematics, computer science preprints
   - Author networks and research collaboration
   - Real-time research publication tracking
   - Full-text search and analysis capabilities

## Proposed Tables

### Wikipedia Knowledge Base

1. **`wikipedia_articles`** - Core article content and metadata
   - Columns: page_id, title, extract, full_text, last_modified, page_views, quality_score, language, categories, infobox_data
   - Primary key: (page_id, language)
   - Partitioned by: language, category
   - Full-text searchable content with structured metadata extraction

2. **`wikipedia_entities`** - Structured entity information
   - Columns: page_id, entity_type, coordinates, founded_date, industry, population, area_km2, key_people, related_entities
   - Primary key: page_id
   - Extracted and structured information from Wikipedia infoboxes

3. **`wikipedia_links`** - Internal and external link relationships
   - Columns: source_page_id, target_page_id, link_type, external_url, link_text, section
   - Primary key: (source_page_id, target_page_id, link_type)
   - Maps relationships between Wikipedia articles and external resources

4. **`wikipedia_categories`** - Category hierarchy and page classifications
   - Columns: category_name, parent_categories, subcategories, page_count, category_depth
   - Primary key: category_name
   - Hierarchical organization of Wikipedia content

### Geographic Intelligence (OpenStreetMap)

5. **`osm_buildings`** - Building footprints and property information
   - Columns: osm_id, building_type, address, geometry, height, levels, construction_year, amenities, business_name, phone, website
   - Primary key: osm_id
   - Partitioned by: country_code, state_code
   - Precise building-level geographic data with business information

6. **`osm_transportation`** - Roads, transit, and transportation infrastructure
   - Columns: osm_id, transport_type, route_name, geometry, max_speed, lanes, surface_type, access_restrictions, public_transit
   - Primary key: osm_id
   - Partitioned by: country_code, transport_type
   - Comprehensive transportation network analysis

7. **`osm_amenities`** - Points of interest and service locations
   - Columns: osm_id, amenity_type, name, address, geometry, opening_hours, contact_info, wheelchair_accessible, capacity
   - Primary key: osm_id
   - Partitioned by: country_code, amenity_type
   - Business locations, services, and community amenities

8. **`osm_administrative`** - Administrative boundaries and governance
   - Columns: osm_id, admin_level, name, geometry, population, official_name, iso_code, postal_codes
   - Primary key: osm_id
   - Detailed administrative boundaries complementing government geographic data

### Structured Knowledge (Wikidata)

9. **`wikidata_entities`** - Core entity database
   - Columns: entity_id, entity_type, labels, descriptions, aliases, coordinates, instance_of, subclass_of, external_ids
   - Primary key: entity_id
   - Multilingual entity information with type classifications

10. **`wikidata_properties`** - Property definitions and relationships
    - Columns: property_id, property_name, property_type, description, domain, range, equivalent_properties
    - Primary key: property_id
    - Defines relationship types and data properties in knowledge graph

11. **`wikidata_statements`** - Facts and claims about entities
    - Columns: entity_id, property_id, value, value_type, qualifiers, references, rank, start_date, end_date
    - Primary key: (entity_id, property_id, value)
    - Partitioned by: entity_type, property_id
    - Temporal facts with source citations and confidence ranking

12. **`wikidata_references`** - Source citations and evidence
    - Columns: reference_id, statement_id, source_type, source_url, publication_date, author, title
    - Primary key: reference_id
    - Citation network supporting factual claims

### Academic and Research Data

13. **`research_publications`** - Academic papers and research output
    - Columns: publication_id, title, abstract, authors, institutions, publication_date, journal, doi, citation_count, research_topics
    - Primary key: publication_id
    - Partitioned by: publication_year, research_field
    - Comprehensive academic literature database

14. **`research_authors`** - Researcher profiles and affiliations
    - Columns: author_id, name, current_institution, past_institutions, research_areas, h_index, total_citations, orcid_id
    - Primary key: author_id
    - Academic researcher network and expertise mapping

15. **`research_institutions`** - Universities and research organizations
    - Columns: institution_id, name, location, country, institution_type, research_output, ranking, founded_year
    - Primary key: institution_id
    - Academic institutional profiles and research capacity

16. **`patent_database`** - Innovation and technology transfer
    - Columns: patent_id, title, abstract, inventors, assignee, filing_date, grant_date, technology_class, citations
    - Primary key: patent_id
    - Partitioned by: filing_year, technology_class
    - Innovation patterns and technology transfer analysis

### Entity Resolution and Integration

17. **`entity_mappings`** - Cross-reference identifiers between systems
    - Columns: source_system, source_id, target_system, target_id, confidence_score, mapping_type, last_verified
    - Primary key: (source_system, source_id, target_system, target_id)
    - Links entities across Wikipedia, Wikidata, government data, and research databases

18. **`knowledge_graph`** - Comprehensive entity relationship network
    - Columns: subject_entity, relationship_type, object_entity, confidence_score, source_system, temporal_scope
    - Primary key: (subject_entity, relationship_type, object_entity)
    - Unified relationship graph across all data sources

## Cross-Schema Relationships

### Links to Government Data (SEC, GEO, ECON, SAFETY)
- **Company Intelligence**: SEC companies linked to Wikipedia articles and Wikidata entities
- **Executive Networks**: Corporate leadership mapped through biographical data and career histories  
- **Geographic Enhancement**: Government geographic data enriched with OSM building-level detail
- **Research Validation**: Economic and policy research correlated with academic literature

### Entity Resolution Examples
```sql
-- Link SEC companies to comprehensive public knowledge
SELECT 
    s.company_name,
    s.cik,
    w.wikipedia_url,
    w.founded_date,
    w.founder_names,
    wd.headquarters_coordinates,
    osm.building_details,
    research.related_publications
FROM sec.company_metadata s
JOIN pub.entity_mappings em ON s.cik = em.source_id 
JOIN pub.wikipedia_entities w ON em.target_id = w.page_id
JOIN pub.wikidata_entities wd ON w.wikidata_id = wd.entity_id
JOIN pub.osm_buildings osm ON ST_Contains(osm.geometry, wd.headquarters_coordinates)
JOIN pub.research_publications research ON research.company_mentions LIKE '%' || s.company_name || '%';
```

### Geographic Intelligence Integration
```sql
-- Enhanced location risk assessment with building-level detail
SELECT 
    geo.county_name,
    safety.crime_rate,
    econ.unemployment_rate,
    osm.business_density,
    osm.transportation_access_score,
    osm.amenity_quality_index,
    wiki.historical_context,
    research.local_research_activity
FROM geo.counties geo
JOIN safety.crime_summary safety ON geo.county_fips = safety.county_fips
JOIN econ.regional_employment econ ON geo.county_fips = econ.area_code
JOIN pub.osm_administrative osm ON geo.county_fips = osm.fips_code
JOIN pub.wikipedia_entities wiki ON geo.county_name = wiki.place_name
JOIN pub.research_institutions research ON ST_Contains(geo.geometry, research.location);
```

## Implementation Approach

### Phase 1: Wikipedia Foundation (Months 1-3)
1. Set up Wikipedia API integration with rate limiting and caching
2. Implement wikipedia_articles table with full-text search capabilities  
3. Create wikipedia_entities table with structured data extraction
4. Build entity linking to SEC companies and geographic locations
5. Add wikipedia_links and categories for relationship mapping

### Phase 2: Geographic Enhancement (Months 4-6)
1. Integrate OpenStreetMap data feeds and real-time updates
2. Implement osm_buildings table with precise location data
3. Create osm_transportation and osm_amenities tables
4. Build spatial indexing and geographic query optimization
5. Link OSM data to government geographic boundaries

### Phase 3: Structured Knowledge (Months 7-9)
1. Connect to Wikidata Query Service and dump processing
2. Implement wikidata_entities and properties tables
3. Create wikidata_statements with temporal and source information
4. Build comprehensive entity_mappings across all systems
5. Construct unified knowledge_graph with relationship scoring

### Phase 4: Academic and Research Integration (Months 10-12)
1. Integrate OpenAlex and arXiv research databases
2. Implement research_publications and authors tables
3. Create patent_database for innovation analysis
4. Build research network analysis and collaboration mapping
5. Link research output to geographic and economic indicators

## Query Examples

### Corporate Intelligence Enhancement
```sql
-- Comprehensive company background analysis
SELECT 
    s.company_name,
    w.founded_date,
    w.founder_biographies,
    w.major_events_timeline,
    wd.subsidiary_companies,
    research.research_partnerships,
    patents.innovation_portfolio
FROM sec.financial_line_items s
JOIN pub.wikipedia_entities w ON s.company_name = w.company_name
JOIN pub.wikidata_entities wd ON w.wikidata_id = wd.entity_id
JOIN pub.research_publications research ON research.company_affiliations LIKE '%' || s.company_name || '%'
JOIN pub.patent_database patents ON patents.assignee = s.company_name
WHERE s.fiscal_year = 2023;
```

### Location Intelligence and Site Selection
```sql
-- Advanced location analysis with multiple data dimensions
SELECT 
    osm.address,
    osm.building_type,
    safety.crime_incidents_nearby,
    econ.economic_indicators,
    osm.transportation_scores,
    osm.amenity_access,
    wiki.neighborhood_context,
    research.local_innovation_activity
FROM pub.osm_buildings osm
JOIN safety.crime_incidents safety ON ST_DWithin(osm.geometry, safety.location, 500)
JOIN econ.census_blocks econ ON ST_Contains(econ.geometry, osm.geometry)
JOIN pub.wikipedia_entities wiki ON wiki.coordinates = ST_Centroid(osm.geometry)
JOIN pub.research_institutions research ON ST_DWithin(osm.geometry, research.location, 5000)
WHERE osm.building_type = 'commercial' 
  AND osm.country_code = 'US';
```

### Research and Innovation Analysis
```sql
-- Academic research impact on local economic development
SELECT 
    uni.institution_name,
    geo.county_name,
    COUNT(pub.publication_id) as research_output,
    AVG(pub.citation_count) as research_impact,
    COUNT(patents.patent_id) as innovation_output,
    econ.high_tech_employment_growth,
    econ.business_formation_rate
FROM pub.research_institutions uni
JOIN geo.counties geo ON ST_Contains(geo.geometry, uni.location)
JOIN pub.research_publications pub ON uni.institution_id = pub.institution_id
JOIN pub.patent_database patents ON patents.inventors LIKE '%' || uni.institution_name || '%'
JOIN econ.regional_indicators econ ON geo.county_fips = econ.area_code
WHERE pub.publication_date >= '2020-01-01'
GROUP BY uni.institution_id, geo.county_fips;
```

### Cross-Domain Knowledge Discovery
```sql
-- Natural language query: "Companies founded by Stanford alumni in low-crime areas"
SELECT 
    companies.company_name,
    founders.name as founder_name,
    founders.education,
    companies.headquarters_location,
    safety.violent_crime_rate,
    safety.property_crime_rate,
    osm.neighborhood_quality_score
FROM pub.wikidata_statements founder_education
JOIN pub.wikidata_entities founders ON founder_education.entity_id = founders.entity_id
JOIN pub.wikidata_statements company_founder ON company_founder.object_entity = founders.entity_id
JOIN pub.wikidata_entities companies ON company_founder.entity_id = companies.entity_id
JOIN pub.osm_buildings osm ON ST_Contains(osm.geometry, companies.headquarters_coordinates)
JOIN safety.public_safety_index safety ON safety.area_code = osm.county_fips
WHERE founder_education.property_id = 'P69' -- educated at
  AND founder_education.value = 'Q41506' -- Stanford University
  AND company_founder.property_id = 'P112' -- founded by
  AND companies.instance_of = 'Q4830453' -- business enterprise
  AND safety.violent_crime_rate < 300; -- per 100k population
```

## Performance Considerations

### Data Volume Estimates
- **Wikipedia**: ~50GB (English articles), ~200GB (all languages)
- **OpenStreetMap**: ~60GB (global), ~5GB (United States)
- **Wikidata**: ~100GB (complete dump with all statements)
- **Research Publications**: ~500GB (OpenAlex complete database)
- **Entity Mappings**: ~10GB (cross-reference tables)

### Optimization Strategies
1. **Spatial Indexing**: R-tree and PostGIS-style indices for OSM geographic queries
2. **Full-Text Search**: Elasticsearch or similar for Wikipedia content search
3. **Graph Indexing**: Specialized indices for knowledge graph traversal
4. **Materialized Views**: Pre-computed entity relationships and popular queries
5. **Partitioning**: Geographic and temporal partitioning for performance
6. **Caching**: Intelligent caching of frequently accessed entities and relationships

## Update Frequency and Data Freshness

- **Wikipedia**: Real-time updates via change stream API
- **OpenStreetMap**: Real-time updates via Overpass API and change feeds
- **Wikidata**: Weekly dumps with daily incremental updates
- **Research Publications**: Daily updates from OpenAlex and arXiv
- **Entity Mappings**: Continuous improvement through automated and manual processes

## API Configuration

```json
{
  "schemas": [{
    "name": "pub",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "pub",
      "wikipediaLanguages": ["en", "es", "fr", "de"],
      "osmRegions": ["us", "canada", "mexico"],
      "wikidataEndpoint": "https://query.wikidata.org/sparql",
      "openalexApiKey": "${OPENALEX_API_KEY}",
      "updateFrequency": "daily",
      "entityLinking": {
        "enabled": true,
        "confidenceThreshold": 0.8,
        "autoMapping": true
      },
      "spatialAnalysis": {
        "enabled": true,
        "coordinateSystem": "EPSG:4326",
        "bufferAnalysis": ["100m", "500m", "1km", "5km"]
      },
      "cacheDirectory": "${PUB_CACHE_DIR:/tmp/pub-cache}"
    }
  }]
}
```

## Dependencies and Integration Requirements

### External APIs and Services
- Wikipedia API and dump processing
- OpenStreetMap Overpass API and Planet OSM
- Wikidata Query Service and SPARQL endpoint
- OpenAlex research publication API
- arXiv submission and metadata feeds

### Processing Libraries
- **Spatial Analysis**: PostGIS, JTS, GeoTools for geographic processing
- **Natural Language Processing**: Stanford NLP, spaCy for entity extraction
- **Graph Processing**: Apache TinkerPop, Neo4j for knowledge graph operations
- **Full-Text Search**: Apache Lucene, Elasticsearch for content search
- **Machine Learning**: Scikit-learn, TensorFlow for entity linking and classification

## Security and Compliance

### Data Access and Licensing
- **Wikipedia**: CC BY-SA license requires attribution and share-alike
- **OpenStreetMap**: ODbL license allows commercial use with attribution
- **Wikidata**: CC0 public domain, freely usable
- **Research Publications**: Varies by source, mostly open access
- **Entity Linking**: Derived data follows most restrictive source license

### Privacy and Ethical Considerations
- **Public Data Only**: All sources are publicly available information
- **Attribution Requirements**: Proper citation and source attribution
- **Bias Awareness**: Documentation of known biases in Wikipedia and OSM
- **Cultural Sensitivity**: Respectful handling of cultural and historical information

## Future Enhancements

### Advanced Analytics Capabilities
1. **AI-Powered Entity Linking**: Machine learning models for improved entity resolution
2. **Knowledge Graph Completion**: Automated inference of missing relationships
3. **Semantic Search**: Natural language queries across all public knowledge
4. **Temporal Analysis**: Historical trend analysis and change detection

### International Expansion
1. **Multi-Language Support**: Wikipedia and Wikidata in multiple languages
2. **Global Geographic Coverage**: OSM data for international markets
3. **Cultural Adaptation**: Region-specific knowledge and cultural context
4. **Local Research Integration**: Country-specific academic and research databases

### Advanced Integration
1. **Social Media Intelligence**: Integration with public social media APIs
2. **News and Media Monitoring**: Real-time news analysis and fact-checking
3. **Government Open Data**: Integration with international open data portals
4. **Corporate Intelligence**: Enhanced business intelligence through public filings globally

This comprehensive public data schema transforms the govdata platform from government-focused to comprehensive public knowledge intelligence, enabling unprecedented analytical capabilities across domains while maintaining the same architectural patterns and performance characteristics as existing schemas.