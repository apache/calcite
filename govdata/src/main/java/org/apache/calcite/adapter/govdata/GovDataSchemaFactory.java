/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.metadata.InformationSchema;
import org.apache.calcite.adapter.file.metadata.PostgreSqlCatalogSchema;
import org.apache.calcite.adapter.govdata.econ.EconSchemaFactory;
import org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory;
import org.apache.calcite.adapter.govdata.pub.PubSchemaFactory;
import org.apache.calcite.adapter.govdata.safety.SafetySchemaFactory;
import org.apache.calcite.adapter.govdata.sec.SecSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Government Data Schema Factory - Uber factory for government data sources.
 *
 * <p>This factory routes to specialized factories based on the 'dataSource' 
 * parameter. Supported data sources:
 * <ul>
 *   <li>sec - Securities and Exchange Commission (EDGAR filings)</li>
 *   <li>geo - Geographic data (Census TIGER, HUD crosswalk, demographics)</li>
 *   <li>econ - Economic data (BLS employment, FRED indicators, Treasury yields)</li>
 *   <li>safety - Public safety data (FBI crime, NHTSA traffic, FEMA disasters)</li>
 *   <li>pub - Public data (Wikipedia, OpenStreetMap, Wikidata, academic research)</li>
 *   <li>census - U.S. Census Bureau data (future)</li>
 *   <li>irs - Internal Revenue Service data (future)</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GOV",
 *   "schemas": [{
 *     "name": "GOV",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "sec",
 *       "ciks": ["AAPL", "MSFT"],
 *       "startYear": 2020,
 *       "endYear": 2023
 *     }
 *   }]
 * }
 * </pre>
 */
public class GovDataSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSchemaFactory.class);
  
  // Store constraint metadata to pass to sub-factories
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;
  
  // Track schemas created in this model for cross-domain constraint detection
  private final Map<String, Schema> createdSchemas = new HashMap<>();
  private final Map<String, String> schemaDataSources = new HashMap<>();

  @Override public Schema create(SchemaPlus parentSchema, String name, 
      Map<String, Object> operand) {
    
    String dataSource = (String) operand.get("dataSource");
    
    // Default to SEC for backward compatibility if no dataSource specified
    if (dataSource == null) {
      dataSource = "sec";
      LOGGER.info("No dataSource specified, defaulting to 'sec'");
    }
    
    LOGGER.info("Creating government data schema for source: {}", dataSource);
    
    // Don't add metadata schemas here - FileSchemaFactory will add them
    // Adding them in both places causes circular references and stack overflow
    // addMetadataSchemas(parentSchema);
    
    switch (dataSource.toLowerCase()) {
      case "sec":
      case "edgar":
        return createSecSchema(parentSchema, name, operand);
      
      case "geo":
      case "geographic":
        return createGeoSchema(parentSchema, name, operand);
      
      case "econ":
      case "economic":
      case "economy":
        return createEconSchema(parentSchema, name, operand);
      
      case "safety":
      case "crime":
      case "publicsafety":
      case "public_safety":
        return createSafetySchema(parentSchema, name, operand);
      
      case "pub":
      case "public":
      case "wikipedia":
      case "osm":
      case "openstreetmap":
        return createPubSchema(parentSchema, name, operand);
      
      case "census":
        throw new UnsupportedOperationException(
            "Census data source not yet implemented. Coming soon!");
        
      case "irs":
        throw new UnsupportedOperationException(
            "IRS data source not yet implemented. Coming soon!");
        
      default:
        throw new IllegalArgumentException(
            "Unsupported government data source: '" + dataSource + "'. " +
            "Supported sources: sec, geo, econ, safety, pub, census (future), irs (future)");
    }
  }
  
  /**
   * Creates SEC/EDGAR schema using the specialized SEC factory.
   */
  private Schema createSecSchema(SchemaPlus parentSchema, String name, 
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to SecSchemaFactory for SEC/EDGAR data");
    
    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), "SEC");
    
    SecSchemaFactory factory = new SecSchemaFactory();
    
    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }
    
    // Add cross-domain constraints if GEO schema exists
    if (schemaDataSources.containsValue("GEO")) {
      Map<String, Map<String, Object>> crossDomainConstraints = defineCrossDomainConstraintsForSec();
      allConstraints.putAll(crossDomainConstraints);
    }
    
    if (!allConstraints.isEmpty() && tableDefinitions != null) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }
    
    Schema schema = factory.create(parentSchema, name, operand);
    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }
  
  /**
   * Creates Geographic data schema using the specialized Geo factory.
   */
  private Schema createGeoSchema(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to GeoSchemaFactory for geographic data");
    
    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), "GEO");
    
    GeoSchemaFactory factory = new GeoSchemaFactory();
    
    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }
    
    // Add cross-domain constraints if SEC schema exists
    if (schemaDataSources.containsValue("SEC")) {
      // GEO doesn't have outgoing FKs to SEC, but we track it for completeness
      LOGGER.debug("SEC schema exists - cross-domain relationships available");
    }
    
    if (!allConstraints.isEmpty() && tableDefinitions != null) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }
    
    Schema schema = factory.create(parentSchema, name, operand);
    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }
  
  /**
   * Creates Economic data schema using the specialized Econ factory.
   */
  private Schema createEconSchema(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to EconSchemaFactory for economic data");
    
    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), "ECON");
    
    EconSchemaFactory factory = new EconSchemaFactory();
    
    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }
    
    // Add cross-domain constraints if SEC or GEO schemas exist
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - economic/financial correlations available");
    }
    if (schemaDataSources.containsValue("GEO")) {
      LOGGER.debug("GEO schema exists - regional economic analysis available");
      Map<String, Map<String, Object>> crossDomainConstraints = defineCrossDomainConstraintsForEcon();
      allConstraints.putAll(crossDomainConstraints);
    }
    
    if (!allConstraints.isEmpty() && tableDefinitions != null) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }
    
    Schema schema = factory.create(parentSchema, name, operand);
    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }
  
  /**
   * Creates Public Safety data schema using the specialized Safety factory.
   */
  private Schema createSafetySchema(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to SafetySchemaFactory for public safety data");
    
    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), "SAFETY");
    
    SafetySchemaFactory factory = new SafetySchemaFactory();
    
    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }
    
    // Add cross-domain constraints if other schemas exist
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - business risk assessment available");
    }
    if (schemaDataSources.containsValue("GEO")) {
      LOGGER.debug("GEO schema exists - spatial crime/safety analysis available");
    }
    if (schemaDataSources.containsValue("ECON")) {
      LOGGER.debug("ECON schema exists - socioeconomic crime correlations available");
    }
    
    if (!allConstraints.isEmpty() && tableDefinitions != null) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }
    
    Schema schema = factory.create(parentSchema, name, operand);
    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }
  
  /**
   * Creates Public data schema using the specialized Pub factory.
   */
  private Schema createPubSchema(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.debug("Delegating to PubSchemaFactory for public data");
    
    // Track this schema for cross-domain constraint detection
    schemaDataSources.put(name.toUpperCase(), "PUB");
    
    PubSchemaFactory factory = new PubSchemaFactory();
    
    // Build constraint metadata including cross-domain constraints
    Map<String, Map<String, Object>> allConstraints = new HashMap<>();
    if (tableConstraints != null) {
      allConstraints.putAll(tableConstraints);
    }
    
    // Add cross-domain constraints if other schemas exist
    if (schemaDataSources.containsValue("SEC")) {
      LOGGER.debug("SEC schema exists - corporate intelligence enhancement available");
    }
    if (schemaDataSources.containsValue("GEO")) {
      LOGGER.debug("GEO schema exists - geographic context enrichment available");
    }
    if (schemaDataSources.containsValue("ECON")) {
      LOGGER.debug("ECON schema exists - economic research correlation available");
    }
    if (schemaDataSources.containsValue("SAFETY")) {
      LOGGER.debug("SAFETY schema exists - contextual safety analysis available");
    }
    
    if (!allConstraints.isEmpty() && tableDefinitions != null) {
      factory.setTableConstraints(allConstraints, tableDefinitions);
    }
    
    Schema schema = factory.create(parentSchema, name, operand);
    createdSchemas.put(name.toUpperCase(), schema);
    return schema;
  }
  
  @Override
  public boolean supportsConstraints() {
    // Enable constraint support for all government data sources
    return true;
  }
  
  @Override
  public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables", 
        tableConstraints != null ? tableConstraints.size() : 0);
  }
  
  /**
   * Defines cross-domain foreign key constraints from SEC tables to GEO tables.
   * These are automatically added when both SEC and GEO schemas are present in the model.
   * 
   * @return Map of table names to their cross-domain constraint definitions
   */
  private Map<String, Map<String, Object>> defineCrossDomainConstraintsForSec() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    
    // Find the GEO schema name
    String geoSchemaName = null;
    for (Map.Entry<String, String> entry : schemaDataSources.entrySet()) {
      if ("GEO".equals(entry.getValue())) {
        geoSchemaName = entry.getKey();
        break;
      }
    }
    
    if (geoSchemaName == null) {
      return constraints;
    }
    
    // Define FK from filing_metadata.state_of_incorporation to tiger_states.state_code
    Map<String, Object> filingMetadataConstraints = new HashMap<>();
    Map<String, Object> stateIncorpFK = new HashMap<>();
    stateIncorpFK.put("columns", Arrays.asList("state_of_incorporation"));
    stateIncorpFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    stateIncorpFK.put("targetColumns", Arrays.asList("state_code"));
    
    filingMetadataConstraints.put("foreignKeys", Arrays.asList(stateIncorpFK));
    constraints.put("filing_metadata", filingMetadataConstraints);
    
    LOGGER.info("Added cross-domain FK constraint: filing_metadata.state_of_incorporation -> {}.tiger_states.state_code", 
        geoSchemaName);
    
    // Future: Add more cross-domain constraints as needed
    // e.g., insider_transactions.insider_state -> tiger_states.state_code
    // e.g., company locations -> census_places
    
    return constraints;
  }
  
  /**
   * Defines cross-domain foreign key constraints from ECON tables to GEO tables.
   * These are automatically added when both ECON and GEO schemas are present in the model.
   * 
   * @return Map of table names to their cross-domain constraint definitions
   */
  private Map<String, Map<String, Object>> defineCrossDomainConstraintsForEcon() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    
    // Find the GEO schema name
    String geoSchemaName = null;
    for (Map.Entry<String, String> entry : schemaDataSources.entrySet()) {
      if ("GEO".equals(entry.getValue())) {
        geoSchemaName = entry.getKey();
        break;
      }
    }
    
    if (geoSchemaName == null) {
      return constraints;
    }
    
    // regional_employment.state_code -> tiger_states.state_code (2-letter codes)
    Map<String, Object> regionalEmploymentConstraints = new HashMap<>();
    List<Map<String, Object>> regionalEmploymentFks = new ArrayList<>();
    
    Map<String, Object> regionalEmploymentToStatesFK = new HashMap<>();
    regionalEmploymentToStatesFK.put("columns", Arrays.asList("state_code"));
    regionalEmploymentToStatesFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    regionalEmploymentToStatesFK.put("targetColumns", Arrays.asList("state_code"));
    regionalEmploymentFks.add(regionalEmploymentToStatesFK);
    
    regionalEmploymentConstraints.put("foreignKeys", regionalEmploymentFks);
    constraints.put("regional_employment", regionalEmploymentConstraints);
    
    LOGGER.info("Added cross-domain FK constraint: regional_employment.state_code -> {}.tiger_states.state_code", 
        geoSchemaName);
    
    // regional_income.geo_fips -> tiger_states.state_fips (FIPS codes)
    // NOTE: This is a partial FK - only valid when geo_fips contains 2-digit state codes
    Map<String, Object> regionalIncomeConstraints = new HashMap<>();
    List<Map<String, Object>> regionalIncomeFks = new ArrayList<>();
    
    Map<String, Object> regionalIncomeToStatesFK = new HashMap<>();
    regionalIncomeToStatesFK.put("columns", Arrays.asList("geo_fips"));
    regionalIncomeToStatesFK.put("targetTable", Arrays.asList(geoSchemaName, "tiger_states"));
    regionalIncomeToStatesFK.put("targetColumns", Arrays.asList("state_fips"));
    regionalIncomeFks.add(regionalIncomeToStatesFK);
    
    regionalIncomeConstraints.put("foreignKeys", regionalIncomeFks);
    constraints.put("regional_income", regionalIncomeConstraints);
    
    LOGGER.info("Added cross-domain FK constraint: regional_income.geo_fips -> {}.tiger_states.state_fips (partial FK for state-level data)", 
        geoSchemaName);
    
    // state_gdp.geo_fips -> tiger_states.state_fips (FIPS codes)
    Map<String, Object> stateGdpConstraints = new HashMap<>();
    List<Map<String, Object>> stateGdpFks = new ArrayList<>();
    
    Map<String, Object> stateGdpToStatesFK = new HashMap<>();
    stateGdpToStatesFK.put("columns", Arrays.asList("geo_fips"));
    stateGdpToStatesFK.put("targetSchema", geoSchemaName);
    stateGdpToStatesFK.put("targetTable", "tiger_states");
    stateGdpToStatesFK.put("targetColumns", Arrays.asList("state_fips"));
    stateGdpFks.add(stateGdpToStatesFK);
    
    stateGdpConstraints.put("foreignKeys", stateGdpFks);
    constraints.put("state_gdp", stateGdpConstraints);
    
    LOGGER.info("Added cross-domain FK constraint: state_gdp.geo_fips -> {}.tiger_states.state_fips", 
        geoSchemaName);
    
    return constraints;
  }
  
  /**
   * Adds metadata schemas (information_schema, pg_catalog, and metadata) to the parent schema.
   * This provides SQL-standard access to table and column metadata including comments.
   * Includes PostgreSQL-compatible pg_catalog for schema comments.
   */
  private void addMetadataSchemas(SchemaPlus parentSchema) {
    // Find root schema
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }
    
    // Only add metadata schemas if they don't already exist
    if (rootSchema.subSchemas().get("information_schema") == null) {
      LOGGER.debug("Adding information_schema to root");
      InformationSchema informationSchema = new InformationSchema(rootSchema, "CALCITE");
      rootSchema.add("information_schema", informationSchema);
    }
    
    // Add PostgreSQL catalog schema for schema comments
    if (rootSchema.subSchemas().get("pg_catalog") == null) {
      LOGGER.debug("Adding pg_catalog schema to root for PostgreSQL compatibility");
      PostgreSqlCatalogSchema pgCatalog = new PostgreSqlCatalogSchema(rootSchema, "CALCITE");
      rootSchema.add("pg_catalog", pgCatalog);
    }
    
    // Legacy metadata schema for backward compatibility
    if (rootSchema.subSchemas().get("metadata") == null) {
      LOGGER.debug("Adding metadata schema to root");
      InformationSchema metadataSchema = new InformationSchema(rootSchema, "CALCITE");
      rootSchema.add("metadata", metadataSchema);
    }
    
    // Don't add reference to metadata schema at current level - it causes circular references
    // when FileSchemaFactory also adds metadata schemas. The metadata schemas are already
    // accessible from the root level.
  }
}