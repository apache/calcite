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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for geographic data schemas that provides access to U.S. government
 * geographic datasets.
 *
 * <p>This factory leverages the file adapter's infrastructure for HTTP operations
 * and Parquet storage, similar to the SEC adapter implementation.
 *
 * <p>Supported data sources:
 * <ul>
 *   <li>Census TIGER/Line boundary files</li>
 *   <li>Census demographic/economic data via API</li>
 *   <li>HUD-USPS ZIP code crosswalk</li>
 *   <li>Census geocoding services</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GEO",
 *   "schemas": [{
 *     "name": "GEO",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory",
 *     "operand": {
 *       "cacheDir": "/path/to/geo-cache",
 *       "censusApiKey": "your-free-api-key",
 *       "hudUsername": "your-hud-username",
 *       "hudPassword": "your-hud-password",
 *       "enabledSources": ["tiger", "census", "hud"],
 *       "dataYear": 2024,
 *       "autoDownload": true
 *     }
 *   }]
 * }
 * </pre>
 */
public class GeoSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoSchemaFactory.class);

  // Store constraint metadata from model files
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  // Default unified government data directory
  private static final String DEFAULT_GOVDATA_HOME = "~/govdata-parquet";
  private static final String GOVDATA_HOME = System.getProperty("govdata.home", DEFAULT_GOVDATA_HOME);

  // Hive-partitioned structure for geographic data
  // Base: /govdata-parquet/source=geo/type={boundary,demographic,crosswalk}/...
  private static final String GEO_SOURCE_PARTITION = "source=geo";
  private static final String BOUNDARY_TYPE = "type=boundary";  // TIGER data
  private static final String DEMOGRAPHIC_TYPE = "type=demographic";  // Census API data
  private static final String CROSSWALK_TYPE = "type=crosswalk";  // HUD data

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    LOGGER.info("Creating geographic data schema: {}", name);

    // Extract configuration parameters
    // Calcite's ModelHandler automatically substitutes ${ENV_VAR} placeholders
    String cacheDir = (String) operand.getOrDefault("cacheDir", GOVDATA_HOME);
    String censusApiKey = (String) operand.get("censusApiKey");
    String hudUsername = (String) operand.get("hudUsername");
    String hudPassword = (String) operand.get("hudPassword");
    String hudToken = (String) operand.get("hudToken");

    // Data source configuration
    Object enabledSourcesObj = operand.get("enabledSources");
    String[] enabledSources;
    if (enabledSourcesObj instanceof String[]) {
      enabledSources = (String[]) enabledSourcesObj;
    } else if (enabledSourcesObj instanceof java.util.List) {
      java.util.List<?> list = (java.util.List<?>) enabledSourcesObj;
      enabledSources = list.toArray(new String[0]);
    } else {
      // Default to all sources if not specified
      enabledSources = new String[]{"tiger", "census", "hud"};
    }

    Integer dataYear = (Integer) operand.getOrDefault("dataYear", 2024);
    Boolean autoDownload = (Boolean) operand.getOrDefault("autoDownload", true);

    // Create cache directory structure
    File cacheRoot = new File(cacheDir);
    if (!cacheRoot.exists()) {
      if (!cacheRoot.mkdirs()) {
        throw new RuntimeException("Failed to create cache directory: " + cacheDir);
      }
    }

    // Create hive-partitioned directory structure
    File geoSourceDir = new File(cacheRoot, GEO_SOURCE_PARTITION);
    File boundaryDir = new File(geoSourceDir, BOUNDARY_TYPE);
    File demographicDir = new File(geoSourceDir, DEMOGRAPHIC_TYPE);
    File crosswalkDir = new File(geoSourceDir, CROSSWALK_TYPE);

    for (File dir : new File[]{geoSourceDir, boundaryDir, demographicDir, crosswalkDir}) {
      if (!dir.exists() && !dir.mkdirs()) {
        LOGGER.warn("Failed to create directory: {}", dir);
      }
    }

    // Log the partitioned structure
    LOGGER.info("Geographic data partitions created:");
    LOGGER.info("  Boundaries: {}", boundaryDir);
    LOGGER.info("  Demographics: {}", demographicDir);
    LOGGER.info("  Crosswalks: {}", crosswalkDir);

    // Log configuration
    LOGGER.info("Geographic data configuration:");
    LOGGER.info("  Cache directory: {}", cacheDir);
    LOGGER.info("  Enabled sources: {}", String.join(", ", enabledSources));
    LOGGER.info("  Data year: {}", dataYear);
    LOGGER.info("  Auto-download: {}", autoDownload);
    LOGGER.info("  Census API key: {}", censusApiKey != null ? "configured" : "not configured");
    LOGGER.info("  HUD credentials: {}", hudUsername != null ? "configured" : "not configured");
    LOGGER.info("  HUD token: {}", hudToken != null ? "configured" : "not configured");

    // Add automatic constraint definitions if enabled
    Boolean enableConstraints = (Boolean) operand.get("enableConstraints");
    if (enableConstraints == null) {
      enableConstraints = true; // Default to true
    }

    // Build constraint metadata for geographic tables
    Map<String, Map<String, Object>> geoConstraints = new HashMap<>();

    if (enableConstraints) {
      // Define constraints for each geographic table
      geoConstraints.putAll(defineGeoTableConstraints());
    }

    // Merge with any constraints from model file
    if (tableConstraints != null) {
      geoConstraints.putAll(tableConstraints);
    }

    // Create and return the geographic schema with constraints
    GeoSchema schema = new GeoSchema(
        parentSchema,
        name,
        cacheDir,
        censusApiKey,
        hudUsername,
        hudPassword,
        hudToken,
        enabledSources,
        dataYear,
        autoDownload
    );

    // Pass constraint metadata to the schema
    schema.setConstraintMetadata(geoConstraints);

    return schema;
  }

  /**
   * Define automatic constraint metadata for geographic tables.
   */
  private Map<String, Map<String, Object>> defineGeoTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    // tiger_states table constraints
    Map<String, Object> statesConstraints = new HashMap<>();
    statesConstraints.put("primaryKey", Arrays.asList("state_fips"));
    constraints.put("tiger_states", statesConstraints);

    // tiger_counties table constraints
    Map<String, Object> countiesConstraints = new HashMap<>();
    countiesConstraints.put("primaryKey", Arrays.asList("county_fips"));
    // Foreign key to states
    Map<String, Object> countyToStateFk = new HashMap<>();
    countyToStateFk.put("columns", Arrays.asList("state_fips"));
    countyToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    countyToStateFk.put("targetColumns", Arrays.asList("state_fips"));
    countiesConstraints.put("foreignKeys", Arrays.asList(countyToStateFk));
    constraints.put("tiger_counties", countiesConstraints);

    // census_places table constraints
    Map<String, Object> placesConstraints = new HashMap<>();
    placesConstraints.put("primaryKey", Arrays.asList("place_code", "state_code"));
    // Foreign key to states
    Map<String, Object> placeToStateFk = new HashMap<>();
    placeToStateFk.put("columns", Arrays.asList("state_code"));
    placeToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    placeToStateFk.put("targetColumns", Arrays.asList("state_code"));
    placesConstraints.put("foreignKeys", Arrays.asList(placeToStateFk));
    constraints.put("census_places", placesConstraints);

    // hud_zip_county table constraints
    Map<String, Object> zipCountyConstraints = new HashMap<>();
    zipCountyConstraints.put("primaryKey", Arrays.asList("zip"));
    // Foreign key to counties
    Map<String, Object> zipToCountyFk = new HashMap<>();
    zipToCountyFk.put("columns", Arrays.asList("county_fips"));
    zipToCountyFk.put("targetTable", Arrays.asList("tiger_counties"));
    zipToCountyFk.put("targetColumns", Arrays.asList("county_fips"));
    zipCountyConstraints.put("foreignKeys", Arrays.asList(zipToCountyFk));
    constraints.put("hud_zip_county", zipCountyConstraints);

    // hud_zip_tract table constraints
    Map<String, Object> zipTractConstraints = new HashMap<>();
    zipTractConstraints.put("primaryKey", Arrays.asList("zip", "tract"));
    // Foreign key to counties
    Map<String, Object> tractToCountyFk = new HashMap<>();
    tractToCountyFk.put("columns", Arrays.asList("county_fips"));
    tractToCountyFk.put("targetTable", Arrays.asList("tiger_counties"));
    tractToCountyFk.put("targetColumns", Arrays.asList("county_fips"));
    zipTractConstraints.put("foreignKeys", Arrays.asList(tractToCountyFk));
    constraints.put("hud_zip_tract", zipTractConstraints);

    // hud_zip_cbsa table constraints
    Map<String, Object> zipCbsaConstraints = new HashMap<>();
    zipCbsaConstraints.put("primaryKey", Arrays.asList("zip", "cbsa_code"));
    constraints.put("hud_zip_cbsa", zipCbsaConstraints);

    // tiger_zctas table constraints
    Map<String, Object> zctasConstraints = new HashMap<>();
    zctasConstraints.put("primaryKey", Arrays.asList("zcta5"));
    constraints.put("tiger_zctas", zctasConstraints);

    // tiger_census_tracts table constraints
    Map<String, Object> tractsConstraints = new HashMap<>();
    tractsConstraints.put("primaryKey", Arrays.asList("tract_geoid"));
    // Foreign key to counties
    Map<String, Object> tractToCountyFk2 = new HashMap<>();
    tractToCountyFk2.put("columns", Arrays.asList("county_fips"));
    tractToCountyFk2.put("targetTable", Arrays.asList("tiger_counties"));
    tractToCountyFk2.put("targetColumns", Arrays.asList("county_fips"));
    tractsConstraints.put("foreignKeys", Arrays.asList(tractToCountyFk2));
    constraints.put("tiger_census_tracts", tractsConstraints);

    // tiger_block_groups table constraints
    Map<String, Object> blockGroupsConstraints = new HashMap<>();
    blockGroupsConstraints.put("primaryKey", Arrays.asList("bg_geoid"));
    // Foreign key to census tracts via tract portion of geoid
    Map<String, Object> bgToTractFk = new HashMap<>();
    bgToTractFk.put("columns", Arrays.asList("tract_code"));
    bgToTractFk.put("targetTable", Arrays.asList("tiger_census_tracts"));
    bgToTractFk.put("targetColumns", Arrays.asList("tract_code"));
    blockGroupsConstraints.put("foreignKeys", Arrays.asList(bgToTractFk));
    constraints.put("tiger_block_groups", blockGroupsConstraints);

    // tiger_cbsa table constraints
    Map<String, Object> cbsaConstraints = new HashMap<>();
    cbsaConstraints.put("primaryKey", Arrays.asList("cbsa_code"));
    constraints.put("tiger_cbsa", cbsaConstraints);

    // hud_zip_cbsa_div table constraints
    Map<String, Object> zipCbsaDivConstraints = new HashMap<>();
    zipCbsaDivConstraints.put("primaryKey", Arrays.asList("zip", "cbsadiv"));
    // Foreign key to parent CBSA
    Map<String, Object> cbsaDivToCbsaFk = new HashMap<>();
    cbsaDivToCbsaFk.put("columns", Arrays.asList("cbsa"));
    cbsaDivToCbsaFk.put("targetTable", Arrays.asList("tiger_cbsa"));
    cbsaDivToCbsaFk.put("targetColumns", Arrays.asList("cbsa_code"));
    zipCbsaDivConstraints.put("foreignKeys", Arrays.asList(cbsaDivToCbsaFk));
    constraints.put("hud_zip_cbsa_div", zipCbsaDivConstraints);

    // hud_zip_congressional table constraints
    Map<String, Object> zipCongressionalConstraints = new HashMap<>();
    zipCongressionalConstraints.put("primaryKey", Arrays.asList("zip", "cd"));
    // Foreign key to states
    Map<String, Object> congressionalToStateFk = new HashMap<>();
    congressionalToStateFk.put("columns", Arrays.asList("state_code"));
    congressionalToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    congressionalToStateFk.put("targetColumns", Arrays.asList("state_code"));
    zipCongressionalConstraints.put("foreignKeys", Arrays.asList(congressionalToStateFk));
    constraints.put("hud_zip_congressional", zipCongressionalConstraints);

    return constraints;
  }

  @Override
  public boolean supportsConstraints() {
    // Enable constraint support for geographic data
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
}
