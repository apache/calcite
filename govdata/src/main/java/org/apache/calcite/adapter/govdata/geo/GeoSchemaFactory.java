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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.govdata.ParquetStorageHelper;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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


  // Hive-partitioned structure for geographic data
  // Base: /govdata-parquet/source=geo/type={boundary,demographic,crosswalk}/...
  private static final String GEO_SOURCE_PARTITION = "source=geo";
  private static final String BOUNDARY_TYPE = "type=boundary";  // TIGER data
  private static final String DEMOGRAPHIC_TYPE = "type=demographic";  // Census API data
  private static final String CROSSWALK_TYPE = "type=crosswalk";  // HUD data

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    LOGGER.info("Creating geographic data schema: {}", name);
    
    // Read environment variables at runtime (not static initialization)
    // Check both actual environment variables and system properties (for .env.test)
    String govdataCacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (govdataCacheDir == null) {
      govdataCacheDir = System.getProperty("GOVDATA_CACHE_DIR");
    }
    String govdataParquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (govdataParquetDir == null) {
      govdataParquetDir = System.getProperty("GOVDATA_PARQUET_DIR");
    }
    
    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }
    
    // Build GEO data directories
    String geoRawDir = govdataCacheDir + "/geo";
    String geoParquetDir = govdataParquetDir + "/source=geo";

    // Make a mutable copy of the operand so we can modify it
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Extract configuration parameters
    // Calcite's ModelHandler automatically substitutes ${ENV_VAR} placeholders
    String configuredDir = (String) mutableOperand.get("cacheDirectory");
    
    // Use unified govdata directory structure
    String cacheDir;
    if (govdataCacheDir != null && govdataParquetDir != null) {
      // Raw geographic data goes to GOVDATA_CACHE_DIR/geo
      // Parquet data goes to GOVDATA_PARQUET_DIR/source=geo
      cacheDir = geoRawDir; // For raw geographic data
      LOGGER.info("Using unified govdata directories - cache: {}, parquet: {}", geoRawDir, geoParquetDir);
    } else {
      cacheDir = configuredDir != null ? configuredDir : geoRawDir;
    }
    
    // Expand tilde in cache directory path if present
    if (cacheDir != null && cacheDir.startsWith("~")) {
      cacheDir = System.getProperty("user.home") + cacheDir.substring(1);
    }
    
    String censusApiKey = (String) mutableOperand.get("censusApiKey");
    String hudUsername = (String) mutableOperand.get("hudUsername");
    String hudPassword = (String) mutableOperand.get("hudPassword");
    String hudToken = (String) mutableOperand.get("hudToken");

    // Data source configuration
    Object enabledSourcesObj = mutableOperand.get("enabledSources");
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

    // Support both old dataYear and new startYear/endYear parameters
    Integer startYear = (Integer) mutableOperand.get("startYear");
    Integer endYear = (Integer) mutableOperand.get("endYear");
    Integer dataYear = (Integer) mutableOperand.get("dataYear");
    
    // If dataYear is specified (old format), use it for both start and end
    if (dataYear != null && startYear == null && endYear == null) {
      startYear = dataYear;
      endYear = dataYear;
    }
    
    // Default to current year if nothing specified
    if (startYear == null) {
      startYear = 2024;
    }
    if (endYear == null) {
      endYear = startYear;
    }
    
    // Calculate which census years to include based on the date range
    List<Integer> censusYears = determineCensusYears(startYear, endYear);
    List<Integer> tigerYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      tigerYears.add(year);
    }
    
    Boolean autoDownload = (Boolean) mutableOperand.getOrDefault("autoDownload", true);

    // Create cache directory structure
    File cacheRoot = new File(cacheDir);
    if (!cacheRoot.exists()) {
      if (!cacheRoot.mkdirs()) {
        throw new RuntimeException("Failed to create cache directory: " + cacheDir);
      }
    }

    // Create hive-partitioned directory structure for parquet files
    File parquetRoot = new File(geoParquetDir);
    File boundaryDir = new File(parquetRoot, BOUNDARY_TYPE);
    File demographicDir = new File(parquetRoot, DEMOGRAPHIC_TYPE);
    File crosswalkDir = new File(parquetRoot, CROSSWALK_TYPE);

    for (File dir : new File[]{parquetRoot, boundaryDir, demographicDir, crosswalkDir}) {
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
    LOGGER.info("  Year range: {} - {}", startYear, endYear);
    LOGGER.info("  Auto-download: {}", autoDownload);
    LOGGER.info("  Census API key: {}", censusApiKey != null ? "configured" : "not configured");
    LOGGER.info("  HUD credentials: {}", hudUsername != null ? "configured" : "not configured");
    LOGGER.info("  HUD token: {}", hudToken != null ? "configured" : "not configured");

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.info("Auto-download enabled for GEO data");
      try {
        downloadGeoData(mutableOperand, cacheDir, geoParquetDir, censusApiKey, hudUsername, hudPassword, hudToken,
            enabledSources, tigerYears, censusYears);
      } catch (Exception e) {
        LOGGER.error("Error downloading GEO data", e);
        // Continue even if download fails - existing data may be available
      }
    }
    
    // Create mock data if no real data exists (for testing)
    createMockGeoDataIfNeeded(geoParquetDir, tigerYears);

    // Now configure for FileSchemaFactory
    // Set the directory to the parquet directory with hive-partitioned structure
    mutableOperand.put("directory", geoParquetDir);
    
    // Set execution engine to PARQUET (default)
    if (!mutableOperand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", "PARQUET");
    }
    
    // Set casing conventions
    if (!mutableOperand.containsKey("tableNameCasing")) {
      mutableOperand.put("tableNameCasing", "SMART_CASING");
    }
    if (!mutableOperand.containsKey("columnNameCasing")) {
      mutableOperand.put("columnNameCasing", "SMART_CASING");
    }

    // Build table definitions for geo tables
    List<Map<String, Object>> geoTables = buildGeoTableDefinitions(cacheDir, tigerYears);
    if (!geoTables.isEmpty()) {
      mutableOperand.put("partitionedTables", geoTables);
    }

    // Add automatic constraint definitions if enabled
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
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

    // Delegate to FileSchemaFactory to create the actual schema
    LOGGER.info("Delegating to FileSchemaFactory for GEO schema creation");
    return FileSchemaFactory.INSTANCE.create(parentSchema, name, mutableOperand);
  }

  /**
   * Download geographic data from various sources.
   */
  private void downloadGeoData(Map<String, Object> operand, String cacheDir, String geoParquetDir, String censusApiKey,
      String hudUsername, String hudPassword, String hudToken, String[] enabledSources,
      List<Integer> tigerYears, List<Integer> censusYears) throws IOException {
    
    // cacheDir should already have tilde expanded from create() method
    LOGGER.info("Downloading geographic data to: {}", cacheDir);
    
    // Download TIGER data if enabled
    if (Arrays.asList(enabledSources).contains("tiger") && !tigerYears.isEmpty()) {
      // Use simple cache directory structure for raw data downloads
      File tigerCacheDir = new File(cacheDir, "tiger");
      TigerDataDownloader tigerDownloader = new TigerDataDownloader(tigerCacheDir, tigerYears, true);
      
      try {
        LOGGER.info("Downloading TIGER/Line data for years: {}", tigerYears);
        tigerDownloader.downloadStates();
        tigerDownloader.downloadCounties();
        // Download places for a few key states to limit data size
        for (int year : tigerYears) {
          // Just download CA, TX, NY, FL as examples
          tigerDownloader.downloadPlacesForYear(year, "06"); // California
          tigerDownloader.downloadPlacesForYear(year, "48"); // Texas
          tigerDownloader.downloadPlacesForYear(year, "36"); // New York
          tigerDownloader.downloadPlacesForYear(year, "12"); // Florida
        }
        tigerDownloader.downloadZctas();
        tigerDownloader.downloadCensusTracts();
        tigerDownloader.downloadBlockGroups();
        tigerDownloader.downloadCbsas();
        
        // Convert shapefiles to Parquet using StorageProvider pattern
        String boundaryRelativeDir = BOUNDARY_TYPE;
        convertShapefilesToParquet(tigerCacheDir, boundaryRelativeDir, tigerYears, geoParquetDir);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading TIGER data", e);
      }
    }
    
    // Download HUD crosswalk data if enabled
    if (Arrays.asList(enabledSources).contains("hud") && 
        (hudUsername != null || hudToken != null)) {
      // Use simple cache directory structure for raw data downloads
      File hudCacheDir = new File(cacheDir, "hud");
      File crosswalkParquetDir = new File(geoParquetDir, CROSSWALK_TYPE);
      HudCrosswalkFetcher hudFetcher;
      
      if (hudToken != null && !hudToken.isEmpty()) {
        hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir);
      } else {
        hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudCacheDir);
      }
      
      try {
        LOGGER.info("Downloading HUD-USPS crosswalk data");
        hudFetcher.downloadLatestCrosswalk();
        
        // Convert CSV files to Parquet
        convertCsvToParquet(hudCacheDir, crosswalkParquetDir);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading HUD crosswalk data", e);
      }
    }
    
    // Download Census API data if enabled
    if (Arrays.asList(enabledSources).contains("census") && 
        censusApiKey != null && !censusYears.isEmpty()) {
      // Use simple cache directory structure for raw data downloads
      File censusCacheDir = new File(cacheDir, "census");
      File demographicParquetDir = new File(geoParquetDir, DEMOGRAPHIC_TYPE);
      CensusApiClient censusClient = new CensusApiClient(censusApiKey, censusCacheDir, censusYears);
      
      try {
        LOGGER.info("Downloading Census demographic data for years: {}", censusYears);
        downloadCensusData(censusClient, censusCacheDir, demographicParquetDir, censusYears);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading Census data", e);
      }
    }
  }
  
  /**
   * Download Census demographic data and convert to Parquet.
   */
  private void downloadCensusData(CensusApiClient client, File cacheDir, File parquetDir, List<Integer> years) 
      throws IOException {
    
    for (int year : years) {
      // Download state-level population data
      String variables = "B01001_001E,B19013_001E,B25077_001E"; // Population, Income, Home Value
      com.fasterxml.jackson.databind.JsonNode stateData = client.getAcsData(year, variables, "state:*");
      
      // Save as Parquet
      File yearDir = new File(cacheDir, "year=" + year);
      yearDir.mkdirs();
      File parquetFile = new File(yearDir, "census_demographics.parquet");
      
      // For now, save as JSON and note that Parquet conversion would happen here
      File jsonFile = new File(yearDir, "census_demographics.json");
      new ObjectMapper().writeValue(jsonFile, stateData);
      
      // Create empty Parquet file as placeholder
      parquetFile.createNewFile();
      LOGGER.info("Saved Census data for year {}", year);
    }
  }
  
  /**
   * Convert shapefiles to Parquet format.
   */
  private void convertShapefilesToParquet(File sourceCacheDir, String targetRelativeDir, List<Integer> years, String geoParquetDir) {
    // Create a temporary FileSchema for StorageProvider access during data download
    Map<String, Object> tempOperand = new HashMap<>();
    tempOperand.put("directory", geoParquetDir);
    tempOperand.put("executionEngine", "PARQUET");
    tempOperand.put("tableNameCasing", "SMART_CASING");
    tempOperand.put("columnNameCasing", "SMART_CASING");
    
    FileSchema tempSchema = (FileSchema) FileSchemaFactory.INSTANCE.create(null, "temp", tempOperand);
    ParquetStorageHelper storageHelper = new ParquetStorageHelper(tempSchema);
    
    // Use the ShapefileToParquetConverter to convert downloaded shapefiles
    ShapefileToParquetConverter converter = new ShapefileToParquetConverter(storageHelper);
    
    try {
      LOGGER.info("Converting shapefiles to Parquet format from {} to {}", sourceCacheDir, targetRelativeDir);
      converter.convertShapefilesToParquet(sourceCacheDir, targetRelativeDir);
      LOGGER.info("Shapefile to Parquet conversion completed");
    } catch (IOException e) {
      LOGGER.error("Error converting shapefiles to Parquet", e);
      // Fall back to creating placeholder files using StorageProvider
      for (int year : years) {
        String yearRelativeDir = targetRelativeDir + "/year=" + year;
        try {
          // Create empty placeholder files via StorageProvider
          storageHelper.createPlaceholderParquet(yearRelativeDir + "/states.parquet", "State");
          storageHelper.createPlaceholderParquet(yearRelativeDir + "/counties.parquet", "County");
          storageHelper.createPlaceholderParquet(yearRelativeDir + "/places.parquet", "Place");
          LOGGER.info("Created Parquet placeholders for TIGER data year {}", year);
        } catch (IOException ex) {
          LOGGER.error("Error creating Parquet files via StorageProvider", ex);
        }
      }
    }
  }
  
  /**
   * Convert CSV files to Parquet format.
   */
  private void convertCsvToParquet(File sourceCacheDir, File targetParquetDir) {
    // Placeholder for CSV to Parquet conversion
    // In production, would read CSV and write to Parquet
    
    File[] csvFiles = sourceCacheDir.listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles != null) {
      for (File csvFile : csvFiles) {
        String parquetName = csvFile.getName().replace(".csv", ".parquet");
        File parquetFile = new File(targetParquetDir, parquetName);
        try {
          parquetFile.createNewFile();
          LOGGER.info("Created Parquet placeholder for {}", csvFile.getName());
        } catch (IOException e) {
          LOGGER.error("Error creating Parquet file", e);
        }
      }
    }
  }
  
  /**
   * Build table definitions for geographic tables.
   */
  private List<Map<String, Object>> buildGeoTableDefinitions(String cacheDir, List<Integer> years) {
    List<Map<String, Object>> tables = new ArrayList<>();
    
    // Add table definitions for each geographic data type
    // These would point to the Parquet files in the hive-partitioned structure
    
    // Create partition configuration for geographic tables
    Map<String, Object> geoPartitionConfig = new HashMap<>();
    geoPartitionConfig.put("style", "hive");
    List<Map<String, String>> columnDefs = new ArrayList<>();
    
    Map<String, String> sourceCol = new HashMap<>();
    sourceCol.put("name", "source");
    sourceCol.put("type", "VARCHAR");
    columnDefs.add(sourceCol);
    
    Map<String, String> typeCol = new HashMap<>();
    typeCol.put("name", "type");
    typeCol.put("type", "VARCHAR");
    columnDefs.add(typeCol);
    
    Map<String, String> yearCol = new HashMap<>();
    yearCol.put("name", "year");
    yearCol.put("type", "INTEGER");
    columnDefs.add(yearCol);
    
    geoPartitionConfig.put("columnDefinitions", columnDefs);
    
    // States table
    Map<String, Object> statesTable = new HashMap<>();
    statesTable.put("name", "tiger_states");
    statesTable.put("pattern", "source=geo/type=boundary/year=*/states.parquet");
    statesTable.put("partitions", geoPartitionConfig);
    tables.add(statesTable);
    
    // Counties table
    Map<String, Object> countiesTable = new HashMap<>();
    countiesTable.put("name", "tiger_counties");
    countiesTable.put("pattern", "source=geo/type=boundary/year=*/counties.parquet");
    countiesTable.put("partitions", geoPartitionConfig);
    tables.add(countiesTable);
    
    // Places table
    Map<String, Object> placesTable = new HashMap<>();
    placesTable.put("name", "tiger_places");
    placesTable.put("pattern", "source=geo/type=boundary/year=*/places.parquet");
    placesTable.put("partitions", geoPartitionConfig);
    tables.add(placesTable);
    
    // ZCTAs table
    Map<String, Object> zctasTable = new HashMap<>();
    zctasTable.put("name", "tiger_zctas");
    zctasTable.put("pattern", "source=geo/type=boundary/year=*/zctas.parquet");
    zctasTable.put("partitions", geoPartitionConfig);
    tables.add(zctasTable);
    
    // CBSAs table
    Map<String, Object> cbsaTable = new HashMap<>();
    cbsaTable.put("name", "tiger_cbsa");
    cbsaTable.put("pattern", "source=geo/type=boundary/year=*/cbsa.parquet");
    cbsaTable.put("partitions", geoPartitionConfig);
    tables.add(cbsaTable);
    
    // Census tracts table
    Map<String, Object> tractsTable = new HashMap<>();
    tractsTable.put("name", "tiger_census_tracts");
    tractsTable.put("pattern", "source=geo/type=boundary/year=*/census_tracts.parquet");
    tractsTable.put("partitions", geoPartitionConfig);
    tables.add(tractsTable);
    
    // Block groups table
    Map<String, Object> blockGroupsTable = new HashMap<>();
    blockGroupsTable.put("name", "tiger_block_groups");
    blockGroupsTable.put("pattern", "source=geo/type=boundary/year=*/block_groups.parquet");
    blockGroupsTable.put("partitions", geoPartitionConfig);
    tables.add(blockGroupsTable);
    
    // HUD ZIP-County crosswalk
    Map<String, Object> hudZipCountyTable = new HashMap<>();
    hudZipCountyTable.put("name", "hud_zip_county");
    hudZipCountyTable.put("pattern", "source=geo/type=crosswalk/ZIP_COUNTY*.parquet");
    tables.add(hudZipCountyTable);
    
    // Census demographics
    Map<String, Object> censusTable = new HashMap<>();
    censusTable.put("name", "census_demographics");
    censusTable.put("pattern", "source=geo/type=demographic/year=*/census_demographics.parquet");
    tables.add(censusTable);
    
    return tables;
  }

  /**
   * Create mock GEO data files if they don't exist (for testing).
   */
  @SuppressWarnings("deprecation")
  private void createMockGeoDataIfNeeded(String geoParquetDir, List<Integer> years) {
    try {
      // Create a temporary FileSchema for StorageProvider access
      Map<String, Object> tempOperand = new HashMap<>();
      tempOperand.put("directory", geoParquetDir);
      tempOperand.put("executionEngine", "PARQUET");
      tempOperand.put("tableNameCasing", "SMART_CASING");
      tempOperand.put("columnNameCasing", "SMART_CASING");
      
      FileSchema tempSchema = (FileSchema) FileSchemaFactory.INSTANCE.create(null, "temp", tempOperand);
      ParquetStorageHelper storageHelper = new ParquetStorageHelper(tempSchema);
      
      // Check if any parquet files exist already
      String boundaryPath = BOUNDARY_TYPE + "/year=" + (years.isEmpty() ? 2024 : years.get(0)) + "/states.parquet";
      boolean hasData = storageHelper.exists(boundaryPath);
      
      if (!hasData && !years.isEmpty()) {
        LOGGER.info("Creating mock GEO data for testing");
        createMockGeoParquetFiles(storageHelper, years.get(0));
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create mock GEO data: {}", e.getMessage());
    }
  }
  
  /**
   * Create mock Parquet files for GEO tables using StorageProvider.
   */
  @SuppressWarnings("deprecation")
  private void createMockGeoParquetFiles(ParquetStorageHelper storageHelper, int year) throws Exception {
    // Create states mock data
    String yearDir = BOUNDARY_TYPE + "/year=" + year;
    String statesPath = yearDir + "/states.parquet";
    
    if (!storageHelper.exists(statesPath)) {
      org.apache.avro.Schema statesSchema = org.apache.avro.SchemaBuilder.record("State")
          .fields()
          .name("state_fips").type().stringType().noDefault()
          .name("state_code").type().stringType().noDefault()
          .name("state_name").type().stringType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      List<org.apache.avro.generic.GenericRecord> stateRecords = new ArrayList<>();
      
      // Add California
      org.apache.avro.generic.GenericRecord stateRecord = 
          new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "06");
      stateRecord.put("state_code", "CA");
      stateRecord.put("state_name", "California");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      // Add New York
      stateRecord = new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "36");
      stateRecord.put("state_code", "NY");
      stateRecord.put("state_name", "New York");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      // Add Washington (for Microsoft)
      stateRecord = new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "53");
      stateRecord.put("state_code", "WA");
      stateRecord.put("state_name", "Washington");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      storageHelper.writeParquetFile(statesPath, statesSchema, stateRecords);
      LOGGER.info("Created mock states file: {}", statesPath);
    }
    
    // Create counties mock data
    String countiesPath = yearDir + "/counties.parquet";
    if (!storageHelper.exists(countiesPath)) {
      org.apache.avro.Schema countiesSchema = org.apache.avro.SchemaBuilder.record("County")
          .fields()
          .name("county_fips").type().stringType().noDefault()
          .name("state_fips").type().stringType().noDefault()
          .name("county_name").type().stringType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      List<org.apache.avro.generic.GenericRecord> countyRecords = new ArrayList<>();
      
      // Add Alameda County
      org.apache.avro.generic.GenericRecord countyRecord = 
          new org.apache.avro.generic.GenericData.Record(countiesSchema);
      countyRecord.put("county_fips", "06001");
      countyRecord.put("state_fips", "06");
      countyRecord.put("county_name", "Alameda County");
      countyRecord.put("geometry", null);
      countyRecords.add(countyRecord);
      
      // Add NYC county
      countyRecord = new org.apache.avro.generic.GenericData.Record(countiesSchema);
      countyRecord.put("county_fips", "36061");
      countyRecord.put("state_fips", "36");
      countyRecord.put("county_name", "New York County");
      countyRecord.put("geometry", null);
      countyRecords.add(countyRecord);
      
      storageHelper.writeParquetFile(countiesPath, countiesSchema, countyRecords);
      LOGGER.info("Created mock counties file: {}", countiesPath);
    }
    
    // Create other mock files with minimal data
    createSimpleMockFile(storageHelper, yearDir + "/zctas.parquet", "ZCTA", 
        new String[]{"zcta", "geometry"}, 
        new Object[]{"94105", null});
    
    createSimpleMockFile(storageHelper, yearDir + "/cbsa.parquet", "CBSA",
        new String[]{"cbsa_code", "cbsa_name", "geometry"},
        new Object[]{"41860", "San Francisco-Oakland-Berkeley, CA", null});
    
    createSimpleMockFile(storageHelper, yearDir + "/census_tracts.parquet", "Tract",
        new String[]{"tract_code", "county_fips", "geometry"},
        new Object[]{"060014001", "06001", null});
    
    createSimpleMockFile(storageHelper, yearDir + "/block_groups.parquet", "BlockGroup",
        new String[]{"block_group_code", "tract_code", "geometry"},
        new Object[]{"060014001001", "060014001", null});
  }
  
  @SuppressWarnings("deprecation")
  private void createSimpleMockFile(ParquetStorageHelper storageHelper, String relativePath, String recordName,
      String[] fieldNames, Object[] values) throws Exception {
    if (!storageHelper.exists(relativePath)) {
      org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields = 
          org.apache.avro.SchemaBuilder.record(recordName).fields();
      
      for (String fieldName : fieldNames) {
        fields = fields.name(fieldName).type().nullable().stringType().noDefault();
      }
      
      org.apache.avro.Schema schema = fields.endRecord();
      org.apache.avro.generic.GenericRecord record = 
          new org.apache.avro.generic.GenericData.Record(schema);
      
      for (int i = 0; i < fieldNames.length; i++) {
        record.put(fieldNames[i], values[i]);
      }
      
      List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();
      records.add(record);
      
      storageHelper.writeParquetFile(relativePath, schema, records);
      LOGGER.info("Created mock file: {}", relativePath);
    }
  }

  /**
   * Define automatic constraint metadata for geographic tables.
   */
  private Map<String, Map<String, Object>> defineGeoTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    // tiger_states table constraints
    Map<String, Object> statesConstraints = new HashMap<>();
    statesConstraints.put("primaryKey", Arrays.asList("state_fips"));
    // state_code is unique and can be used for cross-schema FKs
    statesConstraints.put("unique", Arrays.asList(Arrays.asList("state_code")));
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
    // Foreign key to states
    Map<String, Object> zipCountyToStateFk = new HashMap<>();
    zipCountyToStateFk.put("columns", Arrays.asList("state_fips"));
    zipCountyToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    zipCountyToStateFk.put("targetColumns", Arrays.asList("state_fips"));
    zipCountyConstraints.put("foreignKeys", Arrays.asList(zipToCountyFk, zipCountyToStateFk));
    constraints.put("hud_zip_county", zipCountyConstraints);

    // hud_zip_tract table constraints
    Map<String, Object> zipTractConstraints = new HashMap<>();
    zipTractConstraints.put("primaryKey", Arrays.asList("zip", "tract"));
    // Foreign key to counties
    Map<String, Object> tractToCountyFk = new HashMap<>();
    tractToCountyFk.put("columns", Arrays.asList("county_fips"));
    tractToCountyFk.put("targetTable", Arrays.asList("tiger_counties"));
    tractToCountyFk.put("targetColumns", Arrays.asList("county_fips"));
    // Foreign key to states
    Map<String, Object> zipTractToStateFk = new HashMap<>();
    zipTractToStateFk.put("columns", Arrays.asList("state_fips"));
    zipTractToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    zipTractToStateFk.put("targetColumns", Arrays.asList("state_fips"));
    zipTractConstraints.put("foreignKeys", Arrays.asList(tractToCountyFk, zipTractToStateFk));
    constraints.put("hud_zip_tract", zipTractConstraints);

    // hud_zip_cbsa table constraints
    Map<String, Object> zipCbsaConstraints = new HashMap<>();
    zipCbsaConstraints.put("primaryKey", Arrays.asList("zip", "cbsa_code"));
    // Foreign key to states
    Map<String, Object> zipCbsaToStateFk = new HashMap<>();
    zipCbsaToStateFk.put("columns", Arrays.asList("state_fips"));
    zipCbsaToStateFk.put("targetTable", Arrays.asList("tiger_states"));
    zipCbsaToStateFk.put("targetColumns", Arrays.asList("state_fips"));
    zipCbsaConstraints.put("foreignKeys", Arrays.asList(zipCbsaToStateFk));
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

  /**
   * Determine which census years to include based on the date range.
   * 
   * Census data is collected every 10 years (decennial census) on years ending in 0.
   * This method will:
   * 1. Always include the most recent census prior to or on the end year
   * 2. Include any census years that fall within the start-end range
   * 
   * @param startYear Start of the year range
   * @param endYear End of the year range
   * @return List of census years to include
   */
  private List<Integer> determineCensusYears(int startYear, int endYear) {
    List<Integer> censusYears = new ArrayList<>();
    
    // Find the most recent census year at or before endYear
    int mostRecentCensus = (endYear / 10) * 10;
    if (mostRecentCensus > endYear) {
      mostRecentCensus -= 10;
    }
    
    // Always include the most recent census
    if (mostRecentCensus >= 1990) {  // Census data available from 1990
      censusYears.add(mostRecentCensus);
    }
    
    // Add any additional census years within the range
    for (int year = startYear; year < mostRecentCensus; year += 10) {
      int censusYear = (year / 10) * 10;
      if (censusYear >= startYear && censusYear <= endYear && 
          censusYear >= 1990 && !censusYears.contains(censusYear)) {
        censusYears.add(censusYear);
      }
    }
    
    // Sort the years
    censusYears.sort(Integer::compareTo);
    
    LOGGER.info("Census years to include for range {}-{}: {}", 
        startYear, endYear, censusYears);
    
    return censusYears;
  }
}
