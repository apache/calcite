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
import org.apache.calcite.adapter.govdata.TableCommentDefinitions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema for U.S. government geographic data.
 *
 * <p>Provides tables for:
 * <ul>
 *   <li>States, counties, places (cities/towns)</li>
 *   <li>ZIP Code Tabulation Areas (ZCTAs)</li>
 *   <li>Congressional districts</li>
 *   <li>School districts</li>
 *   <li>Census demographics</li>
 *   <li>Economic indicators</li>
 *   <li>ZIP to Census geography crosswalk</li>
 * </ul>
 */
public class GeoSchema extends AbstractSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoSchema.class);

  private final SchemaPlus parentSchema;
  private final String name;
  private final String cacheDir;
  private final String censusApiKey;
  private final String hudUsername;
  private final String hudPassword;
  private final String hudToken;
  private final Set<String> enabledSources;
  private final List<Integer> tigerYears;
  private final List<Integer> censusYears;
  private final boolean autoDownload;
  
  // Data fetchers (to be implemented)
  private TigerDataDownloader tigerDownloader;
  private CensusApiClient censusClient;
  private HudCrosswalkFetcher hudFetcher;
  
  // Cached tables
  private final Map<String, Table> tableMap = new HashMap<>();
  
  // Constraint metadata from factory
  private Map<String, Map<String, Object>> constraintMetadata = new HashMap<>();

  public GeoSchema(SchemaPlus parentSchema, String name, String cacheDir,
      String censusApiKey, String hudUsername, String hudPassword,
      String[] enabledSources, List<Integer> tigerYears, List<Integer> censusYears, 
      boolean autoDownload) {
    this(parentSchema, name, cacheDir, censusApiKey, hudUsername, hudPassword, 
         null, enabledSources, tigerYears, censusYears, autoDownload);
  }
  
  public GeoSchema(SchemaPlus parentSchema, String name, String cacheDir,
      String censusApiKey, String hudUsername, String hudPassword, String hudToken,
      String[] enabledSources, List<Integer> tigerYears, List<Integer> censusYears, 
      boolean autoDownload) {
    
    this.parentSchema = parentSchema;
    this.name = name;
    this.cacheDir = cacheDir;
    this.censusApiKey = censusApiKey;
    this.hudUsername = hudUsername;
    this.hudPassword = hudPassword;
    this.hudToken = hudToken;
    this.enabledSources = new HashSet<>(Arrays.asList(enabledSources));
    this.tigerYears = tigerYears;
    this.censusYears = censusYears;
    this.autoDownload = autoDownload;
    
    // Initialize data fetchers
    initializeDataFetchers();
    
    // Initialize tables
    initializeTables();
  }

  private void initializeDataFetchers() {
    LOGGER.info("Initializing geographic data fetchers");
    LOGGER.info("  TIGER years: {}", tigerYears);
    LOGGER.info("  Census years: {}", censusYears);
    
    // Initialize TIGER downloader with hive-partitioned structure
    if (enabledSources.contains("tiger") && !tigerYears.isEmpty()) {
      // TIGER data goes under: /govdata-parquet/source=geo/type=boundary/year=YYYY/
      File boundaryDir = new File(new File(cacheDir, "source=geo"), "type=boundary");
      this.tigerDownloader = new TigerDataDownloader(boundaryDir, tigerYears, autoDownload);
    }
    
    // Initialize Census API client with hive-partitioned structure
    if (enabledSources.contains("census") && censusApiKey != null && !censusYears.isEmpty()) {
      // Census data goes under: /govdata-parquet/source=geo/type=demographic/year=YYYY/
      File demographicDir = new File(new File(cacheDir, "source=geo"), "type=demographic");
      this.censusClient = new CensusApiClient(censusApiKey, demographicDir, censusYears);
    }
    
    // Initialize HUD crosswalk fetcher with hive-partitioned structure
    if (enabledSources.contains("hud")) {
      // HUD data goes under: /govdata-parquet/source=geo/type=crosswalk/year=YYYY/
      File crosswalkDir = new File(new File(cacheDir, "source=geo"), "type=crosswalk");
      if (hudToken != null && !hudToken.isEmpty()) {
        // Use token if available
        this.hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, crosswalkDir);
      } else if (hudUsername != null && hudPassword != null) {
        // Fall back to username/password
        this.hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, crosswalkDir);
      }
    }
  }

  private void initializeTables() {
    LOGGER.info("Initializing geographic tables");
    
    // Create HUD crosswalk tables directly from API data
    if (enabledSources.contains("hud") && hudFetcher != null) {
      createHudTables();
    }
    
    // Create Census tables if enabled
    if (enabledSources.contains("census") && censusClient != null) {
      createCensusTables();
    }
    
    // Create TIGER boundary tables if enabled
    if (enabledSources.contains("tiger") && tigerDownloader != null) {
      createTigerTables();
    }
    
    // Log available tables
    if (!tableMap.isEmpty()) {
      LOGGER.info("Available geographic tables: {}", tableMap.keySet());
    } else {
      LOGGER.info("No geographic tables found. Data may need to be downloaded first.");
    }
    
    // If auto-download is enabled and no data exists, trigger initial download
    if (autoDownload && tableMap.isEmpty()) {
      LOGGER.info("Auto-download enabled. Initiating data download...");
      downloadInitialData();
    }
  }
  
  private void createHudTables() {
    LOGGER.info("Creating HUD crosswalk tables");
    
    // Create HUD ZIP-County table
    HudZipCountyTable hudZipCountyTable = new HudZipCountyTable(hudFetcher);
    tableMap.put("hud_zip_county", hudZipCountyTable);
    
    // Create HUD ZIP-Tract table  
    HudZipTractTable hudZipTractTable = new HudZipTractTable(hudFetcher);
    tableMap.put("hud_zip_tract", hudZipTractTable);
    
    // Create HUD ZIP-CBSA table
    HudZipCbsaTable hudZipCbsaTable = new HudZipCbsaTable(hudFetcher);
    tableMap.put("hud_zip_cbsa", hudZipCbsaTable);
    
    // Create HUD ZIP-CBSA Division table
    HudZipCbsaDivTable hudZipCbsaDivTable = new HudZipCbsaDivTable(hudFetcher);
    tableMap.put("hud_zip_cbsa_div", hudZipCbsaDivTable);
    
    // Create HUD ZIP-Congressional District table
    HudZipCongressionalTable hudZipCongressionalTable = new HudZipCongressionalTable(hudFetcher);
    tableMap.put("hud_zip_congressional", hudZipCongressionalTable);
  }
  
  private void createCensusTables() {
    LOGGER.info("Creating Census tables");
    
    // Create Census places (cities) table
    CensusPlacesTable censusPlacesTable = new CensusPlacesTable(censusClient);
    tableMap.put("census_places", censusPlacesTable);
  }
  
  private void createTigerTables() {
    LOGGER.info("Creating TIGER boundary tables");
    
    // Create states table
    TigerStatesTable statesTable = new TigerStatesTable(tigerDownloader);
    tableMap.put("tiger_states", statesTable);
    
    // Create counties table
    TigerCountiesTable countiesTable = new TigerCountiesTable(tigerDownloader);
    tableMap.put("tiger_counties", countiesTable);
    
    // Create ZIP Code Tabulation Areas table
    TigerZctasTable zctasTable = new TigerZctasTable(tigerDownloader);
    tableMap.put("tiger_zctas", zctasTable);
    
    // Create Census Tracts table
    TigerCensusTractsTable censusTractsTable = new TigerCensusTractsTable(tigerDownloader);
    tableMap.put("tiger_census_tracts", censusTractsTable);
    
    // Create Block Groups table
    TigerBlockGroupsTable blockGroupsTable = new TigerBlockGroupsTable(tigerDownloader);
    tableMap.put("tiger_block_groups", blockGroupsTable);
    
    // Create CBSA table
    TigerCbsaTable cbsaTable = new TigerCbsaTable(tigerDownloader);
    tableMap.put("tiger_cbsa", cbsaTable);
  }
  
  private void downloadInitialData() {
    LOGGER.info("Downloading initial geographic data");
    
    // Download TIGER data
    if (tigerDownloader != null) {
      try {
        LOGGER.info("Downloading TIGER/Line data for years: {}", tigerYears);
        tigerDownloader.downloadStates();
        tigerDownloader.downloadCounties();
        tigerDownloader.downloadPlaces();
        tigerDownloader.downloadZctas();
        tigerDownloader.downloadCensusTracts();
        tigerDownloader.downloadBlockGroups();
        tigerDownloader.downloadCbsas();
      } catch (Exception e) {
        LOGGER.error("Error downloading TIGER data", e);
      }
    }
    
    // Download HUD crosswalk
    if (hudFetcher != null) {
      try {
        LOGGER.info("Downloading HUD-USPS crosswalk data");
        hudFetcher.downloadLatestCrosswalk();
      } catch (Exception e) {
        LOGGER.error("Error downloading HUD crosswalk", e);
      }
    }
    
    // Note: Census API data is fetched on-demand, not pre-downloaded
  }

  @Override protected Map<String, Table> getTableMap() {
    // Wrap tables with comment support for known GEO tables
    Map<String, Table> commentableTableMap = new HashMap<>();
    for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
      String tableName = entry.getKey();
      Table originalTable = entry.getValue();
      
      // Check if we have comments for this table
      String tableComment = TableCommentDefinitions.getGeoTableComment(tableName);
      Map<String, String> columnComments = TableCommentDefinitions.getGeoColumnComments(tableName);
      
      if (tableComment != null || !columnComments.isEmpty()) {
        // Wrap with comment support
        commentableTableMap.put(tableName, new CommentableGeoTableWrapper(originalTable, tableComment, columnComments));
      } else {
        commentableTableMap.put(tableName, originalTable);
      }
    }
    return ImmutableMap.copyOf(commentableTableMap);
  }
  
  /**
   * Sets constraint metadata for tables in this schema.
   * Called by GeoSchemaFactory to pass constraint definitions.
   * 
   * @param constraintMetadata Map from table name to constraint definitions
   */
  public void setConstraintMetadata(Map<String, Map<String, Object>> constraintMetadata) {
    this.constraintMetadata = constraintMetadata;
    LOGGER.debug("Received constraint metadata for {} tables", constraintMetadata.size());
    
    // Update existing tables with constraint metadata
    for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
      String tableName = entry.getKey();
      Map<String, Object> constraints = constraintMetadata.get(tableName);
      if (constraints != null) {
        // Tables will use this metadata when getStatistic() is called
        LOGGER.debug("Applied constraints to table: {}", tableName);
      }
    }
  }
  
  /**
   * Gets constraint metadata for a specific table.
   * 
   * @param tableName The table name
   * @return Constraint metadata or null if none defined
   */
  public Map<String, Object> getTableConstraints(String tableName) {
    return constraintMetadata.get(tableName);
  }
  
  /**
   * Define the schema for the states table.
   */
  public static RelDataType getStatesRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("state_name", SqlTypeName.VARCHAR)
        .add("state_abbr", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .add("total_area", SqlTypeName.DOUBLE)
        .add("intpt_lat", SqlTypeName.DOUBLE)
        .add("intpt_lon", SqlTypeName.DOUBLE)
        .build();
  }
  
  /**
   * Define the schema for the counties table.
   */
  public static RelDataType getCountiesRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("county_fips", SqlTypeName.VARCHAR)
        .add("county_name", SqlTypeName.VARCHAR)
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("state_abbr", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .add("total_area", SqlTypeName.DOUBLE)
        .add("intpt_lat", SqlTypeName.DOUBLE)
        .add("intpt_lon", SqlTypeName.DOUBLE)
        .build();
  }
  
  /**
   * Define the schema for the places (cities) table.
   */
  public static RelDataType getPlacesRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("place_fips", SqlTypeName.VARCHAR)
        .add("place_name", SqlTypeName.VARCHAR)
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("state_abbr", SqlTypeName.VARCHAR)
        .add("place_type", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .add("total_area", SqlTypeName.DOUBLE)
        .add("intpt_lat", SqlTypeName.DOUBLE)
        .add("intpt_lon", SqlTypeName.DOUBLE)
        .add("population", SqlTypeName.INTEGER)
        .build();
  }
  
  /**
   * Define the schema for the ZCTAs (ZIP codes) table.
   */
  public static RelDataType getZctasRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("zcta5", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .add("total_area", SqlTypeName.DOUBLE)
        .add("intpt_lat", SqlTypeName.DOUBLE)
        .add("intpt_lon", SqlTypeName.DOUBLE)
        .add("population", SqlTypeName.INTEGER)
        .add("housing_units", SqlTypeName.INTEGER)
        .build();
  }
  
  /**
   * Define the schema for the ZIP to county crosswalk table.
   */
  public static RelDataType getZipToCountyRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("zip", SqlTypeName.VARCHAR)
        .add("county_fips", SqlTypeName.VARCHAR)
        .add("res_ratio", SqlTypeName.DOUBLE)
        .add("bus_ratio", SqlTypeName.DOUBLE)
        .add("oth_ratio", SqlTypeName.DOUBLE)
        .add("tot_ratio", SqlTypeName.DOUBLE)
        .build();
  }
  
  /**
   * Wrapper that adds comment support to existing GEO Table instances.
   */
  private static class CommentableGeoTableWrapper implements CommentableTable {
    private final Table delegate;
    private final @Nullable String tableComment;
    private final Map<String, String> columnComments;
    
    CommentableGeoTableWrapper(Table delegate, @Nullable String tableComment, 
        Map<String, String> columnComments) {
      this.delegate = delegate;
      this.tableComment = tableComment;
      this.columnComments = columnComments;
    }
    
    @Override public @Nullable String getTableComment() {
      return tableComment;
    }
    
    @Override public @Nullable String getColumnComment(String columnName) {
      return columnComments.get(columnName.toLowerCase());
    }
    
    // Delegate all other Table methods
    
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return delegate.getRowType(typeFactory);
    }
    
    @Override public org.apache.calcite.schema.Statistic getStatistic() {
      return delegate.getStatistic();
    }
    
    @Override public org.apache.calcite.schema.Schema.TableType getJdbcTableType() {
      return delegate.getJdbcTableType();
    }
    
    @Override public boolean isRolledUp(String column) {
      return delegate.isRolledUp(column);
    }
    
    @Override public boolean rolledUpColumnValidInsideAgg(String column, 
        org.apache.calcite.sql.SqlCall call,
        org.apache.calcite.sql.SqlNode parent, 
        org.apache.calcite.config.CalciteConnectionConfig config) {
      return delegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }
  }

  @Override public @Nullable String getComment() {
    return "U.S. government geographic data including Census TIGER boundaries, "
        + "HUD USPS ZIP code crosswalks, and demographic statistics. "
        + "Enables spatial analysis, geographic aggregation, and crosswalk between "
        + "ZIP codes, counties, census tracts, congressional districts, and metropolitan areas.";
  }
}