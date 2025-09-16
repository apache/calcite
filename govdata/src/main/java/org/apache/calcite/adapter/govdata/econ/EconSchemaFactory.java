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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Schema factory for U.S. economic data sources.
 *
 * <p>Provides access to economic data from:
 * <ul>
 *   <li>Bureau of Labor Statistics (BLS) - Employment, inflation, wages</li>
 *   <li>Federal Reserve (FRED) - Interest rates, GDP, economic indicators</li>
 *   <li>U.S. Treasury - Treasury yields, auction results, debt statistics</li>
 *   <li>Bureau of Economic Analysis (BEA) - GDP components, trade data</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "ECON",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.econ.EconSchemaFactory",
 *     "operand": {
 *       "blsApiKey": "${BLS_API_KEY}",
 *       "fredApiKey": "${FRED_API_KEY}",
 *       "updateFrequency": "daily",
 *       "historicalDepth": "10 years",
 *       "enabledSources": ["bls", "fred", "treasury"],
 *       "cacheDirectory": "${ECON_CACHE_DIR:/tmp/econ-cache}"
 *     }
 *   }]
 * }
 * </pre>
 */
public class EconSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchemaFactory.class);

  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.info("Creating economic data schema: {}", name);
    
    // Read environment variables at runtime (not static initialization)
    // Check both actual environment variables and system properties (for tests)
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
    
    // ECON data directories
    String econRawDir = govdataCacheDir + "/econ";
    String econParquetDir = govdataParquetDir + "/source=econ";
    
    // Use unified govdata directory structure (matching GEO pattern)
    LOGGER.info("Using unified govdata directories - cache: {}, parquet: {}", 
        econRawDir, econParquetDir);
    
    // Get year range from unified environment variables
    Integer startYear = getConfiguredStartYear(operand);
    Integer endYear = getConfiguredEndYear(operand);
    
    LOGGER.info("Economic data configuration:");
    LOGGER.info("  Cache directory: {}", econRawDir);
    LOGGER.info("  Parquet directory: {}", econParquetDir);
    LOGGER.info("  Year range: {} - {}", startYear, endYear);
    
    // Extract API keys from environment variables or system properties
    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) {
      blsApiKey = System.getProperty("BLS_API_KEY");
    }
    
    String fredApiKey = System.getenv("FRED_API_KEY");
    if (fredApiKey == null) {
      fredApiKey = System.getProperty("FRED_API_KEY");
    }
    
    String beaApiKey = System.getenv("BEA_API_KEY");
    if (beaApiKey == null) {
      beaApiKey = System.getProperty("BEA_API_KEY");
    }
    
    // Check for operand overrides (model can override environment)
    if (operand.get("blsApiKey") != null) {
      blsApiKey = (String) operand.get("blsApiKey");
    }
    if (operand.get("fredApiKey") != null) {
      fredApiKey = (String) operand.get("fredApiKey");
    }
    if (operand.get("beaApiKey") != null) {
      beaApiKey = (String) operand.get("beaApiKey");
    }
    
    // Get enabled sources
    @SuppressWarnings("unchecked")
    List<String> enabledSources = (List<String>) operand.get("enabledSources");
    if (enabledSources == null) {
      enabledSources = java.util.Arrays.asList("bls", "fred", "treasury", "bea", "worldbank");
    }
    
    LOGGER.info("  Enabled sources: {}", enabledSources);
    LOGGER.info("  BLS API key: {}", blsApiKey != null ? "configured" : "not configured");
    LOGGER.info("  FRED API key: {}", fredApiKey != null ? "configured" : "not configured");
    LOGGER.info("  BEA API key: {}", beaApiKey != null ? "configured" : "not configured");
    
    // Check auto-download setting (default true like GEO)
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (autoDownload == null) {
      autoDownload = true;
    }
    
    // Create mutable operand for modifications
    Map<String, Object> mutableOperand = new java.util.HashMap<>(operand);
    mutableOperand.put("startYear", startYear);
    mutableOperand.put("endYear", endYear);
    mutableOperand.put("blsApiKey", blsApiKey);
    mutableOperand.put("fredApiKey", fredApiKey);
    mutableOperand.put("beaApiKey", beaApiKey);
    
    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.info("Auto-download enabled for ECON data");
      try {
        downloadEconData(mutableOperand, econRawDir, econParquetDir, 
            blsApiKey, fredApiKey, beaApiKey, enabledSources, startYear, endYear);
      } catch (Exception e) {
        LOGGER.error("Error downloading ECON data", e);
        // Continue even if download fails - existing data may be available
      }
    }
    
    // Configure for FileSchemaFactory (following GEO pattern)
    mutableOperand.put("directory", econParquetDir);
    
    // Set execution engine to PARQUET
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
    
    // Build table definitions for econ tables
    List<Map<String, Object>> econTables = buildEconTableDefinitions(econParquetDir, startYear, endYear);
    if (!econTables.isEmpty()) {
      mutableOperand.put("partitionedTables", econTables);
    }
    
    // Add constraint definitions
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
    if (enableConstraints == null) {
      enableConstraints = true;
    }
    
    if (enableConstraints) {
      Map<String, Map<String, Object>> econConstraints = defineEconTableConstraints();
      if (tableConstraints != null) {
        econConstraints.putAll(tableConstraints);
      }
      mutableOperand.put("constraintMetadata", econConstraints);
    }
    
    // Delegate to FileSchemaFactory (like GEO and SEC do)
    LOGGER.info("Delegating to FileSchemaFactory for ECON schema creation");
    return org.apache.calcite.adapter.file.FileSchemaFactory.INSTANCE.create(parentSchema, name, mutableOperand);
  }
  
  /**
   * Get configured start year from operand or environment.
   */
  private Integer getConfiguredStartYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("startYear");
    if (year != null) return year;
    
    String envYear = System.getenv("GOVDATA_START_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", envYear);
      }
    }
    
    // Default to 5 years ago
    return java.time.Year.now().getValue() - 5;
  }
  
  /**
   * Get configured end year from operand or environment.
   */
  private Integer getConfiguredEndYear(Map<String, Object> operand) {
    Integer year = (Integer) operand.get("endYear");
    if (year != null) return year;
    
    String envYear = System.getenv("GOVDATA_END_YEAR");
    if (envYear != null) {
      try {
        return Integer.parseInt(envYear);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", envYear);
      }
    }
    
    // Default to current year
    return java.time.Year.now().getValue();
  }
  
  /**
   * Download economic data from various sources following GEO pattern.
   */
  private void downloadEconData(Map<String, Object> operand, String cacheDir, String parquetDir,
      String blsApiKey, String fredApiKey, String beaApiKey, List<String> enabledSources,
      int startYear, int endYear) throws IOException {
    
    LOGGER.info("Downloading economic data to: {}", cacheDir);
    
    // Create cache and parquet directories
    File cacheDirFile = new File(cacheDir);
    File parquetDirFile = new File(parquetDir);
    if (!cacheDirFile.exists()) {
      cacheDirFile.mkdirs();
    }
    if (!parquetDirFile.exists()) {
      parquetDirFile.mkdirs();
    }
    
    // Create hive-partitioned subdirectories
    File timeseriesDir = new File(parquetDir, "type=timeseries");
    File indicatorsDir = new File(parquetDir, "type=indicators");
    File regionalDir = new File(parquetDir, "type=regional");
    timeseriesDir.mkdirs();
    indicatorsDir.mkdirs();
    regionalDir.mkdirs();
    
    // Download BLS data if enabled
    if (enabledSources.contains("bls") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BLS data for years {}-{}", startYear, endYear);
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir);
        
        // Download all BLS data for the year range
        blsDownloader.downloadAll(startYear, endYear);
        
        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert employment statistics  
          File employmentParquet = new File(indicatorsDir, "year=" + year + "/employment_statistics.parquet");
          employmentParquet.getParentFile().mkdirs();
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), employmentParquet);
          
          // Convert inflation metrics
          File inflationParquet = new File(indicatorsDir, "year=" + year + "/inflation_metrics.parquet");
          inflationParquet.getParentFile().mkdirs();
          blsDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), inflationParquet);
        }
        
        LOGGER.info("BLS data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BLS data", e);
      }
    }
    
    // Download FRED data if enabled
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading FRED data for years {}-{}", startYear, endYear);
        FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, fredApiKey);
        
        // Download all FRED data for the year range
        fredDownloader.downloadAll(startYear, endYear);
        
        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          File fredParquet = new File(indicatorsDir, "year=" + year + "/fred_indicators.parquet");
          fredParquet.getParentFile().mkdirs();
          fredDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), fredParquet);
        }
        
        LOGGER.info("FRED data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED data", e);
      }
    }
    
    // Download Treasury data if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      try {
        LOGGER.info("Downloading Treasury data for years {}-{}", startYear, endYear);
        TreasuryDataDownloader treasuryDownloader = new TreasuryDataDownloader(cacheDir);
        
        // Download all Treasury data for the year range
        treasuryDownloader.downloadAll(startYear, endYear);
        
        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          File yieldsParquet = new File(timeseriesDir, "year=" + year + "/treasury_yields.parquet");
          yieldsParquet.getParentFile().mkdirs();
          treasuryDownloader.convertToParquet(new File(cacheDir, "source=econ/type=timeseries/year=" + year), yieldsParquet);
          
          // Convert federal debt data to parquet
          File debtParquet = new File(timeseriesDir, "year=" + year + "/federal_debt.parquet");
          debtParquet.getParentFile().mkdirs();
          treasuryDownloader.convertFederalDebtToParquet(new File(cacheDir, "source=econ/type=timeseries/year=" + year), debtParquet);
        }
        
        LOGGER.info("Treasury data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Treasury data", e);
      }
    }
    
    // Download BEA data if enabled
    if (enabledSources.contains("bea") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BEA data for years {}-{}", startYear, endYear);
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, beaApiKey);
        
        // Download all BEA data for the year range
        beaDownloader.downloadAll(startYear, endYear);
        
        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert GDP components
          File gdpParquet = new File(indicatorsDir, "year=" + year + "/gdp_components.parquet");
          gdpParquet.getParentFile().mkdirs();
          beaDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), gdpParquet);
          
          // Convert regional income - use the specific converter method
          File regionalIncomeParquet = new File(indicatorsDir, "year=" + year + "/regional_income.parquet");
          beaDownloader.convertRegionalIncomeToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), regionalIncomeParquet);
          
          // Convert BEA trade statistics, ITA data, and industry GDP data
          File tradeParquet = new File(indicatorsDir, "year=" + year + "/trade_statistics.parquet");
          beaDownloader.convertTradeStatisticsToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), tradeParquet);
          
          File itaParquet = new File(indicatorsDir, "year=" + year + "/ita_data.parquet");
          beaDownloader.convertItaDataToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), itaParquet);
          
          File industryGdpParquet = new File(indicatorsDir, "year=" + year + "/industry_gdp.parquet");
          beaDownloader.convertIndustryGdpToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), industryGdpParquet);
        }
        
        LOGGER.info("BEA data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BEA data", e);
      }
    }
    
    // Download World Bank data if enabled (no API key required)
    if (enabledSources.contains("worldbank")) {
      try {
        LOGGER.info("Downloading World Bank data for years {}-{}", startYear, endYear);
        WorldBankDataDownloader worldBankDownloader = new WorldBankDataDownloader(cacheDir);
        
        // Download all World Bank data for the year range
        worldBankDownloader.downloadAll(startYear, endYear);
        
        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          File worldParquet = new File(indicatorsDir, "year=" + year + "/world_indicators.parquet");
          worldParquet.getParentFile().mkdirs();
          worldBankDownloader.convertToParquet(new File(cacheDir, "source=econ/type=indicators/year=" + year), worldParquet);
        }
        
        LOGGER.info("World Bank data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading World Bank data", e);
      }
    }
    
    LOGGER.info("ECON data download completed");
  }
  
  /**
   * Build table definitions for ECON schema following GEO pattern.
   */
  private List<Map<String, Object>> buildEconTableDefinitions(String parquetDir, int startYear, int endYear) {
    List<Map<String, Object>> tables = new java.util.ArrayList<>();
    File parquetDirFile = new File(parquetDir);
    
    // Check if the parquet directory exists
    if (!parquetDirFile.exists() || !parquetDirFile.isDirectory()) {
      LOGGER.warn("ECON parquet directory does not exist: {}", parquetDir);
      return createEconTableDefinitions(); // Return default table definitions
    }
    
    // Scan for existing parquet files in hive-partitioned structure
    // Pattern: type=<type>/year=<year>/*.parquet
    File[] typePartitions = parquetDirFile.listFiles((dir, name) -> name.startsWith("type=") && !name.startsWith("."));
    if (typePartitions == null || typePartitions.length == 0) {
      LOGGER.info("No type partitions found in ECON parquet directory, using default table definitions");
      return createEconTableDefinitions();
    }
    
    // Track which tables we've found data for
    java.util.Set<String> foundTables = new java.util.HashSet<>();
    
    for (File typePartition : typePartitions) {
      String typeName = typePartition.getName().substring(5); // Remove "type=" prefix
      
      // Check for year partitions within this type
      File[] yearPartitions = typePartition.listFiles((dir, name) -> name.startsWith("year=") && !name.startsWith("."));
      if (yearPartitions != null) {
        for (File yearPartition : yearPartitions) {
          // List parquet files in this year partition
          File[] parquetFiles = yearPartition.listFiles((dir, name) -> name.endsWith(".parquet") && !name.startsWith("."));
          if (parquetFiles != null) {
            for (File parquetFile : parquetFiles) {
              String tableName = parquetFile.getName().replace(".parquet", "");
              if (!foundTables.contains(tableName)) {
                foundTables.add(tableName);
                
                // Create table definition
                Map<String, Object> tableDef = new java.util.HashMap<>();
                tableDef.put("name", tableName.toLowerCase());
                tableDef.put("type", "PartitionedParquetTable");
                
                // Build the correct pattern based on the type partition
                // typeName is already extracted above (line 448)
                String pattern = "type=" + typeName + "/year=*/" + tableName + ".parquet";
                tableDef.put("pattern", pattern);
                
                // Add comment based on table name
                String comment = getTableComment(tableName);
                if (comment != null) {
                  tableDef.put("comment", comment);
                }
                
                tables.add(tableDef);
                LOGGER.debug("Found ECON table: {} in {}", tableName, yearPartition.getPath());
              }
            }
          }
        }
      }
    }
    
    // If no tables found, return default definitions
    if (tables.isEmpty()) {
      LOGGER.info("No parquet files found in ECON directory, using default table definitions");
      return createEconTableDefinitions();
    }
    
    LOGGER.info("Built {} ECON table definitions from parquet directory", tables.size());
    return tables;
  }
  
  /**
   * Get table comment based on table name.
   */
  private String getTableComment(String tableName) {
    switch (tableName) {
      case "employment_statistics":
        return "U.S. employment and unemployment statistics from BLS including national "
            + "unemployment rate, labor force participation, job openings, and employment by sector. "
            + "Updated monthly with seasonal adjustments.";
      case "inflation_metrics":
        return "Consumer Price Index (CPI) and Producer Price Index (PPI) data tracking "
            + "inflation across different categories of goods and services. Includes urban, regional, "
            + "and sector-specific inflation rates.";
      case "wage_growth":
        return "Average hourly earnings, weekly earnings, and employment cost index by "
            + "industry and occupation. Tracks wage growth trends and labor cost pressures.";
      case "regional_employment":
        return "State and metropolitan area employment statistics including "
            + "unemployment rates, job growth, and labor force participation by geographic region.";
      case "treasury_yields":
        return "Daily U.S. Treasury yield curve rates from 1-month to 30-year maturities. "
            + "Includes nominal yields, TIPS yields, and yield curve shape indicators. "
            + "Source: U.S. Treasury Direct API.";
      case "federal_debt":
        return "U.S. federal debt statistics including total public debt, debt held by public, "
            + "and intragovernmental holdings. Tracks debt levels, composition, and trends. "
            + "Source: Treasury Fiscal Data API.";
      case "world_indicators":
        return "International economic indicators from World Bank for major economies. "
            + "Includes GDP, inflation, unemployment, government debt, and population statistics. "
            + "Enables comparison of U.S. economic performance with global peers.";
      case "fred_indicators":
        return "Federal Reserve Economic Data (FRED) time series including GDP, "
            + "interest rates, monetary aggregates, and other key economic indicators.";
      case "gdp_components":
        return "Detailed GDP components from BEA including consumption, investment, "
            + "government spending, and net exports broken down by subcategories.";
      case "consumer_price_index":
        return "Detailed CPI data by category and geographic area from BLS.";
      case "gdp_statistics":
        return "Quarterly and annual GDP growth rates, nominal and real GDP values.";
      case "regional_income":
        return "State and regional personal income statistics from BEA Regional Economic Accounts. "
            + "Includes total income, per capita income, and population by state.";
      case "trade_statistics":
        return "Detailed U.S. export and import statistics from BEA NIPA Table T40205B. "
            + "Comprehensive breakdown of goods and services trade by category with calculated trade balances.";
      case "ita_data":
        return "International Transactions Accounts (ITA) from BEA providing balance of payments statistics. "
            + "Includes trade balance, current account, capital account, and income balances.";
      case "industry_gdp":
        return "GDP by Industry data from BEA showing value added by NAICS industry sectors. "
            + "Provides comprehensive breakdown of economic output by industry including "
            + "agriculture, mining, manufacturing, services, and government sectors. Available "
            + "at both annual and quarterly frequencies for detailed sectoral analysis.";
      default:
        return null;
    }
  }

  /**
   * Create table definitions for ECON schema.
   */
  private static List<Map<String, Object>> createEconTableDefinitions() {
    List<Map<String, Object>> tables = new java.util.ArrayList<>();
    
    // BLS tables
    Map<String, Object> employmentStats = new java.util.HashMap<>();
    employmentStats.put("name", "employment_statistics");
    employmentStats.put("type", "PartitionedParquetTable");
    employmentStats.put("pattern", "type=indicators/year=*/employment_statistics.parquet");
    employmentStats.put("comment", "U.S. employment and unemployment statistics from BLS including national "
        + "unemployment rate, labor force participation, job openings, and employment by sector. "
        + "Updated monthly with seasonal adjustments.");
    tables.add(employmentStats);
    
    Map<String, Object> inflationMetrics = new java.util.HashMap<>();
    inflationMetrics.put("name", "inflation_metrics");
    inflationMetrics.put("type", "PartitionedParquetTable");
    inflationMetrics.put("pattern", "type=indicators/year=*/inflation_metrics.parquet");
    inflationMetrics.put("comment", "Consumer Price Index (CPI) and Producer Price Index (PPI) data tracking "
        + "inflation across different categories of goods and services. Includes urban, regional, "
        + "and sector-specific inflation rates.");
    tables.add(inflationMetrics);
    
    Map<String, Object> wageGrowth = new java.util.HashMap<>();
    wageGrowth.put("name", "wage_growth");
    wageGrowth.put("type", "PartitionedParquetTable");
    wageGrowth.put("pattern", "type=indicators/year=*/wage_growth.parquet");
    wageGrowth.put("comment", "Average hourly earnings, weekly earnings, and employment cost index by "
        + "industry and occupation. Tracks wage growth trends and labor cost pressures.");
    tables.add(wageGrowth);
    
    Map<String, Object> regionalEmployment = new java.util.HashMap<>();
    regionalEmployment.put("name", "regional_employment");
    regionalEmployment.put("type", "PartitionedParquetTable");
    regionalEmployment.put("pattern", "type=regional/year=*/regional_employment.parquet");
    regionalEmployment.put("comment", "State and metropolitan area employment statistics including "
        + "unemployment rates, job growth, and labor force participation by geographic region.");
    tables.add(regionalEmployment);
    
    // Treasury tables
    Map<String, Object> treasuryYields = new java.util.HashMap<>();
    treasuryYields.put("name", "treasury_yields");
    treasuryYields.put("type", "PartitionedParquetTable");
    treasuryYields.put("pattern", "type=timeseries/year=*/treasury_yields.parquet");
    treasuryYields.put("comment", "Daily U.S. Treasury yield curve rates from 1-month to 30-year maturities. "
        + "Includes nominal yields, TIPS yields, and yield curve shape indicators. "
        + "Source: U.S. Treasury Direct API.");
    tables.add(treasuryYields);
    
    Map<String, Object> federalDebt = new java.util.HashMap<>();
    federalDebt.put("name", "federal_debt");
    federalDebt.put("type", "PartitionedParquetTable");
    federalDebt.put("pattern", "type=timeseries/year=*/federal_debt.parquet");
    federalDebt.put("comment", "U.S. federal debt statistics including total public debt, debt held by public, "
        + "and intragovernmental holdings. Tracks debt levels, composition, and trends. "
        + "Source: Treasury Fiscal Data API.");
    tables.add(federalDebt);
    
    // World Bank tables
    Map<String, Object> worldIndicators = new java.util.HashMap<>();
    worldIndicators.put("name", "world_indicators");
    worldIndicators.put("type", "PartitionedParquetTable");
    worldIndicators.put("pattern", "type=indicators/year=*/world_indicators.parquet");
    worldIndicators.put("comment", "International economic indicators from World Bank for major economies. "
        + "Includes GDP, inflation, unemployment, government debt, and population statistics. "
        + "Enables comparison of U.S. economic performance with global peers.");
    tables.add(worldIndicators);
    
    // FRED indicators table
    Map<String, Object> fredIndicators = new java.util.HashMap<>();
    fredIndicators.put("name", "fred_indicators");
    fredIndicators.put("type", "PartitionedParquetTable");
    fredIndicators.put("pattern", "type=indicators/year=*/fred_indicators.parquet");
    fredIndicators.put("comment", "Federal Reserve Economic Data (FRED) time series including GDP, "
        + "interest rates, monetary aggregates, and other key economic indicators.");
    tables.add(fredIndicators);
    
    // GDP components table
    Map<String, Object> gdpComponents = new java.util.HashMap<>();
    gdpComponents.put("name", "gdp_components");
    gdpComponents.put("type", "PartitionedParquetTable");
    gdpComponents.put("pattern", "type=indicators/year=*/gdp_components.parquet");
    gdpComponents.put("comment", "Detailed GDP components from BEA including consumption, investment, "
        + "government spending, and net exports broken down by subcategories.");
    tables.add(gdpComponents);
    
    // Consumer price index table
    Map<String, Object> consumerPriceIndex = new java.util.HashMap<>();
    consumerPriceIndex.put("name", "consumer_price_index");
    consumerPriceIndex.put("type", "PartitionedParquetTable");
    consumerPriceIndex.put("pattern", "type=indicators/year=*/consumer_price_index.parquet");
    consumerPriceIndex.put("comment", "Detailed CPI data by category and geographic area from BLS.");
    tables.add(consumerPriceIndex);
    
    // GDP statistics table
    Map<String, Object> gdpStatistics = new java.util.HashMap<>();
    gdpStatistics.put("name", "gdp_statistics");
    gdpStatistics.put("type", "PartitionedParquetTable");
    gdpStatistics.put("pattern", "type=indicators/year=*/gdp_statistics.parquet");
    gdpStatistics.put("comment", "Quarterly and annual GDP growth rates, nominal and real GDP values.");
    tables.add(gdpStatistics);
    
    // Regional income table (BEA)
    Map<String, Object> regionalIncome = new java.util.HashMap<>();
    regionalIncome.put("name", "regional_income");
    regionalIncome.put("type", "PartitionedParquetTable");
    regionalIncome.put("pattern", "type=indicators/year=*/regional_income.parquet");
    regionalIncome.put("comment", "State and regional personal income statistics from BEA Regional Economic Accounts. "
        + "Includes total income, per capita income, and population by state.");
    tables.add(regionalIncome);
    
    // Trade statistics table (BEA NIPA)
    Map<String, Object> tradeStatistics = new java.util.HashMap<>();
    tradeStatistics.put("name", "trade_statistics");
    tradeStatistics.put("type", "PartitionedParquetTable");
    tradeStatistics.put("pattern", "type=indicators/year=*/trade_statistics.parquet");
    tradeStatistics.put("comment", "Detailed U.S. export and import statistics from BEA NIPA Table T40205B. "
        + "Comprehensive breakdown of goods and services trade by category with calculated trade balances.");
    tables.add(tradeStatistics);
    
    // ITA data table (BEA International Transactions)
    Map<String, Object> itaData = new java.util.HashMap<>();
    itaData.put("name", "ita_data");
    itaData.put("type", "PartitionedParquetTable");
    itaData.put("pattern", "type=indicators/year=*/ita_data.parquet");
    itaData.put("comment", "International Transactions Accounts (ITA) from BEA providing balance of payments statistics. "
        + "Includes trade balance, current account, capital account, and income balances.");
    tables.add(itaData);
    
    return tables;
  }
  
  /**
   * Define automatic constraint metadata for ECON tables.
   */
  private Map<String, Map<String, Object>> defineEconTableConstraints() {
    Map<String, Map<String, Object>> constraints = new java.util.HashMap<>();
    
    // ======================== PRIMARY KEYS ========================
    
    // employment_statistics table
    Map<String, Object> employmentConstraints = new java.util.HashMap<>();
    employmentConstraints.put("primaryKey", java.util.Arrays.asList("date", "series_id"));
    constraints.put("employment_statistics", employmentConstraints);
    
    // inflation_metrics table
    Map<String, Object> inflationConstraints = new java.util.HashMap<>();
    inflationConstraints.put("primaryKey", java.util.Arrays.asList("date", "index_type", "item_code", "area_code"));
    constraints.put("inflation_metrics", inflationConstraints);
    
    // wage_growth table
    Map<String, Object> wageConstraints = new java.util.HashMap<>();
    wageConstraints.put("primaryKey", java.util.Arrays.asList("date", "series_id", "industry_code", "occupation_code"));
    constraints.put("wage_growth", wageConstraints);
    
    // regional_employment table
    Map<String, Object> regionalConstraints = new java.util.HashMap<>();
    regionalConstraints.put("primaryKey", java.util.Arrays.asList("date", "area_code"));
    
    // treasury_yields table
    Map<String, Object> treasuryYieldsConstraints = new java.util.HashMap<>();
    treasuryYieldsConstraints.put("primaryKey", java.util.Arrays.asList("date", "maturity_months"));
    constraints.put("treasury_yields", treasuryYieldsConstraints);
    
    // federal_debt table
    Map<String, Object> federalDebtConstraints = new java.util.HashMap<>();
    federalDebtConstraints.put("primaryKey", java.util.Arrays.asList("date", "debt_type"));
    constraints.put("federal_debt", federalDebtConstraints);
    
    // world_indicators table
    Map<String, Object> worldIndicatorsConstraints = new java.util.HashMap<>();
    worldIndicatorsConstraints.put("primaryKey", java.util.Arrays.asList("country_code", "indicator_code", "year"));
    constraints.put("world_indicators", worldIndicatorsConstraints);
    
    // fred_indicators table
    Map<String, Object> fredIndicatorsConstraints = new java.util.HashMap<>();
    fredIndicatorsConstraints.put("primaryKey", java.util.Arrays.asList("series_id", "date"));
    constraints.put("fred_indicators", fredIndicatorsConstraints);
    
    // gdp_components table
    Map<String, Object> gdpComponentsConstraints = new java.util.HashMap<>();
    gdpComponentsConstraints.put("primaryKey", java.util.Arrays.asList("table_id", "line_number", "year"));
    constraints.put("gdp_components", gdpComponentsConstraints);
    
    // regional_income table
    Map<String, Object> regionalIncomeConstraints = new java.util.HashMap<>();
    regionalIncomeConstraints.put("primaryKey", java.util.Arrays.asList("geo_fips", "metric", "year"));
    
    // ======================== FOREIGN KEYS ========================
    
    // regional_employment -> geo.tiger_states (state_code matches state_code)
    java.util.List<Map<String, Object>> regionalEmploymentFks = new java.util.ArrayList<>();
    Map<String, Object> regionalToStatesFk = new java.util.HashMap<>();
    regionalToStatesFk.put("columns", java.util.Arrays.asList("state_code"));
    regionalToStatesFk.put("targetSchema", "geo");
    regionalToStatesFk.put("targetTable", "tiger_states");
    regionalToStatesFk.put("targetColumns", java.util.Arrays.asList("state_code"));
    regionalEmploymentFks.add(regionalToStatesFk);
    
    // When area_type = 'county', area_code could reference geo.tiger_counties.county_fips
    // However, area_code format varies by area_type (state/MSA/county), so we can't enforce this
    
    regionalConstraints.put("foreignKeys", regionalEmploymentFks);
    constraints.put("regional_employment", regionalConstraints);
    
    // regional_income -> geo.tiger_states (when geo_fips is 2-digit state FIPS)
    // Note: geo_fips can be state FIPS (2-digit) or other geographic codes
    // We implement this as a partial FK - it will work for state-level data
    java.util.List<Map<String, Object>> regionalIncomeFks = new java.util.ArrayList<>();
    Map<String, Object> regionalIncomeToStatesFk = new java.util.HashMap<>();
    regionalIncomeToStatesFk.put("columns", java.util.Arrays.asList("geo_fips"));
    regionalIncomeToStatesFk.put("targetSchema", "geo");
    regionalIncomeToStatesFk.put("targetTable", "tiger_states");
    regionalIncomeToStatesFk.put("targetColumns", java.util.Arrays.asList("state_fips"));
    regionalIncomeFks.add(regionalIncomeToStatesFk);
    
    regionalIncomeConstraints.put("foreignKeys", regionalIncomeFks);
    constraints.put("regional_income", regionalIncomeConstraints);
    
    // ======================== INTRA-SCHEMA RELATIONSHIPS ========================
    
    // employment_statistics -> fred_indicators (series_id overlap)
    java.util.List<Map<String, Object>> employmentStatisticsFks = new java.util.ArrayList<>();
    Map<String, Object> employmentToFredFk = new java.util.HashMap<>();
    employmentToFredFk.put("columns", java.util.Arrays.asList("series_id"));
    employmentToFredFk.put("targetSchema", "econ");
    employmentToFredFk.put("targetTable", "fred_indicators");
    employmentToFredFk.put("targetColumns", java.util.Arrays.asList("series_id"));
    employmentStatisticsFks.add(employmentToFredFk);
    
    Map<String, Object> existingEmploymentConstraints = constraints.get("employment_statistics");
    if (existingEmploymentConstraints == null) {
      existingEmploymentConstraints = new java.util.HashMap<>();
      existingEmploymentConstraints.put("primaryKey", java.util.Arrays.asList("date", "series_id"));
    }
    existingEmploymentConstraints.put("foreignKeys", employmentStatisticsFks);
    constraints.put("employment_statistics", existingEmploymentConstraints);
    
    // inflation_metrics -> regional_employment (area_code overlap when both geographic)
    java.util.List<Map<String, Object>> inflationMetricsFks = new java.util.ArrayList<>();
    Map<String, Object> inflationToRegionalFk = new java.util.HashMap<>();
    inflationToRegionalFk.put("columns", java.util.Arrays.asList("area_code"));
    inflationToRegionalFk.put("targetSchema", "econ");
    inflationToRegionalFk.put("targetTable", "regional_employment");
    inflationToRegionalFk.put("targetColumns", java.util.Arrays.asList("area_code"));
    inflationMetricsFks.add(inflationToRegionalFk);
    
    inflationConstraints.put("foreignKeys", inflationMetricsFks);
    
    // gdp_components -> fred_indicators (GDP series temporal relationship)
    java.util.List<Map<String, Object>> gdpComponentsFks = new java.util.ArrayList<>();
    Map<String, Object> gdpToFredFk = new java.util.HashMap<>();
    gdpToFredFk.put("columns", java.util.Arrays.asList("table_id"));
    gdpToFredFk.put("targetSchema", "econ");
    gdpToFredFk.put("targetTable", "fred_indicators");
    gdpToFredFk.put("targetColumns", java.util.Arrays.asList("series_id"));
    gdpComponentsFks.add(gdpToFredFk);
    
    gdpComponentsConstraints.put("foreignKeys", gdpComponentsFks);
    
    // ======================== CROSS-SCHEMA TO SEC ========================
    
    // Note: SEC.filing_metadata -> GEO.tiger_states is handled in SEC schema factory
    // We document the relationship here for completeness but don't implement the reverse
    // Economic data is aggregate/macro level while SEC is company-specific
    
    // ======================== CROSS-SCHEMA TO GEO ========================
    
    // Additional potential relationships (documented but not enforced due to format variations):
    // - inflation_metrics.area_code -> geo.tiger_cbsa.cbsa_code (when area_code is MSA)
    // - wage_growth could potentially have geographic dimensions in future
    
    // ======================== UNIQUE CONSTRAINTS ========================
    
    // Add unique constraints where applicable (beyond primary keys)
    // Most primary keys already ensure uniqueness for the key business dimensions
    
    return constraints;
  }

  @Override public boolean supportsConstraints() {
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}