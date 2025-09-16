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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts BLS economic data to Parquet format.
 * Supports employment statistics, inflation metrics, wage growth, and regional employment data.
 */
public class BlsDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlsDataDownloader.class);
  private static final String BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private final String apiKey;
  private final String cacheDir;
  private final HttpClient httpClient;
  private final StorageProvider storageProvider;
  
  // Common BLS series IDs
  public static class Series {
    // Employment Statistics
    public static final String UNEMPLOYMENT_RATE = "LNS14000000";
    public static final String EMPLOYMENT_LEVEL = "CES0000000001";
    public static final String LABOR_FORCE_PARTICIPATION = "LNS11300000";
    
    // Inflation Metrics
    public static final String CPI_ALL_URBAN = "CUUR0000SA0";
    public static final String CPI_CORE = "CUUR0000SA0L1E";
    public static final String PPI_FINAL_DEMAND = "WPUFD4";
    
    // Wage Growth
    public static final String AVG_HOURLY_EARNINGS = "CES0500000003";
    public static final String EMPLOYMENT_COST_INDEX = "CIU1010000000000A";
    
    // Regional Employment (examples)
    public static final String CA_UNEMPLOYMENT = "LASST060000000000003";
    public static final String NY_UNEMPLOYMENT = "LASST360000000000003";
    public static final String TX_UNEMPLOYMENT = "LASST480000000000003";
  }
  
  public BlsDataDownloader(String apiKey, String cacheDir, StorageProvider storageProvider) {
    this.apiKey = apiKey;
    this.cacheDir = cacheDir;
    this.storageProvider = storageProvider;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BlsDataDownloader(String apiKey, String cacheDir) {
    this.apiKey = apiKey;
    this.cacheDir = cacheDir;
    this.storageProvider = org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir);
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  /**
   * Gets the default start year from environment variables.
   * Falls back to GOVDATA_START_YEAR, then defaults to 5 years ago.
   */
  public static int getDefaultStartYear() {
    // First check for ECON-specific override
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return Integer.parseInt(econStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }
    
    // Fall back to unified setting
    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return Integer.parseInt(govdataStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }
    
    // Default to 5 years ago
    return LocalDate.now().getYear() - 5;
  }
  
  /**
   * Gets the default end year from environment variables.
   * Falls back to GOVDATA_END_YEAR, then defaults to current year.
   */
  public static int getDefaultEndYear() {
    // First check for ECON-specific override
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return Integer.parseInt(econEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }
    
    // Fall back to unified setting
    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return Integer.parseInt(govdataEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }
    
    // Default to current year
    return LocalDate.now().getYear();
  }
  
  /**
   * Downloads all BLS data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading BLS data for years {} to {}", startYear, endYear);
    
    // Download employment statistics
    downloadEmploymentStatistics(startYear, endYear);
    
    // Download inflation metrics  
    downloadInflationMetrics(startYear, endYear);
  }
  
  /**
   * Downloads employment statistics using default date range from environment.
   */
  public File downloadEmploymentStatistics() throws IOException, InterruptedException {
    return downloadEmploymentStatistics(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public File downloadEmploymentStatistics(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading employment statistics for {}-{}", startYear, endYear);
    
    // Download for each year separately to match FileSchema partitioning expectations
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
      storageProvider.createDirectories(outputDirPath);
    
    // Download key employment series
    List<String> seriesIds = List.of(
        Series.UNEMPLOYMENT_RATE,
        Series.EMPLOYMENT_LEVEL,
        Series.LABOR_FORCE_PARTICIPATION
    );
    
      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);
      
      // Save raw JSON data to cache directory
      String jsonFilePath = storageProvider.resolvePath(outputDirPath, "employment_statistics.json");
      storageProvider.writeFile(jsonFilePath, rawJson.getBytes(StandardCharsets.UTF_8));
      
      LOGGER.info("Employment statistics raw data saved for year {}: {}", year, jsonFilePath);
      lastFile = new File(jsonFilePath);
    }
    
    return lastFile;
  }
  
  /**
   * Downloads inflation metrics using default date range from environment.
   */
  public File downloadInflationMetrics() throws IOException, InterruptedException {
    return downloadInflationMetrics(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public File downloadInflationMetrics(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading inflation metrics for {}-{}", startYear, endYear);
    
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
      storageProvider.createDirectories(outputDirPath);
    
    List<String> seriesIds = List.of(
        Series.CPI_ALL_URBAN,
        Series.CPI_CORE,
        Series.PPI_FINAL_DEMAND
    );
    
      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);
      
      // Save raw JSON data to cache directory
      String jsonFilePath = storageProvider.resolvePath(outputDirPath, "inflation_metrics.json");
      storageProvider.writeFile(jsonFilePath, rawJson.getBytes(StandardCharsets.UTF_8));
      
      LOGGER.info("Inflation metrics raw data saved for year {}: {}", year, jsonFilePath);
      lastFile = new File(jsonFilePath);
    }
    
    return lastFile;
  }
  
  /**
   * Downloads wage growth using default date range from environment.
   */
  public File downloadWageGrowth() throws IOException, InterruptedException {
    return downloadWageGrowth(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads wage growth data and converts to Parquet.
   */
  public File downloadWageGrowth(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading wage growth data for {}-{}", startYear, endYear);
    
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      Path outputDir = Paths.get(cacheDir, "source=econ", "type=indicators", "year=" + year);
      Files.createDirectories(outputDir);
    
    List<String> seriesIds = List.of(
        Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX
    );
    
      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);
      
      // Save raw JSON data to cache directory
      File jsonFile = new File(outputDir.toFile(), "wage_growth.json");
      Files.writeString(jsonFile.toPath(), rawJson, StandardCharsets.UTF_8);
      
      LOGGER.info("Wage growth raw data saved for year {}: {}", year, jsonFile);
      lastFile = jsonFile;
    }
    
    return lastFile;
  }
  
  /**
   * Downloads regional employment using default date range from environment.
   */
  public File downloadRegionalEmployment() throws IOException, InterruptedException {
    return downloadRegionalEmployment(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads regional employment data for selected states.
   */
  public File downloadRegionalEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading regional employment data for {}-{}", startYear, endYear);
    
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      Path outputDir = Paths.get(cacheDir, "source=econ", "type=regional", "year=" + year);
      Files.createDirectories(outputDir);
    
    // Download data for major states
    List<String> seriesIds = List.of(
        Series.CA_UNEMPLOYMENT,
        Series.NY_UNEMPLOYMENT,
        Series.TX_UNEMPLOYMENT
    );
    
      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);
      
      // Save raw JSON data to cache directory
      File jsonFile = new File(outputDir.toFile(), "regional_employment.json");
      Files.writeString(jsonFile.toPath(), rawJson, StandardCharsets.UTF_8);
      
      LOGGER.info("Regional employment raw data saved for year {}: {}", year, jsonFile);
      lastFile = jsonFile;
    }
    
    return lastFile;
  }
  
  /**
   * Fetches raw JSON response for multiple BLS series in a single API call.
   */
  private String fetchMultipleSeriesRaw(
      List<String> seriesIds, int startYear, int endYear) throws IOException, InterruptedException {
    
    ObjectNode requestBody = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();
    seriesIds.forEach(seriesArray::add);
    requestBody.set("seriesid", seriesArray);
    requestBody.put("startyear", String.valueOf(startYear));
    requestBody.put("endyear", String.valueOf(endYear));
    
    if (apiKey != null && !apiKey.isEmpty()) {
      requestBody.put("registrationkey", apiKey);
    }
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(BLS_API_BASE + "timeseries/data/"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      throw new IOException("BLS API request failed with status: " + response.statusCode() + 
          " - Response: " + response.body());
    }
    
    return response.body();
  }

  /**
   * Fetches multiple BLS series in a single API call.
   */
  private Map<String, List<Map<String, Object>>> fetchMultipleSeries(
      List<String> seriesIds, int startYear, int endYear) throws IOException, InterruptedException {
    
    String rawJson = fetchMultipleSeriesRaw(seriesIds, startYear, endYear);
    return parseMultiSeriesResponse(rawJson);
  }
  
  /**
   * Parses BLS API response for multiple series.
   */
  private Map<String, List<Map<String, Object>>> parseMultiSeriesResponse(String jsonResponse) 
      throws IOException {
    
    Map<String, List<Map<String, Object>>> result = new HashMap<>();
    JsonNode root = MAPPER.readTree(jsonResponse);
    
    String status = root.path("status").asText();
    if (!"REQUEST_SUCCEEDED".equals(status)) {
      String message = root.path("message").asText("Unknown error");
      throw new IOException("BLS API error: " + message);
    }
    
    JsonNode seriesArray = root.path("Results").path("series");
    if (seriesArray.isArray()) {
      for (JsonNode series : seriesArray) {
        String seriesId = series.path("seriesID").asText();
        List<Map<String, Object>> data = new ArrayList<>();
        
        JsonNode dataArray = series.path("data");
        if (dataArray.isArray()) {
          for (JsonNode point : dataArray) {
            Map<String, Object> dataPoint = new HashMap<>();
            String year = point.path("year").asText();
            String period = point.path("period").asText();
            String value = point.path("value").asText();
            
            // Convert period to month
            int month = periodToMonth(period);
            if (month > 0) {
              dataPoint.put("date", LocalDate.of(Integer.parseInt(year), month, 1).toString());
              dataPoint.put("value", Double.parseDouble(value));
              dataPoint.put("series_id", seriesId);
              dataPoint.put("series_name", getSeriesName(seriesId));
              data.add(dataPoint);
            }
          }
        }
        
        result.put(seriesId, data);
      }
    }
    
    return result;
  }
  
  /**
   * Converts BLS period code to month.
   */
  private int periodToMonth(String period) {
    if (period.startsWith("M")) {
      return Integer.parseInt(period.substring(1));
    }
    return 0; // Annual or other non-monthly data
  }
  
  /**
   * Gets human-readable name for series ID.
   */
  private String getSeriesName(String seriesId) {
    switch (seriesId) {
      case Series.UNEMPLOYMENT_RATE: return "Unemployment Rate";
      case Series.EMPLOYMENT_LEVEL: return "Total Nonfarm Employment";
      case Series.LABOR_FORCE_PARTICIPATION: return "Labor Force Participation Rate";
      case Series.CPI_ALL_URBAN: return "CPI-U All Items";
      case Series.CPI_CORE: return "CPI-U Core (Less Food and Energy)";
      case Series.PPI_FINAL_DEMAND: return "PPI Final Demand";
      case Series.AVG_HOURLY_EARNINGS: return "Average Hourly Earnings";
      case Series.EMPLOYMENT_COST_INDEX: return "Employment Cost Index";
      case Series.CA_UNEMPLOYMENT: return "California Unemployment Rate";
      case Series.NY_UNEMPLOYMENT: return "New York Unemployment Rate";
      case Series.TX_UNEMPLOYMENT: return "Texas Unemployment Rate";
      default: return seriesId;
    }
  }
  
  /**
   * Writes employment statistics data to Parquet.
   */
  private void writeEmploymentStatisticsParquet(Map<String, List<Map<String, Object>>> seriesData,
      File outputFile) throws IOException {
    
    Schema schema = SchemaBuilder.record("employment_statistics")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredString("series_id")
        .requiredString("series_name")
        .requiredDouble("value")
        .optionalString("unit")
        .optionalBoolean("seasonally_adjusted")
        .optionalDouble("percent_change_month")
        .optionalDouble("percent_change_year")
        .optionalString("category")
        .optionalString("subcategory")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    
    try {
      
      for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
        for (Map<String, Object> dataPoint : entry.getValue()) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("date", dataPoint.get("date") != null ? dataPoint.get("date") : "");
          record.put("series_id", dataPoint.get("series_id") != null ? dataPoint.get("series_id") : entry.getKey());
          record.put("series_name", dataPoint.get("series_name") != null ? dataPoint.get("series_name") : getSeriesName(entry.getKey()));
          record.put("value", dataPoint.get("value") != null ? dataPoint.get("value") : 0.0);
          record.put("unit", getUnit(entry.getKey()));
          record.put("seasonally_adjusted", isSeasonallyAdjusted(entry.getKey()));
          record.put("category", "Employment");
          record.put("subcategory", getSubcategory(entry.getKey()));
          writer.write(record);
        }
      }
    } finally {
      writer.close();
    }
  }
  
  /**
   * Writes inflation metrics data to Parquet.
   */
  private void writeInflationMetricsParquet(Map<String, List<Map<String, Object>>> seriesData,
      File outputFile) throws IOException {
    
    Schema schema = SchemaBuilder.record("inflation_metrics")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredString("index_type")
        .requiredString("item_code")
        .requiredString("area_code")
        .requiredString("item_name")
        .requiredDouble("index_value")
        .optionalDouble("percent_change_month")
        .optionalDouble("percent_change_year")
        .optionalString("area_name")
        .optionalBoolean("seasonally_adjusted")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    
    try {
      
      for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
        String seriesId = entry.getKey();
        for (Map<String, Object> dataPoint : entry.getValue()) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("date", dataPoint.get("date") != null ? dataPoint.get("date") : "");
          record.put("index_type", getIndexType(seriesId));
          record.put("item_code", getItemCode(seriesId));
          record.put("area_code", "0000");  // National
          record.put("item_name", dataPoint.get("series_name") != null ? dataPoint.get("series_name") : getSeriesName(seriesId));
          record.put("index_value", dataPoint.get("value") != null ? dataPoint.get("value") : 0.0);
          record.put("area_name", "U.S. city average");
          record.put("seasonally_adjusted", isSeasonallyAdjusted(seriesId));
          writer.write(record);
        }
      }
    } finally {
      writer.close();
    }
  }
  
  /**
   * Writes wage growth data to Parquet.
   */
  private void writeWageGrowthParquet(Map<String, List<Map<String, Object>>> seriesData,
      File outputFile) throws IOException {
    
    Schema schema = SchemaBuilder.record("wage_growth")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredString("series_id")
        .requiredString("industry_code")
        .requiredString("occupation_code")
        .optionalString("industry_name")
        .optionalString("occupation_name")
        .optionalDouble("average_hourly_earnings")
        .optionalDouble("average_weekly_earnings")
        .optionalDouble("employment_cost_index")
        .optionalDouble("percent_change_year")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    
    try {
      
      for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
        String seriesId = entry.getKey();
        for (Map<String, Object> dataPoint : entry.getValue()) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("date", dataPoint.get("date"));
          record.put("series_id", seriesId);
          record.put("industry_code", "00");  // All industries
          record.put("occupation_code", "000000");  // All occupations
          record.put("industry_name", "All Industries");
          record.put("occupation_name", "All Occupations");
          
          if (seriesId.equals(Series.AVG_HOURLY_EARNINGS)) {
            record.put("average_hourly_earnings", dataPoint.get("value"));
          } else if (seriesId.equals(Series.EMPLOYMENT_COST_INDEX)) {
            record.put("employment_cost_index", dataPoint.get("value"));
          }
          
          writer.write(record);
        }
      }
    } finally {
      writer.close();
    }
  }
  
  /**
   * Writes regional employment data to Parquet.
   */
  private void writeRegionalEmploymentParquet(Map<String, List<Map<String, Object>>> seriesData,
      File outputFile) throws IOException {
    
    Schema schema = SchemaBuilder.record("regional_employment")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredString("area_code")
        .requiredString("area_name")
        .requiredString("area_type")
        .optionalString("state_code")
        .optionalDouble("unemployment_rate")
        .optionalLong("employment_level")
        .optionalLong("labor_force")
        .optionalDouble("participation_rate")
        .optionalDouble("employment_population_ratio")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    
    try {
      
      for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
        String seriesId = entry.getKey();
        String stateCode = getStateCode(seriesId);
        
        for (Map<String, Object> dataPoint : entry.getValue()) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("date", dataPoint.get("date"));
          record.put("area_code", stateCode);
          record.put("area_name", getStateName(stateCode));
          record.put("area_type", "state");
          record.put("state_code", stateCode);
          record.put("unemployment_rate", dataPoint.get("value"));
          writer.write(record);
        }
      }
    } finally {
      writer.close();
    }
  }
  
  private String getUnit(String seriesId) {
    if (seriesId.contains("RATE") || seriesId.contains("000003")) {
      return "percent";
    }
    return "index";
  }
  
  private boolean isSeasonallyAdjusted(String seriesId) {
    return !seriesId.contains("NSA");
  }
  
  private String getSubcategory(String seriesId) {
    if (seriesId.startsWith("LNS")) return "Labor Force Statistics";
    if (seriesId.startsWith("CES")) return "Current Employment Statistics";
    return "General";
  }
  
  private String getIndexType(String seriesId) {
    if (seriesId.startsWith("CUU")) return "CPI-U";
    if (seriesId.startsWith("WPU")) return "PPI";
    return "Other";
  }
  
  private String getItemCode(String seriesId) {
    if (seriesId.contains("SA0L1E")) return "Core";
    if (seriesId.contains("SA0")) return "All Items";
    return "Other";
  }
  
  private String getStateCode(String seriesId) {
    if (seriesId.contains("ST06")) return "CA";
    if (seriesId.contains("ST36")) return "NY";
    if (seriesId.contains("ST48")) return "TX";
    return "US";
  }
  
  private String getStateName(String stateCode) {
    switch (stateCode) {
      case "CA": return "California";
      case "NY": return "New York";
      case "TX": return "Texas";
      default: return "United States";
    }
  }
  
  
  /**
   * Converts cached BLS employment data to Parquet format.
   */
  public void convertToParquet(File sourceDir, File targetFile) throws IOException {
    LOGGER.info("Converting BLS data from {} to parquet: {}", sourceDir, targetFile);
    
    // Skip if target file already exists
    if (targetFile.exists()) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFile);
      return;
    }
    
    // Ensure target directory exists
    targetFile.getParentFile().mkdirs();
    
    // Read employment statistics JSON files and convert to employment_statistics.parquet
    if (targetFile.getName().equals("employment_statistics.parquet")) {
      convertEmploymentStatisticsToParquet(sourceDir, targetFile);
    } else if (targetFile.getName().equals("inflation_metrics.parquet")) {
      convertInflationMetricsToParquet(sourceDir, targetFile);
    }
  }
  
  private void convertEmploymentStatisticsToParquet(File sourceDir, File targetFile) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();
    
    // Look for employment statistics JSON files
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("employment_statistics.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);
          
          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();
              
              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" + 
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS employment JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Write to parquet
    writeEmploymentStatisticsParquet(seriesData, targetFile);
    LOGGER.info("Converted BLS employment data to parquet: {}", targetFile);
  }
  
  private void convertInflationMetricsToParquet(File sourceDir, File targetFile) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();
    
    // Look for inflation metrics JSON files
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("inflation_metrics.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);
          
          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();
              
              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" + 
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS inflation JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Write to parquet using inflation metrics schema
    writeInflationMetricsParquet(seriesData, targetFile);
    LOGGER.info("Converted BLS inflation data to parquet: {}", targetFile);
  }
}