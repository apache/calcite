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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts World Bank economic data to Parquet format.
 * Provides international economic indicators for comparison with U.S. data.
 * 
 * <p>Uses the World Bank API which requires no authentication.
 */
public class WorldBankDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorldBankDataDownloader.class);
  private static final String WORLD_BANK_API_BASE = "https://api.worldbank.org/v2/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private final String cacheDir;
  private final HttpClient httpClient;
  
  // Key economic indicators to download
  public static class Indicators {
    public static final String GDP_CURRENT_USD = "NY.GDP.MKTP.CD";  // GDP (current US$)
    public static final String GDP_GROWTH = "NY.GDP.MKTP.KD.ZG";    // GDP growth (annual %)
    public static final String GDP_PER_CAPITA = "NY.GDP.PCAP.CD";   // GDP per capita (current US$)
    public static final String INFLATION_CPI = "FP.CPI.TOTL.ZG";    // Inflation, consumer prices (annual %)
    public static final String UNEMPLOYMENT = "SL.UEM.TOTL.ZS";      // Unemployment, total (% of labor force)
    public static final String POPULATION = "SP.POP.TOTL";           // Population, total
    public static final String TRADE_BALANCE = "NE.RSB.GNFS.ZS";    // External balance on goods and services (% of GDP)
    public static final String GOVT_DEBT = "GC.DOD.TOTL.GD.ZS";      // Central government debt, total (% of GDP)
    public static final String INTEREST_RATE = "FR.INR.RINR";        // Real interest rate (%)
    public static final String EXCHANGE_RATE = "PA.NUS.FCRF";        // Official exchange rate (LCU per US$)
  }
  
  // Focus on major economies for comparison
  public static class Countries {
    public static final List<String> G7 = Arrays.asList(
        "USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN"
    );
    
    public static final List<String> G20 = Arrays.asList(
        "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "BRA", "CAN",
        "KOR", "RUS", "AUS", "MEX", "IDN", "TUR", "SAU", "ARG", "ZAF"
    );
    
    public static final List<String> MAJOR_ECONOMIES = Arrays.asList(
        "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "BRA", "ITA", "CAN",
        "KOR", "ESP", "AUS", "RUS", "MEX", "IDN", "NLD", "TUR", "CHE", "POL"
    );
  }
  
  public WorldBankDataDownloader(String cacheDir) {
    this.cacheDir = cacheDir;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  /**
   * Gets the default start year from environment variables.
   */
  public static int getDefaultStartYear() {
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return Integer.parseInt(econStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }
    
    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return Integer.parseInt(govdataStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }
    
    return LocalDate.now().getYear() - 5;
  }
  
  /**
   * Gets the default end year from environment variables.
   */
  public static int getDefaultEndYear() {
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return Integer.parseInt(econEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }
    
    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return Integer.parseInt(govdataEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }
    
    return LocalDate.now().getYear();
  }
  
  /**
   * Downloads all World Bank data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading World Bank data for years {} to {}", startYear, endYear);
    
    for (int year = startYear; year <= endYear; year++) {
      downloadWorldIndicatorsForYear(year);
    }
  }
  
  /**
   * Downloads world economic indicators for a specific year.
   */
  public void downloadWorldIndicatorsForYear(int year) throws IOException, InterruptedException {
    LOGGER.info("Downloading world economic indicators for year {}", year);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=indicators", "year=" + year);
    Files.createDirectories(outputDir);
    
    List<Map<String, Object>> indicators = new ArrayList<>();
    
    // Download key indicators for major economies
    List<String> indicatorCodes = Arrays.asList(
        Indicators.GDP_CURRENT_USD,
        Indicators.GDP_GROWTH,
        Indicators.GDP_PER_CAPITA,
        Indicators.INFLATION_CPI,
        Indicators.UNEMPLOYMENT,
        Indicators.POPULATION,
        Indicators.GOVT_DEBT
    );
    
    // Use G20 countries for broader coverage
    String countriesParam = String.join(";", Countries.G20);
    
    for (String indicatorCode : indicatorCodes) {
      LOGGER.info("Fetching indicator: {} for year {}", indicatorCode, year);
      
      String url = String.format("%scountry/%s/indicator/%s?format=json&date=%d&per_page=1000",
          WORLD_BANK_API_BASE, countriesParam, indicatorCode, year);
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("World Bank API request failed for indicator {} year {} with status: {}", 
            indicatorCode, year, response.statusCode());
        continue;
      }
      
      // World Bank API returns array with metadata in first element, data in second
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue; // Skip null values
            }
            
            Map<String, Object> indicator = new HashMap<>();
            JsonNode countryNode = record.get("country");
            if (countryNode != null) {
              indicator.put("country_code", countryNode.get("id") != null ? countryNode.get("id").asText() : "");
              indicator.put("country_name", countryNode.get("value") != null ? countryNode.get("value").asText() : "");
            } else {
              indicator.put("country_code", "");
              indicator.put("country_name", "");
            }
            JsonNode indicatorNode = record.get("indicator");
            if (indicatorNode != null) {
              indicator.put("indicator_code", indicatorNode.get("id") != null ? indicatorNode.get("id").asText() : "");
              indicator.put("indicator_name", indicatorNode.get("value") != null ? indicatorNode.get("value").asText() : "");
            } else {
              indicator.put("indicator_code", "");
              indicator.put("indicator_name", "");
            }
            
            indicator.put("year", record.get("date") != null ? record.get("date").asInt() : 0);
            indicator.put("value", record.get("value") != null ? record.get("value").asDouble() : 0.0);
            indicator.put("unit", record.get("unit") != null ? record.get("unit").asText("") : "");
            indicator.put("scale", record.get("scale") != null ? record.get("scale").asText("") : "");
            
            indicators.add(indicator);
          }
        }
      }
      
      // Small delay to be respectful to the API
      Thread.sleep(100);
    }
    
    // Save raw JSON data to cache
    File jsonFile = new File(outputDir.toFile(), "world_indicators.json");
    Map<String, Object> data = new HashMap<>();
    data.put("indicators", indicators);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    Files.writeString(jsonFile.toPath(), jsonContent);
    
    LOGGER.info("World indicators saved to: {} ({} records)", jsonFile, indicators.size());
  }

  /**
   * Downloads world economic indicators using default date range.
   */
  public File downloadWorldIndicators() throws IOException, InterruptedException {
    return downloadWorldIndicators(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads world economic indicators for major economies.
   */
  public File downloadWorldIndicators(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading world economic indicators for {}-{}", startYear, endYear);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=world_indicators",
        String.format("year_range=%d_%d", startYear, endYear));
    Files.createDirectories(outputDir);
    
    List<WorldIndicator> indicators = new ArrayList<>();
    
    // Download key indicators for major economies
    List<String> indicatorCodes = Arrays.asList(
        Indicators.GDP_CURRENT_USD,
        Indicators.GDP_GROWTH,
        Indicators.GDP_PER_CAPITA,
        Indicators.INFLATION_CPI,
        Indicators.UNEMPLOYMENT,
        Indicators.POPULATION,
        Indicators.GOVT_DEBT
    );
    
    // Use G20 countries for broader coverage
    String countriesParam = String.join(";", Countries.G20);
    
    for (String indicatorCode : indicatorCodes) {
      LOGGER.info("Fetching indicator: {}", indicatorCode);
      
      String url = String.format("%scountry/%s/indicator/%s?format=json&date=%d:%d&per_page=10000",
          WORLD_BANK_API_BASE, countriesParam, indicatorCode, startYear, endYear);
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("World Bank API request failed for indicator {} with status: {}", 
            indicatorCode, response.statusCode());
        continue;
      }
      
      // World Bank API returns array with metadata in first element, data in second
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue; // Skip null values
            }
            
            WorldIndicator indicator = new WorldIndicator();
            indicator.countryCode = record.get("country").get("id").asText();
            indicator.countryName = record.get("country").get("value").asText();
            indicator.indicatorCode = record.get("indicator").get("id").asText();
            indicator.indicatorName = record.get("indicator").get("value").asText();
            indicator.year = record.get("date").asInt();
            indicator.value = record.get("value").asDouble();
            indicator.unit = record.get("unit").asText("");
            indicator.scale = record.get("scale").asText("");
            
            indicators.add(indicator);
          }
        }
      }
      
      // Small delay to be respectful to the API
      Thread.sleep(100);
    }
    
    // Convert to Parquet
    File parquetFile = new File(outputDir.toFile(), "world_indicators.parquet");
    writeWorldIndicatorsParquet(indicators, parquetFile);
    
    LOGGER.info("World indicators saved to: {} ({} records)", parquetFile, indicators.size());
    return parquetFile;
  }
  
  /**
   * Downloads comparative GDP data for all countries.
   */
  public File downloadGlobalGDP() throws IOException, InterruptedException {
    return downloadGlobalGDP(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads GDP data for all countries for global comparison.
   */
  public File downloadGlobalGDP(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading global GDP data for {}-{}", startYear, endYear);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=global_gdp",
        String.format("year_range=%d_%d", startYear, endYear));
    Files.createDirectories(outputDir);
    
    List<WorldIndicator> gdpData = new ArrayList<>();
    
    // Download GDP data for all countries
    String url = String.format("%scountry/all/indicator/%s?format=json&date=%d:%d&per_page=20000",
        WORLD_BANK_API_BASE, Indicators.GDP_CURRENT_USD, startYear, endYear);
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() == 200) {
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue;
            }
            
            WorldIndicator gdp = new WorldIndicator();
            gdp.countryCode = record.get("country").get("id").asText();
            gdp.countryName = record.get("country").get("value").asText();
            gdp.indicatorCode = Indicators.GDP_CURRENT_USD;
            gdp.indicatorName = "GDP (current US$)";
            gdp.year = record.get("date").asInt();
            gdp.value = record.get("value").asDouble();
            gdp.unit = "USD";
            gdp.scale = "1";
            
            gdpData.add(gdp);
          }
        }
      }
    }
    
    // Convert to Parquet
    File parquetFile = new File(outputDir.toFile(), "global_gdp.parquet");
    writeWorldIndicatorsParquet(gdpData, parquetFile);
    
    LOGGER.info("Global GDP data saved to: {} ({} records)", parquetFile, gdpData.size());
    return parquetFile;
  }
  
  @SuppressWarnings("deprecation")
  private void writeWorldIndicatorsParquet(List<WorldIndicator> indicators, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("WorldIndicator")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("country_code")
        .requiredString("country_name")
        .requiredString("indicator_code")
        .requiredString("indicator_name")
        .requiredInt("year")
        .requiredDouble("value")
        .optionalString("unit")
        .optionalString("scale")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (WorldIndicator indicator : indicators) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("country_code", indicator.countryCode);
        record.put("country_name", indicator.countryName);
        record.put("indicator_code", indicator.indicatorCode);
        record.put("indicator_name", indicator.indicatorName);
        record.put("year", indicator.year);
        record.put("value", indicator.value);
        record.put("unit", indicator.unit);
        record.put("scale", indicator.scale);
        writer.write(record);
      }
    }
  }
  
  // Data class
  private static class WorldIndicator {
    String countryCode;
    String countryName;
    String indicatorCode;
    String indicatorName;
    int year;
    double value;
    String unit;
    String scale;
  }
  
  /**
   * Converts cached World Bank data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   * 
   * @param sourceDir Directory containing cached World Bank JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertToParquet(File sourceDir, File targetFile) throws IOException {
    LOGGER.info("Converting World Bank data from {} to parquet: {}", sourceDir, targetFile);
    
    // Skip if target file already exists
    if (targetFile.exists()) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFile);
      return;
    }
    
    // Ensure target directory exists
    targetFile.getParentFile().mkdirs();
    
    List<Map<String, Object>> indicators = new ArrayList<>();
    
    // Look for World Bank indicators JSON files in the source directory
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> 
        name.equals("world_indicators.json") && !name.startsWith("."));
    
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath());
          JsonNode root = MAPPER.readTree(content);
          JsonNode indicatorsArray = root.get("indicators");
          
          if (indicatorsArray != null && indicatorsArray.isArray()) {
            for (JsonNode ind : indicatorsArray) {
              Map<String, Object> indicator = new HashMap<>();
              indicator.put("country_code", ind.get("country_code").asText());
              indicator.put("country_name", ind.get("country_name").asText());
              indicator.put("indicator_code", ind.get("indicator_code").asText());
              indicator.put("indicator_name", ind.get("indicator_name").asText());
              indicator.put("year", ind.get("year").asInt());
              indicator.put("value", ind.get("value").asDouble());
              indicator.put("unit", ind.get("unit").asText(""));
              indicator.put("scale", ind.get("scale").asText(""));
              
              indicators.add(indicator);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to process World Bank JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Write parquet file
    writeWorldIndicatorsMapParquet(indicators, targetFile);
    
    LOGGER.info("Converted World Bank data to parquet: {} ({} indicators)", targetFile, indicators.size());
  }
  
  @SuppressWarnings("deprecation")
  private void writeWorldIndicatorsMapParquet(List<Map<String, Object>> indicators, File outputFile) 
      throws IOException {
    Schema schema = SchemaBuilder.record("WorldIndicator")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("country_code")
        .requiredString("country_name")
        .requiredString("indicator_code")
        .requiredString("indicator_name")
        .requiredInt("year")
        .requiredDouble("value")
        .optionalString("unit")
        .optionalString("scale")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (Map<String, Object> ind : indicators) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("country_code", ind.get("country_code"));
        record.put("country_name", ind.get("country_name"));
        record.put("indicator_code", ind.get("indicator_code"));
        record.put("indicator_name", ind.get("indicator_name"));
        record.put("year", ind.get("year"));
        record.put("value", ind.get("value"));
        record.put("unit", ind.get("unit"));
        record.put("scale", ind.get("scale"));
        writer.write(record);
      }
    }
  }
}