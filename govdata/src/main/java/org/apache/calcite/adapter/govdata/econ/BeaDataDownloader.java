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
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 * 
 * <p>Requires a BEA API key from https://apps.bea.gov/api/signup/
 */
public class BeaDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);
  private static final String BEA_API_BASE = "https://apps.bea.gov/api/data/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private final String cacheDir;
  private final String apiKey;
  private final HttpClient httpClient;
  
  // BEA dataset names
  public static class Datasets {
    public static final String NIPA = "NIPA";           // National Income and Product Accounts
    public static final String FIXED_ASSETS = "FixedAssets"; // Fixed Assets
    public static final String REGIONAL = "Regional";   // Regional Economic Accounts
    public static final String ITA = "ITA";             // International Transactions
    public static final String IIP = "IIP";             // International Investment Position
    public static final String GDP_BY_INDUSTRY = "GDPbyIndustry"; // GDP by Industry
    public static final String UNDERLYING_DETAIL = "UnderlyingDetail"; // Underlying Detail
  }
  
  // Key NIPA table IDs
  public static class NipaTables {
    public static final String GDP_COMPONENTS = "1";     // GDP and Components
    public static final String PERSONAL_INCOME = "58";   // Personal Income
    public static final String PERSONAL_CONSUMPTION = "66"; // Personal Consumption by Type
    public static final String GOVT_SPENDING = "86";     // Government Current Expenditures
    public static final String INVESTMENT = "51";        // Private Fixed Investment by Type
    public static final String EXPORTS_IMPORTS = "125";  // Exports and Imports by Type
    public static final String CORPORATE_PROFITS = "45"; // Corporate Profits
    public static final String SAVINGS_RATE = "58";      // Personal Saving Rate
  }
  
  public BeaDataDownloader(String cacheDir, String apiKey) {
    this.cacheDir = cacheDir;
    this.apiKey = apiKey;
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
   * Downloads all BEA data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading BEA data for years {} to {}", startYear, endYear);
    
    for (int year = startYear; year <= endYear; year++) {
      downloadGdpComponentsForYear(year);
    }
  }
  
  /**
   * Downloads GDP components for a specific year.
   */
  public void downloadGdpComponentsForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA GDP components for year {}", year);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=indicators", "year=" + year);
    Files.createDirectories(outputDir);
    
    List<GdpComponent> components = new ArrayList<>();
    
    // Download GDP components (Table 1)
    String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%d&ResultFormat=JSON",
        apiKey, Datasets.NIPA, NipaTables.GDP_COMPONENTS, year);
    
    String url = BEA_API_BASE + "?" + params;
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      LOGGER.warn("BEA API request failed for year {} with status: {}", year, response.statusCode());
      return;
    }
    
    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");
    
    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          GdpComponent component = new GdpComponent();
          component.lineNumber = record.get("LineNumber").asInt();
          component.lineDescription = record.get("LineDescription").asText();
          component.seriesCode = record.get("SeriesCode").asText();
          component.year = record.get("TimePeriod").asInt();
          
          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
            try {
              component.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }
          
          component.units = "Billions of dollars";
          component.tableId = NipaTables.GDP_COMPONENTS;
          component.frequency = "A";
          
          components.add(component);
        }
      }
    }
    
    // Save raw JSON data to cache
    File jsonFile = new File(outputDir.toFile(), "gdp_components.json");
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> componentsData = new ArrayList<>();
    
    for (GdpComponent component : components) {
      Map<String, Object> compData = new HashMap<>();
      compData.put("table_id", component.tableId);
      compData.put("line_number", component.lineNumber);
      compData.put("line_description", component.lineDescription);
      compData.put("series_code", component.seriesCode);
      compData.put("year", component.year);
      compData.put("value", component.value);
      compData.put("units", component.units);
      compData.put("frequency", component.frequency);
      componentsData.add(compData);
    }
    
    data.put("components", componentsData);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    Files.writeString(jsonFile.toPath(), jsonContent);
    
    LOGGER.info("GDP components saved to: {} ({} records)", jsonFile, components.size());
  }

  /**
   * Downloads GDP components using default date range.
   */
  public File downloadGdpComponents() throws IOException, InterruptedException {
    return downloadGdpComponents(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads detailed GDP components data.
   */
  public File downloadGdpComponents(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA GDP components for {}-{}", startYear, endYear);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=gdp_components",
        String.format("year_range=%d_%d", startYear, endYear));
    Files.createDirectories(outputDir);
    
    List<GdpComponent> components = new ArrayList<>();
    
    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);
    
    // Download GDP components (Table 1)
    String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON",
        apiKey, Datasets.NIPA, NipaTables.GDP_COMPONENTS, yearParam);
    
    String url = BEA_API_BASE + "?" + params;
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      throw new IOException("BEA API request failed with status: " + response.statusCode());
    }
    
    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");
    
    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          GdpComponent component = new GdpComponent();
          component.lineNumber = record.get("LineNumber").asInt();
          component.lineDescription = record.get("LineDescription").asText();
          component.seriesCode = record.get("SeriesCode").asText();
          component.year = record.get("TimePeriod").asInt();
          
          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
            try {
              component.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }
          
          component.units = "Billions of dollars";
          component.tableId = NipaTables.GDP_COMPONENTS;
          component.frequency = "A";
          
          components.add(component);
        }
      }
    }
    
    // Also download personal consumption details
    Thread.sleep(100); // Rate limiting
    
    params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON",
        apiKey, Datasets.NIPA, NipaTables.PERSONAL_CONSUMPTION, yearParam);
    
    url = BEA_API_BASE + "?" + params;
    request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() == 200) {
      root = MAPPER.readTree(response.body());
      results = root.get("BEAAPI").get("Results");
      
      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            GdpComponent component = new GdpComponent();
            component.lineNumber = record.get("LineNumber").asInt();
            component.lineDescription = record.get("LineDescription").asText();
            component.seriesCode = record.get("SeriesCode").asText();
            component.year = record.get("TimePeriod").asInt();
            
            String dataValue = record.get("DataValue").asText();
            if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
              try {
                component.value = Double.parseDouble(dataValue.replace(",", ""));
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }
            
            component.units = "Billions of dollars";
            component.tableId = NipaTables.PERSONAL_CONSUMPTION;
            component.frequency = "A";
            
            components.add(component);
          }
        }
      }
    }
    
    // Convert to Parquet
    File parquetFile = new File(outputDir.toFile(), "gdp_components.parquet");
    writeGdpComponentsParquet(components, parquetFile);
    
    LOGGER.info("GDP components saved to: {} ({} records)", parquetFile, components.size());
    return parquetFile;
  }
  
  /**
   * Downloads regional income data using default date range.
   */
  public File downloadRegionalIncome() throws IOException, InterruptedException {
    return downloadRegionalIncome(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads regional personal income data by state.
   */
  public File downloadRegionalIncome(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA regional income data for {}-{}", startYear, endYear);
    
    Path outputDir = Paths.get(cacheDir, "source=econ", "type=regional_income",
        String.format("year_range=%d_%d", startYear, endYear));
    Files.createDirectories(outputDir);
    
    List<RegionalIncome> incomeData = new ArrayList<>();
    
    // Build year list
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);
    
    // Download state personal income data
    // LineCode 1 = Total Personal Income, 2 = Population, 3 = Per Capita Income
    String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=SAINC1&LineCode=1,2,3&GeoFips=STATE&Year=%s&ResultFormat=JSON",
        apiKey, Datasets.REGIONAL, yearParam);
    
    String url = BEA_API_BASE + "?" + params;
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      throw new IOException("BEA API request failed with status: " + response.statusCode());
    }
    
    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");
    
    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          RegionalIncome income = new RegionalIncome();
          income.geoFips = record.get("GeoFips").asText();
          income.geoName = record.get("GeoName").asText();
          income.lineCode = record.get("Code").asText();
          income.lineDescription = record.get("Description").asText();
          income.year = Integer.parseInt(record.get("TimePeriod").asText());
          
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              income.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue;
            }
          } else {
            continue;
          }
          
          // Set units based on line code
          if ("1".equals(income.lineCode)) {
            income.units = "Thousands of dollars";
            income.metric = "Total Personal Income";
          } else if ("2".equals(income.lineCode)) {
            income.units = "Persons";
            income.metric = "Population";
          } else if ("3".equals(income.lineCode)) {
            income.units = "Dollars";
            income.metric = "Per Capita Personal Income";
          }
          
          incomeData.add(income);
        }
      }
    }
    
    // Convert to Parquet
    File parquetFile = new File(outputDir.toFile(), "regional_income.parquet");
    writeRegionalIncomeParquet(incomeData, parquetFile);
    
    LOGGER.info("Regional income data saved to: {} ({} records)", parquetFile, incomeData.size());
    return parquetFile;
  }
  
  @SuppressWarnings("deprecation")
  private void writeGdpComponentsParquet(List<GdpComponent> components, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("GdpComponent")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("table_id")
        .requiredInt("line_number")
        .requiredString("line_description")
        .requiredString("series_code")
        .requiredInt("year")
        .requiredDouble("value")
        .requiredString("units")
        .requiredString("frequency")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (GdpComponent component : components) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("table_id", component.tableId);
        record.put("line_number", component.lineNumber);
        record.put("line_description", component.lineDescription);
        record.put("series_code", component.seriesCode);
        record.put("year", component.year);
        record.put("value", component.value);
        record.put("units", component.units);
        record.put("frequency", component.frequency);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void writeRegionalIncomeParquet(List<RegionalIncome> incomeData, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("RegionalIncome")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("geo_fips")
        .requiredString("geo_name")
        .requiredString("metric")
        .requiredString("line_code")
        .requiredString("line_description")
        .requiredInt("year")
        .requiredDouble("value")
        .requiredString("units")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (RegionalIncome income : incomeData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("geo_fips", income.geoFips);
        record.put("geo_name", income.geoName);
        record.put("metric", income.metric);
        record.put("line_code", income.lineCode);
        record.put("line_description", income.lineDescription);
        record.put("year", income.year);
        record.put("value", income.value);
        record.put("units", income.units);
        writer.write(record);
      }
    }
  }
  
  // Data classes
  private static class GdpComponent {
    String tableId;
    int lineNumber;
    String lineDescription;
    String seriesCode;
    int year;
    double value;
    String units;
    String frequency;
  }
  
  private static class RegionalIncome {
    String geoFips;
    String geoName;
    String metric;
    String lineCode;
    String lineDescription;
    int year;
    double value;
    String units;
  }
  
  /**
   * Converts cached BEA data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   * 
   * @param sourceDir Directory containing cached BEA JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertToParquet(File sourceDir, File targetFile) throws IOException {
    LOGGER.info("Converting BEA data from {} to parquet: {}", sourceDir, targetFile);
    
    // Skip if target file already exists
    if (targetFile.exists()) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFile);
      return;
    }
    
    // Ensure target directory exists
    targetFile.getParentFile().mkdirs();
    
    List<Map<String, Object>> components = new ArrayList<>();
    
    // Look for GDP components JSON files in the source directory
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> 
        name.equals("gdp_components.json") && !name.startsWith("."));
    
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath());
          JsonNode root = MAPPER.readTree(content);
          JsonNode componentsArray = root.get("components");
          
          if (componentsArray != null && componentsArray.isArray()) {
            for (JsonNode comp : componentsArray) {
              Map<String, Object> component = new HashMap<>();
              component.put("table_id", comp.get("table_id").asText());
              component.put("line_number", comp.get("line_number").asInt());
              component.put("line_description", comp.get("line_description").asText());
              component.put("series_code", comp.get("series_code").asText());
              component.put("year", comp.get("year").asInt());
              component.put("value", comp.get("value").asDouble());
              component.put("units", comp.get("units").asText());
              component.put("frequency", comp.get("frequency").asText());
              
              components.add(component);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to process BEA JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Write parquet file
    writeGdpComponentsMapParquet(components, targetFile);
    
    LOGGER.info("Converted BEA data to parquet: {} ({} components)", targetFile, components.size());
  }
  
  @SuppressWarnings("deprecation")
  private void writeGdpComponentsMapParquet(List<Map<String, Object>> components, File outputFile) 
      throws IOException {
    Schema schema = SchemaBuilder.record("GdpComponent")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("table_id")
        .requiredInt("line_number")
        .requiredString("line_description")
        .requiredString("series_code")
        .requiredInt("year")
        .requiredDouble("value")
        .requiredString("units")
        .requiredString("frequency")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (Map<String, Object> comp : components) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("table_id", comp.get("table_id"));
        record.put("line_number", comp.get("line_number"));
        record.put("line_description", comp.get("line_description"));
        record.put("series_code", comp.get("series_code"));
        record.put("year", comp.get("year"));
        record.put("value", comp.get("value"));
        record.put("units", comp.get("units"));
        record.put("frequency", comp.get("frequency"));
        writer.write(record);
      }
    }
  }
}