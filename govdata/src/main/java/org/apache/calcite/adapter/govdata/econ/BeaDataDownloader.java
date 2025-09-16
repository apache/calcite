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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
  private final StorageProvider storageProvider;
  
  // BEA dataset names - comprehensive coverage of all available datasets
  public static class Datasets {
    public static final String NIPA = "NIPA";                               // National Income and Product Accounts
    public static final String NI_UNDERLYING_DETAIL = "NIUnderlyingDetail"; // Standard NI underlying detail tables
    public static final String MNE = "MNE";                                 // Multinational Enterprises  
    public static final String FIXED_ASSETS = "FixedAssets";                // Standard Fixed Assets tables
    public static final String ITA = "ITA";                                 // International Transactions Accounts
    public static final String IIP = "IIP";                                 // International Investment Position
    public static final String INPUT_OUTPUT = "InputOutput";                // Input-Output Data
    public static final String INTL_SERV_TRADE = "IntlServTrade";           // International Services Trade
    public static final String INTL_SERV_STA = "IntlServSTA";              // International Services Supplied Through Affiliates
    public static final String GDP_BY_INDUSTRY = "GDPbyIndustry";           // GDP by Industry
    public static final String REGIONAL = "Regional";                       // Regional data sets
    public static final String UNDERLYING_GDP_BY_INDUSTRY = "UnderlyingGDPbyIndustry"; // Underlying GDP by Industry
    public static final String API_DATASET_METADATA = "APIDatasetMetaData"; // Metadata about other API datasets
  }
  
  // Key NIPA table IDs
  public static class NipaTables {
    public static final String GDP_COMPONENTS = "1";     // GDP and Components
    public static final String PERSONAL_INCOME = "58";   // Personal Income
    public static final String PERSONAL_CONSUMPTION = "66"; // Personal Consumption by Type
    public static final String GOVT_SPENDING = "86";     // Government Current Expenditures
    public static final String INVESTMENT = "51";        // Private Fixed Investment by Type
    public static final String EXPORTS_IMPORTS = "T40205B";  // Exports and Imports by Type
    public static final String CORPORATE_PROFITS = "45"; // Corporate Profits
    public static final String SAVINGS_RATE = "58";      // Personal Saving Rate
  }
  
  // Key ITA (International Transactions Accounts) indicators
  public static class ItaIndicators {
    public static final String BALANCE_GOODS = "BalGds";                    // Balance on goods
    public static final String BALANCE_SERVICES = "BalServ";                // Balance on services  
    public static final String BALANCE_GOODS_SERVICES = "BalGdsServ";       // Balance on goods and services
    public static final String BALANCE_CURRENT_ACCOUNT = "BalCurrAcct";     // Balance on current account
    public static final String BALANCE_CAPITAL_ACCOUNT = "BalCapAcct";      // Balance on capital account
    public static final String BALANCE_PRIMARY_INCOME = "BalPrimInc";       // Balance on primary income
    public static final String BALANCE_SECONDARY_INCOME = "BalSecInc";      // Balance on secondary income
    public static final String EXPORTS_GOODS = "ExpGds";                    // Exports of goods
    public static final String IMPORTS_GOODS = "ImpGds";                    // Imports of goods
    public static final String EXPORTS_SERVICES = "ExpServ";                // Exports of services
    public static final String IMPORTS_SERVICES = "ImpServ";                // Imports of services
  }
  
  // Key GDP by Industry table IDs  
  public static class GdpByIndustryTables {
    public static final String VALUE_ADDED_BY_INDUSTRY = "1";               // Gross Output and Value Added by Industry
    public static final String GDP_BY_INDUSTRY_ANNUAL = "2";                // Value Added by Industry as a Percentage of GDP
    public static final String EMPLOYMENT_BY_INDUSTRY = "3";                // Full-Time and Part-Time Employees by Industry
    public static final String COMPENSATION_BY_INDUSTRY = "4";              // Compensation by Industry
  }
  
  public BeaDataDownloader(String cacheDir, String apiKey, StorageProvider storageProvider) {
    this.cacheDir = cacheDir;
    this.apiKey = apiKey;
    this.storageProvider = storageProvider;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir, String apiKey) {
    this.cacheDir = cacheDir;
    this.apiKey = apiKey;
    this.storageProvider = org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir);
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
    LOGGER.info("Downloading all BEA data for years {} to {}", startYear, endYear);
    
    // Download all datasets year by year to match expected directory structure
    for (int year = startYear; year <= endYear; year++) {
      // Download GDP components
      downloadGdpComponentsForYear(year);
      
      // Download regional income for single year
      try {
        LOGGER.info("About to call downloadRegionalIncomeForYear for year {}", year);
        downloadRegionalIncomeForYear(year);
        LOGGER.info("Successfully completed downloadRegionalIncomeForYear for year {}", year);
      } catch (Exception e) {
        LOGGER.error("Failed to download regional income data for year {}: {}", year, e.getMessage(), e);
      }
      
      // Download trade statistics for single year  
      try {
        LOGGER.info("Downloading trade statistics for year {}", year);
        downloadTradeStatisticsForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download trade statistics for year {}: {}", year, e.getMessage());
      }
      
      // Download ITA data for single year
      try {
        LOGGER.info("Downloading ITA data for year {}", year);
        downloadItaDataForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download ITA data for year {}: {}", year, e.getMessage());
      }
      
      // Download industry GDP for single year
      try {
        LOGGER.info("Downloading industry GDP data for year {}", year);
        downloadIndustryGdpForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download industry GDP data for year {}: {}", year, e.getMessage());
      }
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
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
    storageProvider.createDirectories(outputDirPath);
    
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
    String jsonFilePath = storageProvider.resolvePath(outputDirPath, "gdp_components.json");
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
    storageProvider.writeFile(jsonFilePath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    LOGGER.info("GDP components saved to: {} ({} records)", jsonFilePath, components.size());
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
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, 
        String.format("source=econ/type=gdp_components/year_range=%d_%d", startYear, endYear));
    storageProvider.createDirectories(outputDirPath);
    
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
    String parquetFilePath = storageProvider.resolvePath(outputDirPath, "gdp_components.parquet");
    File parquetFile = new File(parquetFilePath);
    writeGdpComponentsParquet(components, parquetFile);
    
    LOGGER.info("GDP components saved to: {} ({} records)", parquetFilePath, components.size());
    return parquetFile;
  }
  
  /**
   * Downloads regional income data using default date range.
   */
  public File downloadRegionalIncome() throws IOException, InterruptedException {
    return downloadRegionalIncome(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads regional income data for a single year.
   */
  public void downloadRegionalIncomeForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.error("BEA API key is missing - cannot download regional income data");
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA regional income data for year {} with API key: {}...", year, apiKey.substring(0, 4));
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
    storageProvider.createDirectories(outputDirPath);
    
    List<RegionalIncome> incomeData = new ArrayList<>();
    
    // Regional API requires separate calls for each LineCode (1=Income, 2=Population, 3=Per Capita)
    String[] lineCodes = {"1", "2", "3"};
    
    for (String lineCode : lineCodes) {
      LOGGER.debug("Downloading regional income data for year {} LineCode {}", year, lineCode);
      
      String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=SAINC1&LineCode=%s&GeoFips=STATE&Year=%d&ResultFormat=JSON",
          apiKey, Datasets.REGIONAL, lineCode, year);
      
      String url = BEA_API_BASE + "?" + params;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("BEA regional income API request failed for year {} LineCode {} with status: {}", year, lineCode, response.statusCode());
        continue;
      }
      
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");
      
      // Check for API errors
      if (results != null && results.has("Error")) {
        JsonNode error = results.get("Error");
        LOGGER.warn("BEA API error for year {} LineCode {}: {} - {}", year, lineCode, 
                   error.get("APIErrorCode").asText(), error.get("APIErrorDescription").asText());
        continue;
      }
      
      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            try {
              RegionalIncome income = new RegionalIncome();
              
              // Get fields with null checks
              JsonNode geoFipsNode = record.get("GeoFips");
              JsonNode geoNameNode = record.get("GeoName");
              JsonNode codeNode = record.get("Code");
              JsonNode lineCodeNode = record.get("LineCode");  // Try both Code and LineCode
              JsonNode descNode = record.get("Description");
              JsonNode timePeriodNode = record.get("TimePeriod");
              JsonNode dataValueNode = record.get("DataValue");
              
              if (geoFipsNode == null || geoNameNode == null || timePeriodNode == null || dataValueNode == null) {
                LOGGER.debug("Skipping record with missing required fields");
                continue;
              }
              
              income.geoFips = geoFipsNode.asText();
              income.geoName = geoNameNode.asText();
              
              // Handle LineCode - BEA Regional uses LineCode, not Code
              if (lineCodeNode != null) {
                income.lineCode = lineCodeNode.asText();
              } else if (codeNode != null) {
                income.lineCode = codeNode.asText();
              } else {
                income.lineCode = lineCode;  // Use the LineCode from the request
              }
              
              // Description might be in different field or not present
              if (descNode != null) {
                income.lineDescription = descNode.asText();
              } else {
                // Set description based on line code
                if ("1".equals(income.lineCode)) {
                  income.lineDescription = "Personal income (thousands of dollars)";
                } else if ("2".equals(income.lineCode)) {
                  income.lineDescription = "Population (persons)";
                } else if ("3".equals(income.lineCode)) {
                  income.lineDescription = "Per capita personal income (dollars)";
                } else {
                  income.lineDescription = "Line " + income.lineCode;
                }
              }
              
              income.year = Integer.parseInt(timePeriodNode.asText());
              
              String dataValue = dataValueNode.asText();
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
            } catch (Exception e) {
              LOGGER.debug("Failed to parse regional income record: {}", e.getMessage());
            }
          }
        }
      }
    }
    
    // Save as JSON
    String jsonFilePath = storageProvider.resolvePath(outputDirPath, "regional_income.json");
    LOGGER.info("Preparing to save {} regional income records to {}", incomeData.size(), jsonFilePath);
    
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> incomeList = new ArrayList<>();
    
    for (RegionalIncome income : incomeData) {
      Map<String, Object> incomeMap = new HashMap<>();
      incomeMap.put("geo_fips", income.geoFips);
      incomeMap.put("geo_name", income.geoName);
      incomeMap.put("metric", income.metric);
      incomeMap.put("line_code", income.lineCode);
      incomeMap.put("line_description", income.lineDescription);
      incomeMap.put("year", income.year);
      incomeMap.put("value", income.value);
      incomeMap.put("units", income.units);
      incomeList.add(incomeMap);
    }
    
    data.put("regional_income", incomeList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    storageProvider.writeFile(jsonFilePath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    LOGGER.info("Regional income data saved to: {} ({} records)", jsonFilePath, incomeData.size());
  }
  
  /**
   * Downloads regional personal income data by state.
   */
  public File downloadRegionalIncome(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA regional income data for {}-{}", startYear, endYear);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir,
        String.format("source=econ/type=regional_income/year_range=%d_%d", startYear, endYear));
    storageProvider.createDirectories(outputDirPath);
    
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
    String parquetFilePath = storageProvider.resolvePath(outputDirPath, "regional_income.parquet");
    File parquetFile = new File(parquetFilePath);
    writeRegionalIncomeParquet(incomeData, parquetFile);
    
    LOGGER.info("Regional income data saved to: {} ({} records)", parquetFilePath, incomeData.size());
    return parquetFile;
  }
  
  /**
   * Downloads trade statistics using default date range.
   */
  public File downloadTradeStatistics() throws IOException, InterruptedException {
    return downloadTradeStatistics(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads trade statistics for a single year.
   */
  public void downloadTradeStatisticsForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA trade statistics for year {}", year);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
    storageProvider.createDirectories(outputDirPath);
    
    List<TradeStatistic> tradeData = new ArrayList<>();
    
    // Download exports and imports (Table 125) for single year
    String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%d&ResultFormat=JSON",
        apiKey, Datasets.NIPA, NipaTables.EXPORTS_IMPORTS, year);
    
    String url = BEA_API_BASE + "?" + params;
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      LOGGER.warn("BEA trade statistics API request failed for year {} with status: {}", year, response.statusCode());
      return;
    }
    
    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");
    
    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          TradeStatistic trade = new TradeStatistic();
          trade.tableId = NipaTables.EXPORTS_IMPORTS;
          trade.lineNumber = record.get("LineNumber").asInt();
          trade.lineDescription = record.get("LineDescription").asText();
          trade.seriesCode = record.get("SeriesCode").asText();
          trade.year = record.get("TimePeriod").asInt();
          trade.frequency = "A";
          trade.units = "Billions of dollars";
          
          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              trade.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }
          
          // Determine trade type and category from line description
          parseTradeTypeAndCategory(trade);
          
          tradeData.add(trade);
        }
      }
    }
    
    // Calculate trade balances for matching export/import pairs
    calculateTradeBalances(tradeData);
    
    // Save as JSON
    String jsonFilePath = storageProvider.resolvePath(outputDirPath, "trade_statistics.json");
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> tradeList = new ArrayList<>();
    
    for (TradeStatistic trade : tradeData) {
      Map<String, Object> tradeMap = new HashMap<>();
      tradeMap.put("table_id", trade.tableId);
      tradeMap.put("line_number", trade.lineNumber);
      tradeMap.put("line_description", trade.lineDescription);
      tradeMap.put("series_code", trade.seriesCode);
      tradeMap.put("year", trade.year);
      tradeMap.put("value", trade.value);
      tradeMap.put("units", trade.units);
      tradeMap.put("frequency", trade.frequency);
      tradeMap.put("trade_type", trade.tradeType);
      tradeMap.put("category", trade.category);
      tradeMap.put("trade_balance", trade.tradeBalance);
      tradeList.add(tradeMap);
    }
    
    data.put("trade_statistics", tradeList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    storageProvider.writeFile(jsonFilePath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    LOGGER.info("Trade statistics saved to: {} ({} records)", jsonFilePath, tradeData.size());
  }
  
  /**
   * Downloads trade statistics (exports and imports) from BEA Table 125.
   * Provides detailed breakdown of exports and imports by category.
   */
  public File downloadTradeStatistics(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA trade statistics for {}-{}", startYear, endYear);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir,
        String.format("source=econ/type=trade_statistics/year_range=%d_%d", startYear, endYear));
    storageProvider.createDirectories(outputDirPath);
    
    List<TradeStatistic> tradeData = new ArrayList<>();
    
    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);
    
    // Download exports and imports (Table 125)
    String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON",
        apiKey, Datasets.NIPA, NipaTables.EXPORTS_IMPORTS, yearParam);
    
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
          TradeStatistic trade = new TradeStatistic();
          trade.tableId = NipaTables.EXPORTS_IMPORTS;
          trade.lineNumber = record.get("LineNumber").asInt();
          trade.lineDescription = record.get("LineDescription").asText();
          trade.seriesCode = record.get("SeriesCode").asText();
          trade.year = record.get("TimePeriod").asInt();
          trade.frequency = "A";
          trade.units = "Billions of dollars";
          
          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              trade.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }
          
          // Determine trade type and category from line description
          parseTradeTypeAndCategory(trade);
          
          tradeData.add(trade);
        }
      }
    }
    
    // Calculate trade balances for matching export/import pairs
    calculateTradeBalances(tradeData);
    
    // Convert to Parquet
    String parquetFilePath = storageProvider.resolvePath(outputDirPath, "trade_statistics.parquet");
    File parquetFile = new File(parquetFilePath);
    writeTradeStatisticsParquet(tradeData, parquetFile);
    
    LOGGER.info("Trade statistics saved to: {} ({} records)", parquetFilePath, tradeData.size());
    return parquetFile;
  }
  
  /**
   * Parses trade type and category from BEA line description.
   * Maps line descriptions to export/import categories.
   */
  private void parseTradeTypeAndCategory(TradeStatistic trade) {
    String desc = trade.lineDescription.toLowerCase();
    
    // Determine if this is an export or import based on line description
    if (desc.contains("export")) {
      trade.tradeType = "Exports";
    } else if (desc.contains("import")) {
      trade.tradeType = "Imports";
    } else if (trade.lineNumber >= 1 && trade.lineNumber <= 50) {
      // Lines 1-50 are typically exports in Table 125
      trade.tradeType = "Exports";
    } else if (trade.lineNumber >= 51 && trade.lineNumber <= 100) {
      // Lines 51-100 are typically imports in Table 125
      trade.tradeType = "Imports";
    } else {
      trade.tradeType = "Other";
    }
    
    // Parse category from line description
    if (desc.contains("goods")) {
      trade.category = "Goods";
    } else if (desc.contains("services")) {
      trade.category = "Services";
    } else if (desc.contains("food")) {
      trade.category = "Food";
    } else if (desc.contains("industrial supplies") || desc.contains("materials")) {
      trade.category = "Industrial Supplies";
    } else if (desc.contains("capital goods") || desc.contains("machinery")) {
      trade.category = "Capital Goods";
    } else if (desc.contains("automotive") || desc.contains("vehicle")) {
      trade.category = "Automotive";
    } else if (desc.contains("consumer goods")) {
      trade.category = "Consumer Goods";
    } else if (desc.contains("petroleum") || desc.contains("oil")) {
      trade.category = "Petroleum";
    } else if (desc.contains("travel")) {
      trade.category = "Travel Services";
    } else if (desc.contains("transport")) {
      trade.category = "Transportation";
    } else if (desc.contains("financial")) {
      trade.category = "Financial Services";
    } else if (desc.contains("intellectual property") || desc.contains("royalties")) {
      trade.category = "Intellectual Property";
    } else {
      // Use the first few words as category
      String[] words = trade.lineDescription.split("\\s+");
      if (words.length >= 2) {
        trade.category = words[0] + " " + words[1];
      } else {
        trade.category = "Other";
      }
    }
  }
  
  /**
   * Calculates trade balances for matching export/import categories.
   */
  private void calculateTradeBalances(List<TradeStatistic> tradeData) {
    // Group by year and category to calculate balances
    Map<String, Map<String, Double>> exports = new HashMap<>();
    Map<String, Map<String, Double>> imports = new HashMap<>();
    
    // Separate exports and imports
    for (TradeStatistic trade : tradeData) {
      String key = trade.year + "_" + trade.category;
      
      if ("Exports".equals(trade.tradeType)) {
        exports.computeIfAbsent(key, k -> new HashMap<>()).put(trade.category, trade.value);
      } else if ("Imports".equals(trade.tradeType)) {
        imports.computeIfAbsent(key, k -> new HashMap<>()).put(trade.category, trade.value);
      }
    }
    
    // Calculate trade balance for each record
    for (TradeStatistic trade : tradeData) {
      String key = trade.year + "_" + trade.category;
      Double exportValue = exports.getOrDefault(key, new HashMap<>()).get(trade.category);
      Double importValue = imports.getOrDefault(key, new HashMap<>()).get(trade.category);
      
      if (exportValue != null && importValue != null) {
        trade.tradeBalance = exportValue - importValue;
      } else if ("Exports".equals(trade.tradeType) && importValue != null) {
        trade.tradeBalance = trade.value - importValue;
      } else if ("Imports".equals(trade.tradeType) && exportValue != null) {
        trade.tradeBalance = exportValue - trade.value;
      } else {
        trade.tradeBalance = 0.0; // No matching pair found
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void writeTradeStatisticsParquet(List<TradeStatistic> tradeStats, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("TradeStatistic")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("table_id").type().stringType().noDefault()
        .name("line_number").type().intType().noDefault()
        .name("line_description").type().stringType().noDefault()
        .name("series_code").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .name("trade_type").type().stringType().noDefault()
        .name("category").type().stringType().noDefault()
        .name("trade_balance").type().doubleType().noDefault()
        .endRecord();
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (TradeStatistic trade : tradeStats) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("table_id", trade.tableId);
        record.put("line_number", trade.lineNumber);
        record.put("line_description", trade.lineDescription);
        record.put("series_code", trade.seriesCode);
        record.put("year", trade.year);
        record.put("value", trade.value);
        record.put("units", trade.units);
        record.put("frequency", trade.frequency);
        record.put("trade_type", trade.tradeType);
        record.put("category", trade.category);
        record.put("trade_balance", trade.tradeBalance);
        writer.write(record);
      }
    }
    
    LOGGER.info("Trade statistics Parquet written: {} ({} records)", outputFile.getAbsolutePath(), tradeStats.size());
  }
  
  /**
   * Downloads International Transactions Accounts data using default date range.
   */
  public File downloadItaData() throws IOException, InterruptedException {
    return downloadItaData(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads ITA data for a single year.
   */
  public void downloadItaDataForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA ITA data for year {}", year);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
    storageProvider.createDirectories(outputDirPath);
    
    List<ItaData> itaRecords = new ArrayList<>();
    
    // Download key ITA indicators for single year
    String[] indicators = {
        ItaIndicators.BALANCE_GOODS,
        ItaIndicators.BALANCE_SERVICES,
        ItaIndicators.BALANCE_GOODS_SERVICES,
        ItaIndicators.BALANCE_CURRENT_ACCOUNT
    };
    
    for (String indicator : indicators) {
      String params = String.format("UserID=%s&method=GetData&datasetname=%s&Indicator=%s&AreaOrCountry=AllCountries&Frequency=A&Year=%d&ResultFormat=JSON",
          apiKey, Datasets.ITA, indicator, year);
      
      String url = BEA_API_BASE + "?" + params;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("ITA API request failed for indicator {} year {} with status: {}", indicator, year, response.statusCode());
        continue;
      }
      
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");
      
      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            ItaData ita = new ItaData();
            ita.indicator = record.get("Indicator").asText();
            ita.areaOrCountry = record.get("AreaOrCountry").asText();
            ita.frequency = record.get("Frequency").asText();
            ita.year = Integer.parseInt(record.get("Year").asText());
            ita.timeSeriesId = record.get("TimeSeriesId").asText();
            ita.timeSeriesDescription = record.get("TimeSeriesDescription").asText();
            ita.units = "USD Millions";
            
            // Parse value, handling special cases
            String dataValue = record.get("DataValue").asText();
            if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
              try {
                ita.value = Double.parseDouble(dataValue.replace(",", ""));
              } catch (NumberFormatException e) {
                continue; // Skip invalid values
              }
            } else {
              continue;
            }
            
            // Set indicator description
            ita.indicatorDescription = getItaIndicatorDescription(ita.indicator);
            
            itaRecords.add(ita);
          }
        }
      }
    }
    
    // Save as JSON
    String jsonFilePath = storageProvider.resolvePath(outputDirPath, "ita_data.json");
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> itaList = new ArrayList<>();
    
    for (ItaData ita : itaRecords) {
      Map<String, Object> itaMap = new HashMap<>();
      itaMap.put("indicator", ita.indicator);
      itaMap.put("indicator_description", ita.indicatorDescription);
      itaMap.put("area_or_country", ita.areaOrCountry);
      itaMap.put("frequency", ita.frequency);
      itaMap.put("year", ita.year);
      itaMap.put("value", ita.value);
      itaMap.put("units", ita.units);
      itaMap.put("time_series_id", ita.timeSeriesId);
      itaMap.put("time_series_description", ita.timeSeriesDescription);
      itaList.add(itaMap);
    }
    
    data.put("ita_data", itaList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    storageProvider.writeFile(jsonFilePath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    LOGGER.info("ITA data saved to: {} ({} records)", jsonFilePath, itaRecords.size());
  }
  
  /**
   * Downloads comprehensive International Transactions Accounts (ITA) data.
   * Provides detailed trade balances, current account, and capital flows.
   */
  public File downloadItaData(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA ITA data for {}-{}", startYear, endYear);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir,
        String.format("source=econ/type=ita_data/year_range=%d_%d", startYear, endYear));
    storageProvider.createDirectories(outputDirPath);
    
    List<ItaData> itaRecords = new ArrayList<>();
    
    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);
    
    // Download key ITA indicators
    String[] indicators = {
        ItaIndicators.BALANCE_GOODS,
        ItaIndicators.BALANCE_SERVICES,
        ItaIndicators.BALANCE_GOODS_SERVICES,
        ItaIndicators.BALANCE_CURRENT_ACCOUNT,
        ItaIndicators.BALANCE_CAPITAL_ACCOUNT,
        ItaIndicators.BALANCE_PRIMARY_INCOME,
        ItaIndicators.BALANCE_SECONDARY_INCOME
    };
    
    for (String indicator : indicators) {
      String params = String.format("UserID=%s&method=GetData&datasetname=%s&Indicator=%s&AreaOrCountry=AllCountries&Frequency=A&Year=%s&ResultFormat=JSON",
          apiKey, Datasets.ITA, indicator, yearParam);
      
      String url = BEA_API_BASE + "?" + params;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("ITA API request failed for indicator {} with status: {}", indicator, response.statusCode());
        continue;
      }
      
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");
      
      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            ItaData ita = new ItaData();
            ita.indicator = record.get("Indicator").asText();
            ita.areaOrCountry = record.get("AreaOrCountry").asText();
            ita.frequency = record.get("Frequency").asText();
            ita.year = Integer.parseInt(record.get("Year").asText());
            ita.timeSeriesId = record.get("TimeSeriesId").asText();
            ita.timeSeriesDescription = record.get("TimeSeriesDescription").asText();
            ita.units = "USD Millions";
            
            // Parse value, handling special cases
            String dataValue = record.get("DataValue").asText();
            if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
              try {
                ita.value = Double.parseDouble(dataValue.replace(",", ""));
              } catch (NumberFormatException e) {
                continue; // Skip invalid values
              }
            } else {
              continue;
            }
            
            // Set indicator description
            ita.indicatorDescription = getItaIndicatorDescription(ita.indicator);
            
            itaRecords.add(ita);
          }
        }
      }
    }
    
    // Convert to Parquet
    String parquetFilePath = storageProvider.resolvePath(outputDirPath, "ita_data.parquet");
    File parquetFile = new File(parquetFilePath);
    writeItaDataParquet(itaRecords, parquetFile);
    
    LOGGER.info("ITA data saved to: {} ({} records)", parquetFilePath, itaRecords.size());
    return parquetFile;
  }
  
  /**
   * Maps ITA indicator codes to human-readable descriptions.
   */
  private String getItaIndicatorDescription(String indicator) {
    switch (indicator) {
      case "BalGds": return "Balance on goods";
      case "BalServ": return "Balance on services";
      case "BalGdsServ": return "Balance on goods and services";
      case "BalCurrAcct": return "Balance on current account";
      case "BalCapAcct": return "Balance on capital account";
      case "BalPrimInc": return "Balance on primary income";
      case "BalSecInc": return "Balance on secondary income";
      default: return "Unknown indicator";
    }
  }
  
  @SuppressWarnings("deprecation")
  private void writeItaDataParquet(List<ItaData> itaRecords, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("ItaData")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("indicator").type().stringType().noDefault()
        .name("indicator_description").type().stringType().noDefault()
        .name("area_or_country").type().stringType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("time_series_id").type().stringType().noDefault()
        .name("time_series_description").type().stringType().noDefault()
        .endRecord();
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (ItaData ita : itaRecords) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("indicator", ita.indicator);
        record.put("indicator_description", ita.indicatorDescription);
        record.put("area_or_country", ita.areaOrCountry);
        record.put("frequency", ita.frequency);
        record.put("year", ita.year);
        record.put("value", ita.value);
        record.put("units", ita.units);
        record.put("time_series_id", ita.timeSeriesId);
        record.put("time_series_description", ita.timeSeriesDescription);
        writer.write(record);
      }
    }
    
    LOGGER.info("ITA data Parquet written: {} ({} records)", outputFile.getAbsolutePath(), itaRecords.size());
  }
  
  /**
   * Downloads GDP by Industry data using default date range.
   */
  public File downloadIndustryGdp() throws IOException, InterruptedException {
    return downloadIndustryGdp(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads industry GDP data for a single year.
   */
  public void downloadIndustryGdpForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA GDP by Industry data for year {}", year);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
    storageProvider.createDirectories(outputDirPath);
    
    List<IndustryGdpData> industryData = new ArrayList<>();
    
    // Key industries to download (NAICS codes) - limited for single year
    String[] keyIndustries = {
        "31G",     // Manufacturing
        "52",      // Finance and insurance
        "53",      // Real estate and rental and leasing
        "54",      // Professional, scientific, and technical services
        "GSLG",    // Government
    };
    
    // Download annual data for Table 1 (Value Added by Industry)
    for (String industry : keyIndustries) {
      String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=A&Year=%d&Industry=%s&ResultFormat=JSON",
          apiKey, Datasets.GDP_BY_INDUSTRY, year, industry);
      
      String url = BEA_API_BASE + "?" + params;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      try {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
          LOGGER.warn("GDP by Industry API request failed for industry {} year {} with status: {}", industry, year, response.statusCode());
          continue;
        }
        
        JsonNode root = MAPPER.readTree(response.body());
        
        // The GDP by Industry API returns data in a different structure
        JsonNode results = root.get("BEAAPI").get("Results");
        if (results != null && results.isArray() && results.size() > 0) {
          JsonNode dataNode = results.get(0);
          if (dataNode.has("Data")) {
            JsonNode dataArray = dataNode.get("Data");
            if (dataArray != null && dataArray.isArray()) {
              for (JsonNode record : dataArray) {
                IndustryGdpData gdp = new IndustryGdpData();
                gdp.tableId = record.get("TableID").asInt();
                gdp.frequency = record.get("Frequency").asText();
                gdp.year = Integer.parseInt(record.get("Year").asText());
                gdp.quarter = record.get("Quarter").asText();
                gdp.industryCode = record.get("Industry").asText();
                gdp.industryDescription = record.get("IndustrYDescription").asText();
                gdp.units = "Billions of dollars";
                
                // Parse value, handling special cases
                String dataValue = record.get("DataValue").asText();
                if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                  try {
                    gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                  } catch (NumberFormatException e) {
                    continue; // Skip invalid values
                  }
                } else {
                  continue;
                }
                
                if (record.has("NoteRef")) {
                  gdp.noteRef = record.get("NoteRef").asText();
                }
                
                industryData.add(gdp);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error processing industry {} for year {}: {}", industry, year, e.getMessage());
      }
    }
    
    // Save as JSON
    String jsonFilePath = storageProvider.resolvePath(outputDirPath, "industry_gdp.json");
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> gdpList = new ArrayList<>();
    
    for (IndustryGdpData gdp : industryData) {
      Map<String, Object> gdpMap = new HashMap<>();
      gdpMap.put("table_id", gdp.tableId);
      gdpMap.put("frequency", gdp.frequency);
      gdpMap.put("year", gdp.year);
      gdpMap.put("quarter", gdp.quarter);
      gdpMap.put("industry_code", gdp.industryCode);
      gdpMap.put("industry_description", gdp.industryDescription);
      gdpMap.put("value", gdp.value);
      gdpMap.put("units", gdp.units);
      gdpMap.put("note_ref", gdp.noteRef);
      gdpList.add(gdpMap);
    }
    
    data.put("industry_gdp", gdpList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    storageProvider.writeFile(jsonFilePath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    LOGGER.info("Industry GDP data saved to: {} ({} records)", jsonFilePath, industryData.size());
  }
  
  /**
   * Downloads GDP by Industry data showing value added by NAICS industry classification.
   * Provides quarterly and annual data for all industries including manufacturing, 
   * services, finance, technology, and government sectors.
   */
  public File downloadIndustryGdp(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading BEA GDP by Industry data for {}-{}", startYear, endYear);
    
    String outputDirPath = storageProvider.resolvePath(cacheDir,
        String.format("source=econ/type=industry_gdp/year_range=%d_%d", startYear, endYear));
    storageProvider.createDirectories(outputDirPath);
    
    List<IndustryGdpData> industryData = new ArrayList<>();
    
    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);
    
    // Key industries to download (NAICS codes)
    String[] keyIndustries = {
        "11",      // Agriculture, forestry, fishing, and hunting
        "21",      // Mining
        "22",      // Utilities
        "23",      // Construction
        "31G",     // Manufacturing
        "42",      // Wholesale trade
        "44RT",    // Retail trade
        "48TW",    // Transportation and warehousing
        "51",      // Information
        "52",      // Finance and insurance
        "53",      // Real estate and rental and leasing
        "54",      // Professional, scientific, and technical services
        "55",      // Management of companies and enterprises
        "56",      // Administrative and waste management services
        "61",      // Educational services
        "62",      // Health care and social assistance
        "71",      // Arts, entertainment, and recreation
        "72",      // Accommodation and food services
        "81",      // Other services
        "GSLG",    // Government
    };
    
    // Download annual data for Table 1 (Value Added by Industry)
    for (String industry : keyIndustries) {
      String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=A&Year=%s&Industry=%s&ResultFormat=JSON",
          apiKey, Datasets.GDP_BY_INDUSTRY, yearParam, industry);
      
      String url = BEA_API_BASE + "?" + params;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      try {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
          LOGGER.warn("GDP by Industry API request failed for industry {} with status: {}", industry, response.statusCode());
          continue;
        }
        
        JsonNode root = MAPPER.readTree(response.body());
        
        // The GDP by Industry API returns data in a different structure
        JsonNode results = root.get("BEAAPI").get("Results");
        if (results != null && results.isArray() && results.size() > 0) {
          JsonNode dataNode = results.get(0);
          if (dataNode.has("Data")) {
            JsonNode dataArray = dataNode.get("Data");
            if (dataArray != null && dataArray.isArray()) {
              for (JsonNode record : dataArray) {
                IndustryGdpData gdp = new IndustryGdpData();
                gdp.tableId = record.get("TableID").asInt();
                gdp.frequency = record.get("Frequency").asText();
                gdp.year = Integer.parseInt(record.get("Year").asText());
                gdp.quarter = record.get("Quarter").asText();
                gdp.industryCode = record.get("Industry").asText();
                gdp.industryDescription = record.get("IndustrYDescription").asText();
                gdp.units = "Billions of dollars";
                
                // Parse value, handling special cases
                String dataValue = record.get("DataValue").asText();
                if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                  try {
                    gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                  } catch (NumberFormatException e) {
                    continue; // Skip invalid values
                  }
                } else {
                  continue;
                }
                
                if (record.has("NoteRef")) {
                  gdp.noteRef = record.get("NoteRef").asText();
                }
                
                industryData.add(gdp);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error processing industry {}: {}", industry, e.getMessage());
      }
    }
    
    // Also download quarterly data for recent years (last 2 years only for size)
    int quarterlyStartYear = Math.max(startYear, endYear - 1);
    for (int year = quarterlyStartYear; year <= endYear; year++) {
      for (String quarter : new String[]{"Q1", "Q2", "Q3", "Q4"}) {
        // Download quarterly data for manufacturing sector as example
        String params = String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=Q&Year=%d&Quarter=%s&Industry=31G&ResultFormat=JSON",
            apiKey, Datasets.GDP_BY_INDUSTRY, year, quarter);
        
        String url = BEA_API_BASE + "?" + params;
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .build();
        
        try {
          HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
          
          if (response.statusCode() == 200) {
            JsonNode root = MAPPER.readTree(response.body());
            JsonNode results = root.get("BEAAPI").get("Results");
            if (results != null && results.isArray() && results.size() > 0) {
              JsonNode dataNode = results.get(0);
              if (dataNode.has("Data")) {
                JsonNode dataArray = dataNode.get("Data");
                if (dataArray != null && dataArray.isArray()) {
                  for (JsonNode record : dataArray) {
                    IndustryGdpData gdp = new IndustryGdpData();
                    gdp.tableId = record.get("TableID").asInt();
                    gdp.frequency = record.get("Frequency").asText();
                    gdp.year = year;
                    gdp.quarter = quarter;
                    gdp.industryCode = record.get("Industry").asText();
                    gdp.industryDescription = record.get("IndustrYDescription").asText();
                    gdp.units = "Billions of dollars";
                    
                    String dataValue = record.get("DataValue").asText();
                    if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                      try {
                        gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                      } catch (NumberFormatException e) {
                        continue;
                      }
                    } else {
                      continue;
                    }
                    
                    industryData.add(gdp);
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Quarterly data not available for {} {}", year, quarter);
        }
      }
    }
    
    // Convert to Parquet
    String parquetFilePath = storageProvider.resolvePath(outputDirPath, "industry_gdp.parquet");
    File parquetFile = new File(parquetFilePath);
    writeIndustryGdpParquet(industryData, parquetFile);
    
    LOGGER.info("Industry GDP data saved to: {} ({} records)", parquetFilePath, industryData.size());
    return parquetFile;
  }
  
  @SuppressWarnings("deprecation")
  private void writeIndustryGdpParquet(List<IndustryGdpData> industryData, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("IndustryGdpData")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("table_id").type().intType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("quarter").type().stringType().noDefault()
        .name("industry_code").type().stringType().noDefault()
        .name("industry_description").type().stringType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("note_ref").type().nullable().stringType().noDefault()
        .endRecord();
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (IndustryGdpData gdp : industryData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("table_id", gdp.tableId);
        record.put("frequency", gdp.frequency);
        record.put("year", gdp.year);
        record.put("quarter", gdp.quarter);
        record.put("industry_code", gdp.industryCode);
        record.put("industry_description", gdp.industryDescription);
        record.put("value", gdp.value);
        record.put("units", gdp.units);
        record.put("note_ref", gdp.noteRef);
        writer.write(record);
      }
    }
    
    LOGGER.info("Industry GDP Parquet written: {} ({} records)", outputFile.getAbsolutePath(), industryData.size());
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
    // IMPORTANT: Do not include 'year' in the schema - it's a partition key derived from directory structure
    Schema schema = SchemaBuilder.record("RegionalIncome")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("geo_fips").type().stringType().noDefault()
        .name("geo_name").type().stringType().noDefault()
        .name("metric").type().nullable().stringType().noDefault()
        .name("line_code").type().stringType().noDefault()
        .name("line_description").type().stringType().noDefault()
        // year removed - it's a partition key from directory structure
        .name("value").type().doubleType().noDefault()
        .name("units").type().nullable().stringType().noDefault()
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
        // Don't put year - it's derived from the partition directory
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
  
  private static class TradeStatistic {
    String tableId;
    int lineNumber;
    String lineDescription;
    String seriesCode;
    int year;
    double value;
    String units;
    String frequency;
    String tradeType;  // "Exports" or "Imports"
    String category;   // Parsed from lineDescription
    double tradeBalance; // Calculated for matching import/export pairs
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
  
  private static class ItaData {
    String indicator;
    String indicatorDescription;
    String areaOrCountry;
    String frequency;
    int year;
    double value;
    String units;
    String timeSeriesId;
    String timeSeriesDescription;
  }
  
  private static class GdpByIndustryData {
    String tableId;
    String industry;
    String industryDescription;
    int year;
    String quarter;
    double value;
    String units;
    String metric;
    String frequency;
  }
  
  private static class IndustryGdpData {
    int tableId;
    String frequency;
    int year;
    String quarter;
    String industryCode;
    String industryDescription;
    double value;
    String units;
    String noteRef;
  }
  
  /**
   * Converts regional income JSON to Parquet format.
   */
  public void convertRegionalIncomeToParquet(File sourceDir, File targetFile) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    String targetFilePath = targetFile.getAbsolutePath();
    
    LOGGER.info("Converting regional income data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }
    
    // Ensure target directory exists
    String parentDir = targetFile.getParent();
    if (parentDir != null) {
      storageProvider.createDirectories(parentDir);
    }
    
    List<RegionalIncome> incomeData = new ArrayList<>();
    
    // Look for regional income JSON files in the source directory
    List<StorageProvider.FileEntry> files = storageProvider.listFiles(sourceDirPath, false);
    
    for (StorageProvider.FileEntry file : files) {
      if ("regional_income.json".equals(file.getName()) && !file.getName().startsWith(".")) {
        try {
          String content;
          try (InputStream inputStream = storageProvider.openInputStream(file.getPath())) {
            content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
          }
          JsonNode root = MAPPER.readTree(content);
          JsonNode incomeArray = root.get("regional_income");
          
          if (incomeArray != null && incomeArray.isArray()) {
            for (JsonNode inc : incomeArray) {
              try {
                RegionalIncome income = new RegionalIncome();
                income.geoFips = inc.get("geo_fips").asText();
                income.geoName = inc.get("geo_name").asText();
                income.metric = inc.get("metric").asText();
                income.lineCode = inc.get("line_code").asText();
                income.lineDescription = inc.get("line_description").asText();
                income.year = inc.get("year").asInt();
                income.value = inc.get("value").asDouble();
                income.units = inc.get("units").asText();
                
                incomeData.add(income);
              } catch (Exception e) {
                LOGGER.warn("Failed to parse regional income record: {}", e.getMessage());
              }
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to process regional income JSON file {}: {}", file.getPath(), e.getMessage());
        }
      }
    }
    
    if (!incomeData.isEmpty()) {
      // Write parquet file
      writeRegionalIncomeParquet(incomeData, targetFile);
      LOGGER.info("Converted regional income data to parquet: {} ({} records)", targetFilePath, incomeData.size());
    } else {
      LOGGER.warn("No regional income data found in {}", sourceDirPath);
    }
  }
  
  /**
   * Converts cached BEA GDP components data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   * 
   * @param sourceDir Directory containing cached BEA JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertToParquet(File sourceDir, File targetFile) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    String targetFilePath = targetFile.getAbsolutePath();
    
    LOGGER.info("Converting BEA data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }
    
    // Ensure target directory exists
    String parentDir = targetFile.getParent();
    if (parentDir != null) {
      storageProvider.createDirectories(parentDir);
    }
    
    List<Map<String, Object>> components = new ArrayList<>();
    
    // Look for GDP components JSON files in the source directory
    List<StorageProvider.FileEntry> files = storageProvider.listFiles(sourceDirPath, false);
    
    for (StorageProvider.FileEntry file : files) {
      if ("gdp_components.json".equals(file.getName()) && !file.getName().startsWith(".")) {
        try {
          String content;
          try (InputStream inputStream = storageProvider.openInputStream(file.getPath())) {
            content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
          }
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
          LOGGER.warn("Failed to process BEA JSON file {}: {}", file.getPath(), e.getMessage());
        }
      }
    }
    
    // Write parquet file
    writeGdpComponentsMapParquet(components, targetFile);
    
    LOGGER.info("Converted BEA data to parquet: {} ({} components)", targetFilePath, components.size());
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
  
  /**
   * Converts trade statistics JSON files to Parquet format.
   */
  public void convertTradeStatisticsToParquet(File sourceDir, File targetFile) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    String targetFilePath = targetFile.getAbsolutePath();
    
    LOGGER.info("Converting trade statistics data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }
    
    // Read JSON file
    File jsonFile = new File(sourceDir, "trade_statistics.json");
    if (!jsonFile.exists()) {
      LOGGER.warn("Trade statistics JSON file not found: {}", jsonFile.getAbsolutePath());
      return;
    }
    
    String jsonContent;
    try (InputStream inputStream = storageProvider.openInputStream(jsonFile.getAbsolutePath())) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseTradeStatisticsJson(jsonContent);
    
    if (records.isEmpty()) {
      LOGGER.warn("No trade statistics records found in {}", jsonFile.getAbsolutePath());
      return;
    }
    
    writeTradeStatisticsParquet(records, targetFilePath);
  }
  
  private List<Map<String, Object>> parseTradeStatisticsJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);
    
    List<Map<String, Object>> records = new ArrayList<>();
    
    JsonNode results = rootNode.path("BEAAPI").path("Results");
    if (results.isArray() && results.size() > 0) {
      JsonNode data = results.get(0).path("Data");
      
      for (JsonNode item : data) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("TableID").asText());
        record.put("line_number", item.path("LineNumber").asText());
        record.put("line_description", item.path("LineDescription").asText());
        record.put("series_code", item.path("SeriesCode").asText());
        record.put("year", item.path("TimePeriod").asInt());
        record.put("value", item.path("DataValue").asDouble());
        record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
        record.put("frequency", "A");
        record.put("trade_type", parseTradeType(item.path("LineDescription").asText()));
        record.put("category", parseTradeCategory(item.path("LineDescription").asText()));
        record.put("trade_balance", 0.0);
        records.add(record);
      }
    }
    
    return records;
  }
  
  @SuppressWarnings("deprecation")
  private void writeTradeStatisticsParquet(List<Map<String, Object>> records, String outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("TradeStatistics")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("table_id").type().stringType().noDefault()
        .name("line_number").type().stringType().noDefault()
        .name("line_description").type().stringType().noDefault()
        .name("series_code").type().stringType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .name("trade_type").type().stringType().noDefault()
        .name("category").type().stringType().noDefault()
        .name("trade_balance").type().doubleType().noDefault()
        .endRecord();
    
    File file = new File(outputFile);
    file.getParentFile().mkdirs();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (Map<String, Object> record : records) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("table_id", record.get("table_id"));
        avroRecord.put("line_number", record.get("line_number"));
        avroRecord.put("line_description", record.get("line_description"));
        avroRecord.put("series_code", record.get("series_code"));
        avroRecord.put("value", record.get("value"));
        avroRecord.put("units", record.get("units"));
        avroRecord.put("frequency", record.get("frequency"));
        avroRecord.put("trade_type", record.get("trade_type"));
        avroRecord.put("category", record.get("category"));
        avroRecord.put("trade_balance", record.get("trade_balance"));
        writer.write(avroRecord);
      }
    }
  }
  
  /**
   * Converts ITA data JSON files to Parquet format.
   */
  public void convertItaDataToParquet(File sourceDir, File targetFile) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    String targetFilePath = targetFile.getAbsolutePath();
    
    LOGGER.info("Converting ITA data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }
    
    // Read JSON file
    File jsonFile = new File(sourceDir, "ita_data.json");
    if (!jsonFile.exists()) {
      LOGGER.warn("ITA data JSON file not found: {}", jsonFile.getAbsolutePath());
      return;
    }
    
    String jsonContent;
    try (InputStream inputStream = storageProvider.openInputStream(jsonFile.getAbsolutePath())) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseItaDataJson(jsonContent);
    
    if (records.isEmpty()) {
      LOGGER.warn("No ITA data records found in {}", jsonFile.getAbsolutePath());
      return;
    }
    
    writeItaDataParquet(records, targetFilePath);
  }
  
  private List<Map<String, Object>> parseItaDataJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);
    
    List<Map<String, Object>> records = new ArrayList<>();
    
    JsonNode results = rootNode.path("BEAAPI").path("Results");
    if (results.isArray() && results.size() > 0) {
      JsonNode data = results.get(0).path("Data");
      
      for (JsonNode item : data) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("TableID").asText());
        record.put("line_number", item.path("LineNumber").asText());
        record.put("line_description", item.path("LineDescription").asText());
        record.put("series_code", item.path("SeriesCode").asText());
        record.put("value", item.path("DataValue").asDouble());
        record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
        record.put("frequency", "A");
        records.add(record);
      }
    }
    
    return records;
  }
  
  @SuppressWarnings("deprecation")
  private void writeItaDataParquet(List<Map<String, Object>> records, String outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("ItaData")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("table_id").type().stringType().noDefault()
        .name("line_number").type().stringType().noDefault()
        .name("line_description").type().stringType().noDefault()
        .name("series_code").type().stringType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .endRecord();
    
    File file = new File(outputFile);
    file.getParentFile().mkdirs();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (Map<String, Object> record : records) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("table_id", record.get("table_id"));
        avroRecord.put("line_number", record.get("line_number"));
        avroRecord.put("line_description", record.get("line_description"));
        avroRecord.put("series_code", record.get("series_code"));
        avroRecord.put("value", record.get("value"));
        avroRecord.put("units", record.get("units"));
        avroRecord.put("frequency", record.get("frequency"));
        writer.write(avroRecord);
      }
    }
  }
  
  /**
   * Converts industry GDP JSON files to Parquet format.
   */
  public void convertIndustryGdpToParquet(File sourceDir, File targetFile) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    String targetFilePath = targetFile.getAbsolutePath();
    
    LOGGER.info("Converting industry GDP data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }
    
    // Read JSON file
    File jsonFile = new File(sourceDir, "industry_gdp.json");
    if (!jsonFile.exists()) {
      LOGGER.warn("Industry GDP JSON file not found: {}", jsonFile.getAbsolutePath());
      return;
    }
    
    String jsonContent;
    try (InputStream inputStream = storageProvider.openInputStream(jsonFile.getAbsolutePath())) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseIndustryGdpJson(jsonContent);
    
    if (records.isEmpty()) {
      LOGGER.warn("No industry GDP records found in {}", jsonFile.getAbsolutePath());
      return;
    }
    
    writeIndustryGdpParquet(records, targetFilePath);
  }
  
  private List<Map<String, Object>> parseIndustryGdpJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);
    
    List<Map<String, Object>> records = new ArrayList<>();
    
    JsonNode results = rootNode.path("BEAAPI").path("Results");
    if (results.isArray() && results.size() > 0) {
      JsonNode data = results.get(0).path("Data");
      
      for (JsonNode item : data) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("TableID").asText());
        record.put("line_number", item.path("LineNumber").asText());
        record.put("line_description", item.path("LineDescription").asText());
        record.put("series_code", item.path("SeriesCode").asText());
        record.put("value", item.path("DataValue").asDouble());
        record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
        record.put("frequency", "A");
        record.put("industry", parseIndustryFromDescription(item.path("LineDescription").asText()));
        records.add(record);
      }
    }
    
    return records;
  }
  
  @SuppressWarnings("deprecation")
  private void writeIndustryGdpParquet(List<Map<String, Object>> records, String outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("IndustryGdp")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("table_id").type().stringType().noDefault()
        .name("line_number").type().stringType().noDefault()
        .name("line_description").type().stringType().noDefault()
        .name("series_code").type().stringType().noDefault()
        .name("value").type().doubleType().noDefault()
        .name("units").type().stringType().noDefault()
        .name("frequency").type().stringType().noDefault()
        .name("industry").type().stringType().noDefault()
        .endRecord();
    
    File file = new File(outputFile);
    file.getParentFile().mkdirs();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (Map<String, Object> record : records) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("table_id", record.get("table_id"));
        avroRecord.put("line_number", record.get("line_number"));
        avroRecord.put("line_description", record.get("line_description"));
        avroRecord.put("series_code", record.get("series_code"));
        avroRecord.put("value", record.get("value"));
        avroRecord.put("units", record.get("units"));
        avroRecord.put("frequency", record.get("frequency"));
        avroRecord.put("industry", record.get("industry"));
        writer.write(avroRecord);
      }
    }
  }
  
  private String parseTradeType(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("export")) {
      return "Exports";
    } else if (desc.contains("import")) {
      return "Imports";
    } else {
      return "Other";
    }
  }
  
  private String parseTradeCategory(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("goods")) {
      return "Goods";
    } else if (desc.contains("services")) {
      return "Services";
    } else if (desc.contains("food")) {
      return "Food";
    } else {
      return "Other";
    }
  }
  
  private String parseIndustryFromDescription(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("agriculture")) {
      return "Agriculture";
    } else if (desc.contains("mining")) {
      return "Mining";
    } else if (desc.contains("manufacturing")) {
      return "Manufacturing";
    } else if (desc.contains("construction")) {
      return "Construction";
    } else if (desc.contains("finance")) {
      return "Finance";
    } else if (desc.contains("retail")) {
      return "Retail";
    } else if (desc.contains("real estate")) {
      return "Real Estate";
    } else if (desc.contains("information")) {
      return "Information";
    } else {
      return "Other";
    }
  }
}