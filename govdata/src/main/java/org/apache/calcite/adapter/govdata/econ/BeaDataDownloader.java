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
    String industryId;
    int year;
    double value;
    String units;
    String metric;
    String frequency;
  }
  
  /**
   * Converts cached BEA data to Parquet format.
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
}