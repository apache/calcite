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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Downloads and converts U.S. Treasury data to Parquet format.
 * Supports daily treasury yields and federal debt statistics.
 * 
 * <p>Uses the Treasury Fiscal Data API which requires no authentication.
 */
public class TreasuryDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TreasuryDataDownloader.class);
  private static final String TREASURY_API_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
  
  private final String cacheDir;
  private final HttpClient httpClient;
  
  public TreasuryDataDownloader(String cacheDir) {
    this.cacheDir = cacheDir;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  /**
   * Downloads all Treasury data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading Treasury data for years {} to {}", startYear, endYear);
    
    // Download treasury yields data
    downloadTreasuryYields(startYear, endYear);
    
    // Download federal debt data
    downloadFederalDebt(startYear, endYear);
  }
  
  /**
   * Gets the default start year from environment variables.
   */
  public static int getDefaultStartYear() {
    // Check for ECON-specific override
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
   */
  public static int getDefaultEndYear() {
    // Check for ECON-specific override
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
   * Downloads treasury yields using default date range from environment.
   */
  public File downloadTreasuryYields() throws IOException, InterruptedException {
    return downloadTreasuryYields(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads daily treasury yield curve data.
   */
  public File downloadTreasuryYields(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading treasury yields for {}-{}", startYear, endYear);
    
    // Download for each year separately to match FileSchema partitioning expectations
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      Path outputDir = Paths.get(cacheDir, "source=econ", "type=timeseries", "year=" + year);
      Files.createDirectories(outputDir);
      
      // Fetch data from Treasury API for this year
      String startDate = year + "-01-01";
      String endDate = year + "-12-31";
      
      String url = TREASURY_API_BASE + "v2/accounting/od/avg_interest_rates"
          + "?filter=record_date:gte:" + startDate
          + ",record_date:lte:" + endDate
          + "&sort=-record_date&page[size]=10000";
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }
      
      // Save raw JSON data to cache directory
      File jsonFile = new File(outputDir.toFile(), "treasury_yields.json");
      Files.writeString(jsonFile.toPath(), response.body(), StandardCharsets.UTF_8);
      
      LOGGER.info("Treasury yields raw data saved for year {}: {}", year, jsonFile);
      lastFile = jsonFile;
    }
    
    return lastFile;
  }
  
  /**
   * Downloads federal debt using default date range from environment.
   */
  public File downloadFederalDebt() throws IOException, InterruptedException {
    return downloadFederalDebt(getDefaultStartYear(), getDefaultEndYear());
  }
  
  /**
   * Downloads federal debt statistics.
   */
  public File downloadFederalDebt(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading federal debt data for {}-{}", startYear, endYear);
    
    // Download for each year separately to match FileSchema partitioning expectations
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      Path outputDir = Paths.get(cacheDir, "source=econ", "type=timeseries", "year=" + year);
      Files.createDirectories(outputDir);
      
      // Fetch debt to the penny data for this year
      String startDate = year + "-01-01";
      String endDate = year + "-12-31";
      
      String url = TREASURY_API_BASE + "v2/accounting/od/debt_to_penny"
          + "?filter=record_date:gte:" + startDate
          + ",record_date:lte:" + endDate
          + "&page[size]=10000";
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }
      
      // Save raw JSON data to cache directory
      File jsonFile = new File(outputDir.toFile(), "federal_debt.json");
      Files.writeString(jsonFile.toPath(), response.body(), StandardCharsets.UTF_8);
      
      LOGGER.info("Federal debt raw data saved for year {}: {}", year, jsonFile);
      lastFile = jsonFile;
    }
    
    return lastFile;
  }
  
  private int parseMaturityMonths(String description) {
    // Parse maturity from Treasury API security descriptions
    // Handle explicit maturity descriptions first
    if (description.contains("30-Year")) return 360;
    if (description.contains("20-Year")) return 240;
    if (description.contains("10-Year")) return 120;
    if (description.contains("7-Year")) return 84;
    if (description.contains("5-Year")) return 60;
    if (description.contains("3-Year")) return 36;
    if (description.contains("2-Year")) return 24;
    if (description.contains("1-Year")) return 12;
    if (description.contains("6-Month")) return 6;
    if (description.contains("3-Month")) return 3;
    if (description.contains("1-Month")) return 1;
    if (description.contains("4-Week")) return 1;
    
    // Handle Treasury API security types (typical maturity ranges)
    if (description.contains("Treasury Bills")) return 3; // Bills are typically 3-month average
    if (description.contains("Treasury Notes")) return 60; // Notes are typically 2-10 years, use 5-year average
    if (description.contains("Treasury Bonds")) return 360; // Bonds are typically 20-30 years, use 30-year average
    if (description.contains("Treasury Floating Rate Notes") || description.contains("FRN")) return 24; // FRNs are typically 2-year
    if (description.contains("Treasury Inflation-Protected Securities") || description.contains("TIPS")) return 120; // TIPS vary, use 10-year average
    if (description.contains("Federal Financing Bank")) return 120; // FFB varies, use 10-year average
    if (description.contains("Total Marketable")) return 60; // Average of all marketable, use 5-year
    if (description.contains("Total Non-marketable")) return 60; // Average of all non-marketable, use 5-year
    if (description.contains("Total Interest-bearing Debt")) return 60; // Average of all debt, use 5-year
    
    return 0; // Unknown
  }
  
  private String formatMaturityLabel(int months) {
    if (months >= 12) {
      int years = months / 12;
      return years + "Y";
    } else {
      return months + "M";
    }
  }
  
  @SuppressWarnings("deprecation")
  private void writeTreasuryYieldsParquet(List<TreasuryYield> yields, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("TreasuryYield")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredInt("maturity_months")
        .requiredString("maturity_label")
        .requiredDouble("yield_percent")
        .requiredString("yield_type")
        .requiredString("source")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (TreasuryYield yield : yields) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", yield.date);
        record.put("maturity_months", yield.maturityMonths);
        record.put("maturity_label", yield.maturityLabel);
        record.put("yield_percent", yield.avgInterestRate);
        record.put("yield_type", yield.securityType);
        record.put("source", "Treasury Direct");
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void writeFederalDebtParquet(List<FederalDebt> debtRecords, File outputFile) throws IOException {
    Schema schema = SchemaBuilder.record("FederalDebt")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("date")
        .requiredString("debt_type")
        .requiredDouble("amount_billions")
        .optionalDouble("percent_of_gdp")
        .requiredString("holder_category")
        .requiredDouble("debt_held_by_public")
        .requiredDouble("intragovernmental_holdings")
        .endRecord();
    
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (FederalDebt debt : debtRecords) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", debt.date);
        record.put("debt_type", debt.debtType);
        record.put("amount_billions", debt.totalDebt);
        record.put("percent_of_gdp", null); // Would need GDP data to calculate
        record.put("holder_category", debt.holderCategory);
        record.put("debt_held_by_public", debt.debtHeldByPublic);
        record.put("intragovernmental_holdings", debt.intragovDebt);
        writer.write(record);
      }
    }
  }
  
  // Data classes
  private static class TreasuryYield {
    String date;
    String securityType;
    String securityDesc;
    int maturityMonths;
    String maturityLabel;
    double avgInterestRate;
  }
  
  private static class FederalDebt {
    String date;
    String debtType;
    double totalDebt;
    double debtHeldByPublic;
    double intragovDebt;
    String holderCategory;
  }
  
  /**
   * Converts cached Treasury yields data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   * 
   * @param sourceDir Directory containing cached Treasury JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertToParquet(File sourceDir, File targetFile) throws IOException {
    LOGGER.info("Converting Treasury data from {} to parquet: {}", sourceDir, targetFile);
    
    // Skip if target file already exists
    if (targetFile.exists()) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFile);
      return;
    }
    
    // Ensure target directory exists
    targetFile.getParentFile().mkdirs();
    
    List<TreasuryYield> yields = new ArrayList<>();
    
    // Look for JSON files in the source directory
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.endsWith(".json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);
          JsonNode data = root.get("data");
          
          if (data != null && data.isArray()) {
            for (JsonNode record : data) {
              TreasuryYield yield = new TreasuryYield();
              yield.date = record.get("record_date").asText();
              yield.securityType = record.get("security_type_desc").asText("");
              yield.securityDesc = record.get("security_desc").asText("");
              yield.avgInterestRate = record.get("avg_interest_rate_amt").asDouble(0.0);
              
              // Parse maturity from description
              yield.maturityMonths = parseMaturityMonths(yield.securityDesc);
              yield.maturityLabel = formatMaturityLabel(yield.maturityMonths);
              
              yields.add(yield);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading Treasury JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Create parquet file
    writeTreasuryYieldsParquet(yields, targetFile);
    LOGGER.info("Converted Treasury yields data to parquet: {} ({} records)", targetFile, yields.size());
  }
  
  /**
   * Converts cached federal debt data to Parquet format.
   * 
   * @param sourceDir Directory containing cached federal debt JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertFederalDebtToParquet(File sourceDir, File targetFile) throws IOException {
    LOGGER.info("Converting federal debt data from {} to parquet: {}", sourceDir, targetFile);
    
    // Skip if target file already exists
    if (targetFile.exists()) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFile);
      return;
    }
    
    // Ensure target directory exists
    targetFile.getParentFile().mkdirs();
    
    List<FederalDebt> debtRecords = new ArrayList<>();
    
    // Look for federal debt JSON files in the source directory
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("federal_debt.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);
          JsonNode data = root.get("data");
          
          if (data != null && data.isArray()) {
            for (JsonNode record : data) {
              FederalDebt debt = new FederalDebt();
              debt.date = record.get("record_date").asText();
              debt.debtType = "Total Public Debt Outstanding";
              
              // Parse debt amounts (convert from millions to billions)
              JsonNode totalDebtNode = record.get("tot_pub_debt_out_amt");
              if (totalDebtNode != null) {
                debt.totalDebt = totalDebtNode.asDouble(0.0) / 1000.0; // Convert millions to billions
              }
              
              JsonNode publicDebtNode = record.get("debt_held_public_amt");
              if (publicDebtNode != null) {
                debt.debtHeldByPublic = publicDebtNode.asDouble(0.0) / 1000.0;
              }
              
              JsonNode intragovDebtNode = record.get("intragov_hold_amt");
              if (intragovDebtNode != null) {
                debt.intragovDebt = intragovDebtNode.asDouble(0.0) / 1000.0;
              }
              
              debt.holderCategory = "All";
              
              debtRecords.add(debt);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading federal debt JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }
    
    // Create parquet file
    writeFederalDebtParquet(debtRecords, targetFile);
    LOGGER.info("Converted federal debt data to parquet: {} ({} records)", targetFile, debtRecords.size());
  }
}