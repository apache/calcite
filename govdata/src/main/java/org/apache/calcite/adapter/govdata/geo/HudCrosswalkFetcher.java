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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Fetches HUD-USPS ZIP Code Crosswalk files.
 *
 * <p>The HUD-USPS Crosswalk Files are free quarterly datasets that map
 * ZIP codes to various Census geographies (counties, tracts, CBSAs).
 *
 * <p>Registration required at: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
 * <p>Free registration, no cost for data access.
 *
 * <p>Provides mappings from ZIP codes to:
 * <ul>
 *   <li>Counties</li>
 *   <li>Census Tracts</li>
 *   <li>Core Based Statistical Areas (CBSAs/Metro areas)</li>
 *   <li>Congressional Districts</li>
 * </ul>
 *
 * <p>Data includes residential, business, and other address ratios for
 * proportional allocation of data from ZIP codes to Census geographies.
 */
public class HudCrosswalkFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(HudCrosswalkFetcher.class);
  
  private static final String HUD_API_BASE = "https://www.huduser.gov/hudapi/public/usps";
  private static final String HUD_DOWNLOAD_BASE = "https://www.huduser.gov/portal/datasets/usps";
  
  private final String username;
  private final String password;
  private final String token;
  private final File cacheDir;
  private final ObjectMapper objectMapper;
  
  public HudCrosswalkFetcher(String username, String password, File cacheDir) {
    this(username, password, null, cacheDir);
  }
  
  public HudCrosswalkFetcher(String username, String password, String token, File cacheDir) {
    this.username = username;
    this.password = password;
    this.token = token;
    this.cacheDir = cacheDir;
    this.objectMapper = new ObjectMapper();
    
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    
    LOGGER.info("HUD crosswalk fetcher initialized with cache directory: {}", cacheDir);
  }
  
  /**
   * Download the latest ZIP to County crosswalk file.
   */
  public File downloadZipToCounty(String quarter, int year) throws IOException {
    String filename = String.format("ZIP_COUNTY_%dQ%s.csv", year, quarter);
    File outputFile = new File(cacheDir, filename);
    
    if (outputFile.exists()) {
      LOGGER.info("ZIP to County crosswalk already exists: {}", outputFile);
      return outputFile;
    }
    
    LOGGER.info("Downloading ZIP to County crosswalk for {}Q{}", year, quarter);
    
    // HUD API endpoint for ZIP to County (type=2 is zip-county crosswalk)
    String url = String.format("%s?type=2&query=All&year=%d&quarter=%s",
        HUD_API_BASE, year, quarter);
    
    JsonNode data = fetchHudData(url);
    convertJsonToCsv(data, outputFile, "zip_county");
    
    return outputFile;
  }
  
  /**
   * Download the latest ZIP to Census Tract crosswalk file.
   */
  public File downloadZipToTract(String quarter, int year) throws IOException {
    String filename = String.format("ZIP_TRACT_%dQ%s.csv", year, quarter);
    File outputFile = new File(cacheDir, filename);
    
    if (outputFile.exists()) {
      LOGGER.info("ZIP to Tract crosswalk already exists: {}", outputFile);
      return outputFile;
    }
    
    LOGGER.info("Downloading ZIP to Tract crosswalk for {}Q{}", year, quarter);
    
    // HUD API endpoint for ZIP to Tract (type=1 is zip-tract crosswalk)
    String url = String.format("%s?type=1&query=All&year=%d&quarter=%s",
        HUD_API_BASE, year, quarter);
    
    JsonNode data = fetchHudData(url);
    convertJsonToCsv(data, outputFile, "zip_tract");
    
    return outputFile;
  }
  
  /**
   * Download the latest ZIP to CBSA (Metro Area) crosswalk file.
   */
  public File downloadZipToCbsa(String quarter, int year) throws IOException {
    String filename = String.format("ZIP_CBSA_%dQ%s.csv", year, quarter);
    File outputFile = new File(cacheDir, filename);
    
    if (outputFile.exists()) {
      LOGGER.info("ZIP to CBSA crosswalk already exists: {}", outputFile);
      return outputFile;
    }
    
    LOGGER.info("Downloading ZIP to CBSA crosswalk for {}Q{}", year, quarter);
    
    // HUD API endpoint for ZIP to CBSA (type=3 is zip-cbsa crosswalk)
    String url = String.format("%s?type=3&query=All&year=%d&quarter=%s",
        HUD_API_BASE, year, quarter);
    
    JsonNode data = fetchHudData(url);
    convertJsonToCsv(data, outputFile, "zip_cbsa");
    
    return outputFile;
  }
  
  /**
   * Download all crosswalk files for the latest available quarter.
   */
  public void downloadLatestCrosswalk() throws IOException {
    // Default to most recent complete quarter
    // Q2 2024 data is confirmed available
    String quarter = "2";
    int year = 2024;
    
    LOGGER.info("Downloading HUD crosswalk files for {}Q{}", year, quarter);
    
    try {
      downloadZipToCounty(quarter, year);
      downloadZipToTract(quarter, year);
      downloadZipToCbsa(quarter, year);
      
      // Also download the congressional district crosswalk if available
      downloadZipToCongressionalDistrict(quarter, year);
      
      LOGGER.info("Successfully downloaded all HUD crosswalk files");
    } catch (Exception e) {
      LOGGER.error("Error downloading HUD crosswalk files", e);
      throw new IOException("Failed to download HUD crosswalk files", e);
    }
  }
  
  /**
   * Download ZIP to Congressional District crosswalk.
   */
  public File downloadZipToCongressionalDistrict(String quarter, int year) throws IOException {
    String filename = String.format("ZIP_CD_%dQ%s.csv", year, quarter);
    File outputFile = new File(cacheDir, filename);
    
    if (outputFile.exists()) {
      LOGGER.info("ZIP to Congressional District crosswalk already exists: {}", outputFile);
      return outputFile;
    }
    
    LOGGER.info("Downloading ZIP to Congressional District crosswalk for {}Q{}", year, quarter);
    
    // HUD API endpoint for ZIP to Congressional District (type=5 is zip-cd crosswalk)
    String url = String.format("%s?type=5&query=All&year=%d&quarter=%s",
        HUD_API_BASE, year, quarter);
    
    try {
      JsonNode data = fetchHudData(url);
      convertJsonToCsv(data, outputFile, "zip_cd");
    } catch (Exception e) {
      LOGGER.warn("Congressional District crosswalk may not be available: {}", e.getMessage());
      // Not all quarters have CD crosswalk, this is not a fatal error
    }
    
    return outputFile;
  }
  
  /**
   * Fetch data from HUD API with authentication.
   */
  private JsonNode fetchHudData(String urlString) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    
    // Add authentication - prefer token over basic auth
    if (token != null && !token.isEmpty()) {
      // Use token authentication if available
      conn.setRequestProperty("Authorization", "Bearer " + token);
    } else if (username != null && password != null) {
      // Fall back to basic authentication
      String auth = username + ":" + password;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
      conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
    }
    
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(30000);
    
    int responseCode = conn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      try (InputStream is = conn.getInputStream()) {
        return objectMapper.readTree(is);
      }
    } else if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new IOException("HUD API authentication failed. Please check username and password.");
    } else {
      throw new IOException("HUD API request failed with code " + responseCode);
    }
  }
  
  /**
   * Convert HUD JSON response to CSV format.
   */
  private void convertJsonToCsv(JsonNode jsonData, File outputFile, String dataType) 
      throws IOException {
    
    try (FileWriter writer = new FileWriter(outputFile)) {
      // Write headers based on data type
      switch (dataType) {
        case "zip_county":
          writer.write("zip,county,res_ratio,bus_ratio,oth_ratio,tot_ratio,city,state\n");
          break;
        case "zip_tract":
          writer.write("zip,tract,res_ratio,bus_ratio,oth_ratio,tot_ratio,city,state\n");
          break;
        case "zip_cbsa":
          writer.write("zip,cbsa,res_ratio,bus_ratio,oth_ratio,tot_ratio,city,state\n");
          break;
        case "zip_cd":
          writer.write("zip,cd,res_ratio,bus_ratio,oth_ratio,tot_ratio,city,state\n");
          break;
      }
      
      // Write data rows - HUD API returns data in {"data": {"results": [...]}} format
      if (jsonData.has("data") && jsonData.get("data").has("results")) {
        JsonNode results = jsonData.get("data").get("results");
        if (results.isArray()) {
          for (JsonNode record : results) {
            String zip = record.get("zip").asText();
            String geoCode = record.get("geoid").asText(); // HUD API uses "geoid" for all geo codes
            
            double resRatio = record.get("res_ratio").asDouble();
            double busRatio = record.get("bus_ratio").asDouble();
            double othRatio = record.get("oth_ratio").asDouble();
            double totRatio = record.get("tot_ratio").asDouble();
            
            String city = record.has("city") ? record.get("city").asText() : "";
            String state = record.has("state") ? record.get("state").asText() : "";
            
            writer.write(String.format("%s,%s,%.6f,%.6f,%.6f,%.6f,%s,%s\n",
                zip, geoCode, resRatio, busRatio, othRatio, totRatio, city, state));
          }
        }
      }
    }
    
    LOGGER.info("Converted HUD data to CSV: {}", outputFile);
  }
  
  /**
   * Load crosswalk data from CSV file.
   */
  public List<CrosswalkRecord> loadCrosswalkData(File csvFile) throws IOException {
    List<CrosswalkRecord> records = new ArrayList<>();
    
    try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
      String line = reader.readLine(); // Skip header
      
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length >= 6) {
          CrosswalkRecord record = new CrosswalkRecord();
          record.zip = parts[0];
          record.geoCode = parts[1];
          record.resRatio = Double.parseDouble(parts[2]);
          record.busRatio = Double.parseDouble(parts[3]);
          record.othRatio = Double.parseDouble(parts[4]);
          record.totRatio = Double.parseDouble(parts[5]);
          
          // Add city and state if available (new format)
          if (parts.length >= 8) {
            record.city = parts[6];
            record.state = parts[7];
          }
          
          records.add(record);
        }
      }
    }
    
    LOGGER.info("Loaded {} crosswalk records from {}", records.size(), csvFile);
    return records;
  }
  
  /**
   * Get the cache directory.
   */
  public File getCacheDir() {
    return cacheDir;
  }
  
  /**
   * Crosswalk record structure.
   */
  public static class CrosswalkRecord {
    public String zip;
    public String geoCode; // County, Tract, CBSA, or CD depending on type
    public double resRatio; // Residential address ratio
    public double busRatio; // Business address ratio
    public double othRatio; // Other address ratio
    public double totRatio; // Total address ratio
    public String city; // City name (if available)
    public String state; // State code (if available)
    
    @Override public String toString() {
      return String.format("Crosswalk[zip=%s, geo=%s, city=%s, state=%s, res=%.3f, bus=%.3f, tot=%.3f]",
          zip, geoCode, city, state, resRatio, busRatio, totRatio);
    }
  }
}