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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client for accessing U.S. Census Bureau APIs.
 *
 * <p>Provides access to:
 * <ul>
 *   <li>American Community Survey (ACS) demographic data</li>
 *   <li>Decennial Census data</li>
 *   <li>Economic indicators</li>
 *   <li>Geocoding services</li>
 * </ul>
 *
 * <p>The Census API is free but requires registration for an API key at
 * https://api.census.gov/data/key_signup.html
 *
 * <p>Rate limits: 500 requests per IP address per day (very generous)
 */
public class CensusApiClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusApiClient.class);
  
  private static final String BASE_URL = "https://api.census.gov/data";
  private static final String GEOCODING_URL = "https://geocoding.geo.census.gov/geocoder";
  
  // Rate limiting: Census allows 500/day, we'll be conservative
  private static final int MAX_REQUESTS_PER_SECOND = 2;
  private static final long RATE_LIMIT_DELAY_MS = 500; // 2 requests per second
  
  private final String apiKey;
  private final File cacheDir;
  private final ObjectMapper objectMapper;
  private final Semaphore rateLimiter;
  private final AtomicLong lastRequestTime;
  
  public CensusApiClient(String apiKey, File cacheDir) {
    this.apiKey = apiKey;
    this.cacheDir = cacheDir;
    this.objectMapper = new ObjectMapper();
    this.rateLimiter = new Semaphore(MAX_REQUESTS_PER_SECOND);
    this.lastRequestTime = new AtomicLong(0);
    
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    
    LOGGER.info("Census API client initialized with cache directory: {}", cacheDir);
  }
  
  /**
   * Get demographic data from American Community Survey (ACS).
   *
   * @param year Year of data (e.g., 2022 for 2018-2022 5-year estimates)
   * @param variables Comma-separated list of variables (e.g., "B01001_001E,B19013_001E")
   * @param geography Geographic level (e.g., "state:*", "county:*", "tract:*")
   * @return JSON response from Census API
   */
  public JsonNode getAcsData(int year, String variables, String geography) throws IOException {
    String cacheKey = String.format("acs_%d_%s_%s", year, 
        variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));
    
    // Check cache first
    File cacheFile = new File(cacheDir, cacheKey + ".json");
    if (cacheFile.exists()) {
      LOGGER.debug("Using cached ACS data from {}", cacheFile);
      return objectMapper.readTree(cacheFile);
    }
    
    // Build API URL
    String url = String.format("%s/%d/acs/acs5?get=%s&for=%s&key=%s",
        BASE_URL, year, variables, URLEncoder.encode(geography, "UTF-8"), apiKey);
    
    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);
    
    // Cache the response
    objectMapper.writeValue(cacheFile, response);
    LOGGER.info("Cached ACS data to {}", cacheFile);
    
    return response;
  }
  
  /**
   * Get data from Decennial Census.
   *
   * @param year Census year (2010 or 2020)
   * @param variables Variables to retrieve
   * @param geography Geographic level
   * @return JSON response from Census API
   */
  public JsonNode getDecennialData(int year, String variables, String geography) throws IOException {
    String cacheKey = String.format("dec_%d_%s_%s", year,
        variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));
    
    // Check cache first
    File cacheFile = new File(cacheDir, cacheKey + ".json");
    if (cacheFile.exists()) {
      LOGGER.debug("Using cached Decennial data from {}", cacheFile);
      return objectMapper.readTree(cacheFile);
    }
    
    // Build API URL
    String dataset = (year == 2020) ? "dec/dhc" : "dec/sf1";
    String url = String.format("%s/%d/%s?get=%s&for=%s&key=%s",
        BASE_URL, year, dataset, variables, URLEncoder.encode(geography, "UTF-8"), apiKey);
    
    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);
    
    // Cache the response
    objectMapper.writeValue(cacheFile, response);
    LOGGER.info("Cached Decennial data to {}", cacheFile);
    
    return response;
  }
  
  /**
   * Geocode an address to get coordinates and Census geography.
   *
   * @param street Street address
   * @param city City name
   * @param state State abbreviation
   * @param zip ZIP code (optional)
   * @return Geocoding result with lat/lon and Census geography codes
   */
  public GeocodeResult geocodeAddress(String street, String city, String state, String zip) 
      throws IOException {
    
    // Build geocoding URL
    StringBuilder urlBuilder = new StringBuilder(GEOCODING_URL);
    urlBuilder.append("/locations/onelineaddress?address=");
    
    // Construct address string
    String address = street + ", " + city + ", " + state;
    if (zip != null && !zip.isEmpty()) {
      address += " " + zip;
    }
    urlBuilder.append(URLEncoder.encode(address, "UTF-8"));
    
    urlBuilder.append("&benchmark=2020");
    urlBuilder.append("&format=json");
    
    // Make API request (geocoding doesn't require API key)
    JsonNode response = makeApiRequest(urlBuilder.toString());
    
    // Parse result
    JsonNode result = response.get("result");
    if (result != null && result.has("addressMatches") && result.get("addressMatches").size() > 0) {
      JsonNode match = result.get("addressMatches").get(0);
      JsonNode coords = match.get("coordinates");
      JsonNode geos = match.get("geographies");
      
      GeocodeResult geocodeResult = new GeocodeResult();
      geocodeResult.latitude = coords.get("y").asDouble();
      geocodeResult.longitude = coords.get("x").asDouble();
      
      // Extract Census geography codes
      if (geos != null && geos.has("Census Tracts")) {
        JsonNode tract = geos.get("Census Tracts").get(0);
        geocodeResult.stateFips = tract.get("STATE").asText();
        geocodeResult.countyFips = tract.get("COUNTY").asText();
        geocodeResult.tractCode = tract.get("TRACT").asText();
        geocodeResult.blockGroup = tract.get("BLKGRP").asText();
      }
      
      return geocodeResult;
    }
    
    return null; // No match found
  }
  
  /**
   * Make an API request with rate limiting.
   */
  private JsonNode makeApiRequest(String urlString) throws IOException {
    try {
      // Rate limiting
      rateLimiter.acquire();
      
      // Ensure minimum time between requests
      long now = System.currentTimeMillis();
      long timeSinceLastRequest = now - lastRequestTime.get();
      if (timeSinceLastRequest < RATE_LIMIT_DELAY_MS) {
        Thread.sleep(RATE_LIMIT_DELAY_MS - timeSinceLastRequest);
      }
      lastRequestTime.set(System.currentTimeMillis());
      
      // Make HTTP request (Java 8 compatible)
      URI uri = URI.create(urlString);
      URL url = uri.toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(30000);
      
      int responseCode = conn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        return objectMapper.readTree(conn.getInputStream());
      } else {
        throw new IOException("Census API request failed with code " + responseCode + 
            " for URL: " + urlString);
      }
      
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Rate limiting interrupted", e);
    } finally {
      rateLimiter.release();
    }
  }
  
  /**
   * Convert Census API JSON response to CSV for easier processing.
   */
  public void convertJsonToCsv(JsonNode jsonData, File outputFile) throws IOException {
    if (!jsonData.isArray() || jsonData.size() < 2) {
      throw new IllegalArgumentException("Invalid Census API response format");
    }
    
    try (FileWriter writer = new FileWriter(outputFile)) {
      // First row is headers
      JsonNode headers = jsonData.get(0);
      for (int i = 0; i < headers.size(); i++) {
        if (i > 0) writer.write(",");
        writer.write(headers.get(i).asText());
      }
      writer.write("\n");
      
      // Remaining rows are data
      for (int row = 1; row < jsonData.size(); row++) {
        JsonNode dataRow = jsonData.get(row);
        for (int i = 0; i < dataRow.size(); i++) {
          if (i > 0) writer.write(",");
          writer.write(dataRow.get(i).asText());
        }
        writer.write("\n");
      }
    }
    
    LOGGER.info("Converted Census API response to CSV: {}", outputFile);
  }
  
  /**
   * Result of geocoding an address.
   */
  public static class GeocodeResult {
    public double latitude;
    public double longitude;
    public String stateFips;
    public String countyFips;
    public String tractCode;
    public String blockGroup;
    
    @Override public String toString() {
      return String.format("GeocodeResult[lat=%.6f, lon=%.6f, state=%s, county=%s, tract=%s]",
          latitude, longitude, stateFips, countyFips, tractCode);
    }
  }
  
  /**
   * Common Census variables for reference.
   */
  public static class Variables {
    // Population
    public static final String TOTAL_POPULATION = "B01001_001E";
    public static final String MALE_POPULATION = "B01001_002E";
    public static final String FEMALE_POPULATION = "B01001_026E";
    
    // Income
    public static final String MEDIAN_HOUSEHOLD_INCOME = "B19013_001E";
    public static final String PER_CAPITA_INCOME = "B19301_001E";
    
    // Housing
    public static final String TOTAL_HOUSING_UNITS = "B25001_001E";
    public static final String OCCUPIED_HOUSING_UNITS = "B25002_002E";
    public static final String VACANT_HOUSING_UNITS = "B25002_003E";
    public static final String MEDIAN_HOME_VALUE = "B25077_001E";
    
    // Employment
    public static final String LABOR_FORCE = "B23025_002E";
    public static final String EMPLOYED = "B23025_004E";
    public static final String UNEMPLOYED = "B23025_005E";
    
    // Education
    public static final String HIGH_SCHOOL_GRADUATE = "B15003_017E";
    public static final String BACHELORS_DEGREE = "B15003_022E";
    public static final String GRADUATE_DEGREE = "B15003_024E";
  }
}