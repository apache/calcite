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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Client for accessing Bureau of Labor Statistics (BLS) API.
 *
 * <p>Provides methods to fetch employment, inflation, and wage data from BLS.
 * Supports both authenticated (with API key) and unauthenticated access.
 */
public class BlsApiClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlsApiClient.class);
  private static final String BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private final String apiKey;
  private final HttpClient httpClient;
  
  public BlsApiClient(String apiKey) {
    this.apiKey = apiKey;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  
  /**
   * Fetches time series data from BLS API.
   *
   * @param seriesId BLS series ID (e.g., "UNRATE" for unemployment rate)
   * @param startYear Start year for data
   * @param endYear End year for data
   * @return List of data points with date and value
   */
  public List<Map<String, Object>> fetchTimeSeries(String seriesId, int startYear, int endYear) 
      throws IOException, InterruptedException {
    
    ObjectNode requestBody = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();
    seriesArray.add(seriesId);
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
      throw new IOException("BLS API request failed with status: " + response.statusCode());
    }
    
    return parseTimeSeriesResponse(response.body(), seriesId);
  }
  
  /**
   * Fetches multiple series in a single request (more efficient).
   */
  public Map<String, List<Map<String, Object>>> fetchMultipleSeries(
      List<String> seriesIds, int startYear, int endYear) 
      throws IOException, InterruptedException {
    
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
      throw new IOException("BLS API request failed with status: " + response.statusCode());
    }
    
    return parseMultiSeriesResponse(response.body());
  }
  
  private List<Map<String, Object>> parseTimeSeriesResponse(String json, String seriesId) 
      throws IOException {
    List<Map<String, Object>> results = new ArrayList<>();
    ObjectNode root = (ObjectNode) MAPPER.readTree(json);
    
    if (!"REQUEST_SUCCEEDED".equals(root.get("status").asText())) {
      throw new IOException("BLS API error: " + root.get("message").asText());
    }
    
    ArrayNode series = (ArrayNode) root.get("Results").get("series");
    if (series != null && series.size() > 0) {
      ArrayNode data = (ArrayNode) series.get(0).get("data");
      data.forEach(node -> {
        Map<String, Object> row = new HashMap<>();
        row.put("series_id", seriesId);
        row.put("year", node.get("year").asText());
        row.put("period", node.get("period").asText());
        row.put("value", node.get("value").asDouble());
        row.put("periodName", node.get("periodName").asText());
        
        // Calculate date from year and period
        String date = calculateDate(node.get("year").asText(), node.get("period").asText());
        row.put("date", date);
        
        results.add(row);
      });
    }
    
    return results;
  }
  
  private Map<String, List<Map<String, Object>>> parseMultiSeriesResponse(String json) 
      throws IOException {
    Map<String, List<Map<String, Object>>> results = new HashMap<>();
    ObjectNode root = (ObjectNode) MAPPER.readTree(json);
    
    if (!"REQUEST_SUCCEEDED".equals(root.get("status").asText())) {
      throw new IOException("BLS API error: " + root.get("message").asText());
    }
    
    ArrayNode series = (ArrayNode) root.get("Results").get("series");
    if (series != null) {
      series.forEach(seriesNode -> {
        String seriesId = seriesNode.get("seriesID").asText();
        List<Map<String, Object>> seriesData = new ArrayList<>();
        
        ArrayNode data = (ArrayNode) seriesNode.get("data");
        data.forEach(node -> {
          Map<String, Object> row = new HashMap<>();
          row.put("series_id", seriesId);
          row.put("year", node.get("year").asText());
          row.put("period", node.get("period").asText());
          row.put("value", node.get("value").asDouble());
          row.put("periodName", node.get("periodName").asText());
          
          String date = calculateDate(node.get("year").asText(), node.get("period").asText());
          row.put("date", date);
          
          seriesData.add(row);
        });
        
        results.put(seriesId, seriesData);
      });
    }
    
    return results;
  }
  
  private String calculateDate(String year, String period) {
    // Convert BLS period codes to dates
    // M01-M12 = months, Q01-Q04 = quarters, A01 = annual
    if (period.startsWith("M")) {
      String month = period.substring(1);
      return String.format("%s-%02d-01", year, Integer.parseInt(month));
    } else if (period.startsWith("Q")) {
      int quarter = Integer.parseInt(period.substring(1));
      int month = (quarter - 1) * 3 + 1;
      return String.format("%s-%02d-01", year, month);
    } else if (period.equals("A01")) {
      return year + "-01-01";
    }
    return year + "-01-01"; // Default to January 1st
  }
  
  /**
   * Common BLS series IDs for easy reference.
   */
  public static class Series {
    public static final String UNEMPLOYMENT_RATE = "LNS14000000";
    public static final String CPI_ALL_URBAN = "CUUR0000SA0";
    public static final String PPI_FINAL_DEMAND = "WPUFD4";
    public static final String EMPLOYMENT_TOTAL = "CES0000000001";
    public static final String AVERAGE_HOURLY_EARNINGS = "CES0500000003";
    public static final String LABOR_FORCE_PARTICIPATION = "LNS11300000";
  }
}