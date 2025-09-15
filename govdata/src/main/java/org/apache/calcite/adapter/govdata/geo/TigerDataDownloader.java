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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Downloads and manages U.S. Census TIGER/Line geographic boundary files.
 *
 * <p>TIGER/Line Shapefiles are free, publicly available geographic boundary
 * files from the U.S. Census Bureau. No registration required.
 *
 * <p>Available datasets:
 * <ul>
 *   <li>States and equivalent entities</li>
 *   <li>Counties and equivalent entities</li>
 *   <li>Places (cities, towns, CDPs)</li>
 *   <li>ZIP Code Tabulation Areas (ZCTAs)</li>
 *   <li>Congressional districts</li>
 *   <li>School districts</li>
 * </ul>
 *
 * <p>Data is available from: https://www2.census.gov/geo/tiger/
 */
public class TigerDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerDataDownloader.class);
  
  private static final String TIGER_BASE_URL = "https://www2.census.gov/geo/tiger";
  
  private final File cacheDir;
  private final List<Integer> dataYears;
  private final boolean autoDownload;
  
  /**
   * Constructor with year list.
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload) {
    this.cacheDir = cacheDir;
    this.dataYears = dataYears;
    this.autoDownload = autoDownload;
    
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    
    LOGGER.info("TIGER data downloader initialized for years {} in directory: {}", 
        dataYears, cacheDir);
  }
  
  /**
   * Backward compatibility constructor with single year.
   */
  public TigerDataDownloader(File cacheDir, int dataYear, boolean autoDownload) {
    this(cacheDir, Arrays.asList(dataYear), autoDownload);
  }
  
  /**
   * Download state boundary shapefiles for all configured years.
   */
  public void downloadStates() throws IOException {
    for (int year : dataYears) {
      downloadStatesForYear(year);
    }
  }
  
  /**
   * Download state boundary shapefile for the first configured year.
   * For backward compatibility with tests.
   */
  public File downloadStatesFirstYear() throws IOException {
    if (dataYears.isEmpty()) {
      return null;
    }
    return downloadStatesForYear(dataYears.get(0));
  }
  
  /**
   * Download state boundary shapefile for a specific year.
   */
  public File downloadStatesForYear(int year) throws IOException {
    String filename = String.format("tl_%d_us_state.zip", year);
    String url = String.format("%s/TIGER%d/STATE/%s", TIGER_BASE_URL, year, filename);
    
    // Create year-partitioned directory structure
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "states");
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("States shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. States shapefile not found for year {}: {}", year, zipFile);
      return null;
    }
    
    LOGGER.info("Downloading states shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Download county boundary shapefiles for all configured years.
   */
  public void downloadCounties() throws IOException {
    for (int year : dataYears) {
      downloadCountiesForYear(year);
    }
  }
  
  /**
   * Download county boundary shapefile for the first configured year.
   * For backward compatibility with tests.
   */
  public File downloadCountiesFirstYear() throws IOException {
    if (dataYears.isEmpty()) {
      return null;
    }
    return downloadCountiesForYear(dataYears.get(0));
  }
  
  /**
   * Download county boundary shapefile for a specific year.
   */
  public File downloadCountiesForYear(int year) throws IOException {
    String filename = String.format("tl_%d_us_county.zip", year);
    String url = String.format("%s/TIGER%d/COUNTY/%s", TIGER_BASE_URL, year, filename);
    
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "counties");
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("Counties shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Counties shapefile not found for year {}: {}", year, zipFile);
      return null;
    }
    
    LOGGER.info("Downloading counties shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Download places (cities, towns) boundary shapefiles for all configured years.
   */
  public void downloadPlaces() throws IOException {
    // Download places for all states (simplified for now)
    for (int year : dataYears) {
      downloadAllPlacesForYear(year);
    }
  }
  
  /**
   * Download places (cities, towns) boundary shapefile.
   * Note: Places are downloaded by state FIPS code.
   */
  public File downloadPlacesForYear(int year, String stateFips) throws IOException {
    String filename = String.format("tl_%d_%s_place.zip", year, stateFips);
    String url = String.format("%s/TIGER%d/PLACE/%s", TIGER_BASE_URL, year, filename);
    
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "places/" + stateFips);
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("Places shapefile already exists for state {}: {}", stateFips, zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Places shapefile not found for state {}: {}", 
          stateFips, zipFile);
      return null;
    }
    
    LOGGER.info("Downloading places shapefile for state {} from: {}", stateFips, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Download all places for all states for a specific year.
   */
  private void downloadAllPlacesForYear(int year) throws IOException {
    // Download places for all 50 states + DC + territories
    String[] stateFipsCodes = {
        "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
        "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
        "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
        "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56", "60", "66", "69", "72", "78" // Territories
    };
    
    for (String stateFips : stateFipsCodes) {
      try {
        downloadPlacesForYear(year, stateFips);
      } catch (Exception e) {
        LOGGER.warn("Failed to download places for state {} year {}: {}", stateFips, year, e.getMessage());
      }
    }
  }
  
  /**
   * Download ZIP Code Tabulation Areas (ZCTAs) shapefiles for all configured years.
   */
  public void downloadZctas() throws IOException {
    for (int year : dataYears) {
      downloadZctasForYear(year);
    }
  }
  
  /**
   * Download ZIP Code Tabulation Areas (ZCTAs) shapefile for a specific year.
   */
  public File downloadZctasForYear(int year) throws IOException {
    String filename = String.format("tl_%d_us_zcta520.zip", year);
    String url = String.format("%s/TIGER%d/ZCTA520/%s", TIGER_BASE_URL, year, filename);
    
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "zctas");
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("ZCTAs shapefile already exists: {}", zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. ZCTAs shapefile not found: {}", zipFile);
      return null;
    }
    
    LOGGER.info("Downloading ZCTAs shapefile from: {}", url);
    LOGGER.warn("Note: ZCTA file is large (~200MB), this may take a while...");
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Download congressional districts shapefiles for all configured years.
   */
  public void downloadCongressionalDistricts() throws IOException {
    for (int year : dataYears) {
      downloadCongressionalDistrictsForYear(year);
    }
  }
  
  /**
   * Download congressional districts shapefile for a specific year.
   */
  public File downloadCongressionalDistrictsForYear(int year) throws IOException {
    String filename = String.format("tl_%d_us_cd118.zip", year); // 118th Congress
    String url = String.format("%s/TIGER%d/CD/%s", TIGER_BASE_URL, year, filename);
    
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "congressional_districts");
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("Congressional districts shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Congressional districts shapefile not found for year {}: {}", year, zipFile);
      return null;
    }
    
    LOGGER.info("Downloading congressional districts shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Download a file from a URL.
   */
  private void downloadFile(String urlString, File outputFile) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(60000);
    
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to download file. HTTP response code: " + responseCode);
    }
    
    long contentLength = conn.getContentLengthLong();
    LOGGER.info("Downloading {} ({} MB)", outputFile.getName(), contentLength / (1024 * 1024));
    
    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(outputFile)) {
      
      byte[] buffer = new byte[8192];
      int bytesRead;
      long totalBytesRead = 0;
      long lastLogTime = System.currentTimeMillis();
      
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;
        
        // Log progress every 5 seconds
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > 5000) {
          int percentComplete = (int) ((totalBytesRead * 100) / contentLength);
          LOGGER.info("Download progress: {}% ({} MB / {} MB)", 
              percentComplete,
              totalBytesRead / (1024 * 1024),
              contentLength / (1024 * 1024));
          lastLogTime = currentTime;
        }
      }
    }
    
    LOGGER.info("Download complete: {}", outputFile);
  }
  
  /**
   * Extract a ZIP file to a directory.
   */
  private void extractZipFile(File zipFile, File outputDir) throws IOException {
    LOGGER.info("Extracting ZIP file: {}", zipFile);
    
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath()))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        File outputFile = new File(outputDir, entry.getName());
        
        if (entry.isDirectory()) {
          outputFile.mkdirs();
        } else {
          outputFile.getParentFile().mkdirs();
          
          try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = zis.read(buffer)) != -1) {
              fos.write(buffer, 0, bytesRead);
            }
          }
          
          LOGGER.debug("Extracted: {}", outputFile.getName());
        }
        
        zis.closeEntry();
      }
    }
    
    LOGGER.info("Extraction complete to: {}", outputDir);
  }
  
  /**
   * Download census tracts shapefiles for all configured years.
   */
  public void downloadCensusTracts() throws IOException {
    for (int year : dataYears) {
      downloadCensusTractsForYear(year);
    }
  }
  
  /**
   * Download census tracts shapefile for a specific year.
   * Census tracts are organized by state, so we'll download for selected states.
   */
  public File downloadCensusTractsForYear(int year) throws IOException {
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "census_tracts");
    
    // Check if we already have census tract data
    if (targetDir.exists() && targetDir.listFiles() != null && targetDir.listFiles().length > 0) {
      LOGGER.info("Census tracts already downloaded for year {}: {}", year, targetDir);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Census tracts not found for year {}: {}", year, targetDir);
      return null;
    }
    
    targetDir.mkdirs();
    
    // Download census tracts for selected states only
    // Using same states as we do for places: CA(06), TX(48), NY(36), FL(12)
    String[] stateFips = {"06", "48", "36", "12"};
    
    for (String fips : stateFips) {
      String filename = String.format("tl_%d_%s_tract.zip", year, fips);
      String url = String.format("%s/TIGER%d/TRACT/%s", TIGER_BASE_URL, year, filename);
      
      File stateDir = new File(targetDir, fips);
      File zipFile = new File(stateDir, filename);
      
      if (zipFile.exists()) {
        LOGGER.info("Census tracts shapefile already exists for state {}: {}", fips, zipFile);
        continue;
      }
      
      LOGGER.info("Downloading census tracts shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();
      
      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to download census tracts for state {}: {}", fips, e.getMessage());
        // Continue with other states even if one fails
      }
    }
    
    return targetDir;
  }
  
  /**
   * Download block groups shapefiles for all configured years.
   */
  public void downloadBlockGroups() throws IOException {
    for (int year : dataYears) {
      downloadBlockGroupsForYear(year);
    }
  }
  
  /**
   * Download block groups shapefile for a specific year.
   * Block groups are organized by state, so we'll download for selected states.
   */
  public File downloadBlockGroupsForYear(int year) throws IOException {
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "block_groups");
    
    // Check if we already have block group data
    if (targetDir.exists() && targetDir.listFiles() != null && targetDir.listFiles().length > 0) {
      LOGGER.info("Block groups already downloaded for year {}: {}", year, targetDir);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Block groups not found for year {}: {}", year, targetDir);
      return null;
    }
    
    targetDir.mkdirs();
    
    // Download block groups for selected states only
    // Using same states as we do for places: CA(06), TX(48), NY(36), FL(12)
    String[] stateFips = {"06", "48", "36", "12"};
    
    for (String fips : stateFips) {
      String filename = String.format("tl_%d_%s_bg.zip", year, fips);
      String url = String.format("%s/TIGER%d/BG/%s", TIGER_BASE_URL, year, filename);
      
      File stateDir = new File(targetDir, fips);
      File zipFile = new File(stateDir, filename);
      
      if (zipFile.exists()) {
        LOGGER.info("Block groups shapefile already exists for state {}: {}", fips, zipFile);
        continue;
      }
      
      LOGGER.info("Downloading block groups shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();
      
      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to download block groups for state {}: {}", fips, e.getMessage());
        // Continue with other states even if one fails
      }
    }
    
    return targetDir;
  }
  
  /**
   * Download Core Based Statistical Areas (CBSAs) shapefiles for all configured years.
   */
  public void downloadCbsas() throws IOException {
    for (int year : dataYears) {
      downloadCbsasForYear(year);
    }
  }
  
  /**
   * Download Core Based Statistical Areas (CBSAs) shapefile for a specific year.
   */
  public File downloadCbsasForYear(int year) throws IOException {
    String filename = String.format("tl_%d_us_cbsa.zip", year);
    String url = String.format("%s/TIGER%d/CBSA/%s", TIGER_BASE_URL, year, filename);
    
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "cbsa");
    File zipFile = new File(targetDir, filename);
    
    if (zipFile.exists()) {
      LOGGER.info("CBSA shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }
    
    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. CBSA shapefile not found for year {}: {}", year, zipFile);
      return null;
    }
    
    LOGGER.info("Downloading CBSA shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);
    
    return targetDir;
  }
  
  /**
   * Get the cache directory.
   */
  public File getCacheDir() {
    return cacheDir;
  }
  
  /**
   * Check if auto-download is enabled.
   */
  public boolean isAutoDownload() {
    return autoDownload;
  }

  /**
   * Check if a shapefile exists in the cache.
   */
  public boolean isShapefileAvailable(String category) {
    File dir = new File(cacheDir, category);
    if (!dir.exists()) {
      return false;
    }
    
    // Check for .shp file
    File[] shpFiles = dir.listFiles((d, name) -> name.endsWith(".shp"));
    return shpFiles != null && shpFiles.length > 0;
  }
}