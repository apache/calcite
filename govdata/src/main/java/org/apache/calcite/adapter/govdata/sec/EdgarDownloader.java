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
package org.apache.calcite.adapter.sec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Downloads XBRL filings from SEC EDGAR.
 * This implementation downloads actual documents from SEC EDGAR API.
 */
public class EdgarDownloader {
  private static final Logger LOGGER = Logger.getLogger(EdgarDownloader.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String USER_AGENT = "Apache Calcite SEC Adapter (apache-calcite@apache.org)";

  // SEC API endpoints
  private static final String SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK%s.json";
  private static final String XBRL_URL = "https://www.sec.gov/Archives/edgar/data/%s/%s/%s";

  private final Map<String, Object> edgarConfig;
  private final File targetDirectory;

  public EdgarDownloader(Map<String, Object> edgarConfig, File targetDirectory) {
    this.edgarConfig = edgarConfig;
    this.targetDirectory = targetDirectory;
    targetDirectory.mkdirs();
  }

  /**
   * Download filings from SEC EDGAR.
   * @return List of downloaded XBRL files
   */
  public List<File> downloadFilings() throws IOException {
    List<File> downloadedFiles = new ArrayList<>();

    // Get CIKs from configuration
    List<String> ciks = new ArrayList<>();
    if (edgarConfig.get("ciks") instanceof List) {
      List<String> configCiks = (List<String>) edgarConfig.get("ciks");
      for (String identifier : configCiks) {
        ciks.addAll(CikRegistry.resolveCiks(identifier));
      }
    } else if (edgarConfig.get("cik") instanceof String) {
      String identifier = (String) edgarConfig.get("cik");
      ciks.addAll(CikRegistry.resolveCiks(identifier));
    }

    // Get filing types
    List<String> filingTypes =
        (List<String>) edgarConfig.getOrDefault("filingTypes", Arrays.asList("10-K", "10-Q", "8-K"));

    // Get date range
    String startDateStr = (String) edgarConfig.get("startDate");
    String endDateStr = (String) edgarConfig.get("endDate");

    LocalDate startDate = startDateStr != null
        ? LocalDate.parse(startDateStr)
        : LocalDate.of(2020, 1, 1);
    LocalDate endDate = endDateStr != null
        ? LocalDate.parse(endDateStr)
        : LocalDate.now();

    LOGGER.info("SEC EDGAR Download Configuration:");
    LOGGER.info("  CIKs: " + ciks);
    LOGGER.info("  Filing Types: " + filingTypes);
    LOGGER.info("  Date Range: " + startDate + " to " + endDate);
    LOGGER.info("  Target Directory: " + targetDirectory);

    // Process each CIK
    for (String cik : ciks) {
      // Normalize CIK to 10 digits
      cik = String.format(Locale.ROOT, "%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));

      LOGGER.info("Processing CIK " + cik + "...");

      try {
        // Fetch submissions metadata
        JsonNode submissions = fetchSubmissions(cik);
        if (submissions == null) {
          LOGGER.warning("Could not fetch submissions for CIK " + cik);
          continue;
        }

        // Extract and download XBRL files
        List<File> cikFiles = downloadXBRLFiles(cik, submissions, filingTypes, startDate, endDate);
        downloadedFiles.addAll(cikFiles);

        LOGGER.info("Downloaded " + cikFiles.size() + " files for CIK " + cik);

      } catch (Exception e) {
        LOGGER.warning("Error processing CIK " + cik + ": " + e.getMessage());
      }
    }

    LOGGER.info("Total downloaded files: " + downloadedFiles.size());
    return downloadedFiles;
  }

  /**
   * Fetch submissions metadata for a CIK.
   */
  private JsonNode fetchSubmissions(String cik) throws IOException {
    String url = String.format(Locale.ROOT, SUBMISSIONS_URL, cik);

    LOGGER.fine("Fetching submissions from: " + url);

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Accept", "application/json");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Rate limiting - SEC allows 10 requests per second
    try {
      Thread.sleep(100); // 100ms = 10 requests per second max
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    int responseCode = conn.getResponseCode();
    if (responseCode == 200) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        return MAPPER.readTree(reader);
      }
    } else {
      LOGGER.warning("Failed to fetch submissions: HTTP " + responseCode);
      return null;
    }
  }

  /**
   * Download XBRL files based on submissions data.
   */
  private List<File> downloadXBRLFiles(String cik, JsonNode submissions,
      List<String> filingTypes, LocalDate startDate, LocalDate endDate) throws IOException {

    List<File> downloadedFiles = new ArrayList<>();

    // Get recent filings
    JsonNode recent = submissions.path("filings").path("recent");
    if (recent.isMissingNode()) {
      LOGGER.warning("No recent filings found for CIK " + cik);
      return downloadedFiles;
    }

    // Extract filing arrays
    JsonNode forms = recent.path("form");
    JsonNode filingDates = recent.path("filingDate");
    JsonNode accessionNumbers = recent.path("accessionNumber");
    JsonNode primaryDocuments = recent.path("primaryDocument");

    if (!forms.isArray()) {
      LOGGER.warning("Invalid filings structure for CIK " + cik);
      return downloadedFiles;
    }

    // Process each filing
    for (int i = 0; i < forms.size(); i++) {
      String formType = forms.get(i).asText();
      String filingDateStr = filingDates.get(i).asText();
      String accessionNumber = accessionNumbers.get(i).asText();
      String primaryDoc = primaryDocuments.get(i).asText();

      // Check if we should download this filing
      LocalDate filingDate = LocalDate.parse(filingDateStr);

      if (!shouldDownload(formType, filingDate, filingTypes, startDate, endDate)) {
        continue;
      }

      // Try to download XBRL instance document
      File xbrlFile =
          downloadXBRLDocument(cik, formType, filingDate, accessionNumber, primaryDoc);

      if (xbrlFile != null) {
        downloadedFiles.add(xbrlFile);
        LOGGER.info("Downloaded: " + xbrlFile.getName());
      }
    }

    return downloadedFiles;
  }

  /**
   * Check if we should download this filing.
   */
  private boolean shouldDownload(String formType, LocalDate filingDate,
      List<String> filingTypes, LocalDate startDate, LocalDate endDate) {

    // Check filing type
    // Empty list means download all filing types
    if (!filingTypes.isEmpty()) {
      boolean typeMatch = false;
      for (String allowedType : filingTypes) {
        if (formType.equals(allowedType) || formType.startsWith(allowedType)) {
          typeMatch = true;
          break;
        }
      }
      if (!typeMatch) {
        return false;
      }
    }

    // Check date range
    if (filingDate.isBefore(startDate) || filingDate.isAfter(endDate)) {
      return false;
    }

    return true;
  }

  /**
   * Download a specific XBRL document.
   */
  private File downloadXBRLDocument(String cik, String formType, LocalDate filingDate,
      String accessionNumber, String primaryDoc) throws IOException {

    // Generate local filename
    String dateStr = filingDate.format(DateTimeFormatter.BASIC_ISO_DATE);
    String cleanFormType = formType.replace("/", "-").replace(" ", "_");
    String filename =
        String.format(Locale.ROOT, "%s_%s_%s.xml", cik, dateStr, cleanFormType);
    File localFile = new File(targetDirectory, filename);

    // Skip if already downloaded (EDGAR filings are immutable)
    if (localFile.exists() && localFile.length() > 100) {
      LOGGER.fine("Already cached: " + localFile.getName());
      return localFile;
    }

    // Try different XBRL document naming patterns
    String[] xbrlPatterns = {
        primaryDoc.replace(".htm", "_htm.xml"),  // Most common pattern
        primaryDoc.replace(".htm", "-xbrl.xml"), // Alternative pattern
        accessionNumber + "-xbrl.xml",           // Accession-based
        accessionNumber + ".xml"                 // Simple XML
    };

    String accessionNoDash = accessionNumber.replace("-", "");
    String cikNoLeadingZeros = cik.replaceFirst("^0+", "");

    for (String xbrlDoc : xbrlPatterns) {
      String url =
          String.format(Locale.ROOT, XBRL_URL, cikNoLeadingZeros, accessionNoDash, xbrlDoc);

      LOGGER.fine("Trying: " + url);

      try {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);

        // Rate limiting
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
          // Download the file
          try (InputStream in = conn.getInputStream();
               FileOutputStream out = new FileOutputStream(localFile)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            long totalBytes = 0;

            while ((bytesRead = in.read(buffer)) != -1) {
              out.write(buffer, 0, bytesRead);
              totalBytes += bytesRead;
            }

            LOGGER.info("Downloaded " + totalBytes + " bytes: " + localFile.getName());
            return localFile;
          }
        } else if (responseCode == 429) {
          // Rate limited
          LOGGER.warning("Rate limited by SEC. Waiting 60 seconds...");
          try {
            Thread.sleep(60000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } catch (Exception e) {
        LOGGER.fine("Failed to download " + xbrlDoc + ": " + e.getMessage());
      }
    }

    LOGGER.fine("Could not find XBRL document for " + formType + " " + filingDate);
    return null;
  }
}
