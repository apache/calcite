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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for SEC schemas that extends FileSchema with SEC-specific capabilities.
 * Uses file adapter's infrastructure for HTTP operations, HTML processing, and
 * partitioned Parquet storage.
 *
 * <p>This factory leverages the file adapter's FileConversionManager for XBRL
 * processing and HtmlToJsonConverter for HTML table extraction.
 */
public class SecSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecSchemaFactory.class);

  static {
    System.out.println("DEBUG: SecSchemaFactory class loaded!!");
  }

  // Standard SEC data cache directory - XBRL files are immutable, cache forever
  // Default to /Volumes/T9 for testing, can be overridden with -Dsec.data.home
  private static final String DEFAULT_SEC_HOME = "/Volumes/T9/calcite-sec-cache";
  private static final String SEC_DATA_HOME = System.getProperty("sec.data.home", DEFAULT_SEC_HOME);
  private static final String SEC_RAW_DIR = SEC_DATA_HOME + "/sec-data";
  private static final String SEC_PARQUET_DIR = SEC_DATA_HOME + "/sec-parquet";

  // Parallel processing configuration
  private static final int DOWNLOAD_THREADS = 3; // Reduced to 3 concurrent downloads for better rate limiting
  private static final int CONVERSION_THREADS = 8; // 8 concurrent conversions
  private static final int SEC_RATE_LIMIT_PER_SECOND = 8; // Conservative: 8 requests/sec (SEC allows 10)
  private static final long INITIAL_RATE_LIMIT_DELAY_MS = 125; // Initial: 125ms between requests (8/sec)
  private static final long MAX_RATE_LIMIT_DELAY_MS = 500; // Max: 500ms between requests (2/sec)

  // Dynamic rate limiting
  private final AtomicLong currentRateLimitDelayMs = new AtomicLong(INITIAL_RATE_LIMIT_DELAY_MS);

  // Thread pools and rate limiting
  private final ExecutorService downloadExecutor = Executors.newFixedThreadPool(DOWNLOAD_THREADS);
  private final ExecutorService conversionExecutor = Executors.newFixedThreadPool(CONVERSION_THREADS);
  private final Semaphore rateLimiter = new Semaphore(1); // One permit, released every 125ms
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> downloadFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> conversionFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<FilingToDownload> retryQueue = new ConcurrentLinkedQueue<>();
  private final List<File> scheduledForReconversion = new ArrayList<>();
  private final AtomicInteger totalDownloads = new AtomicInteger(0);
  private final AtomicInteger completedDownloads = new AtomicInteger(0);
  private final AtomicInteger totalConversions = new AtomicInteger(0);
  private final AtomicInteger completedConversions = new AtomicInteger(0);
  private final AtomicInteger rateLimitHits = new AtomicInteger(0);
  private Map<String, Object> currentOperand; // Store operand for table auto-discovery

  public static final SecSchemaFactory INSTANCE = new SecSchemaFactory();

  static {
    LOGGER.debug("SecSchemaFactory class loaded");
  }

  private void shutdownExecutors() {
    try {
      downloadExecutor.shutdown();
      conversionExecutor.shutdown();
      if (!downloadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        downloadExecutor.shutdownNow();
      }
      if (!conversionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        conversionExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      downloadExecutor.shutdownNow();
      conversionExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public SecSchemaFactory() {
    System.out.println("DEBUG: SecSchemaFactory constructor called!!");
    LOGGER.debug("SecSchemaFactory constructor called");
  }

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    System.out.println("DEBUG: SecSchemaFactory.create() called with operand: " + operand);
    LOGGER.debug("SecSchemaFactory.create() called");
    LOGGER.debug("Operand: {}", operand);

    // Create mutable copy of operand to allow modifications
    Map<String, Object> mutableOperand = new HashMap<>(operand);
    this.currentOperand = mutableOperand; // Store for table auto-discovery

    // Determine cache directory
    String configuredDir = (String) mutableOperand.get("directory");
    if (configuredDir == null) {
      configuredDir = (String) mutableOperand.get("cacheDirectory");
    }
    String cacheHome = configuredDir != null ? configuredDir : SEC_DATA_HOME;

    // Handle SEC data download if configured
    System.out.println("DEBUG: About to check shouldDownloadData...");
    LOGGER.debug("Checking shouldDownloadData...");
    if (shouldDownloadData(mutableOperand)) {
      LOGGER.debug("shouldDownloadData = true, calling downloadSecData");
      // Get base directory from operand
      if (configuredDir != null) {
        File baseDir = new File(configuredDir);
        // Rebuild manifest from existing files first
        rebuildManifestFromExistingFiles(baseDir);
      }
      downloadSecData(mutableOperand);
    } else {
      LOGGER.debug("shouldDownloadData = false");
    }

    // Pre-define partitioned tables using the partitionedTables format
    // These will be resolved by FileSchemaFactory after downloads complete
    List<Map<String, Object>> partitionedTables = new ArrayList<>();

    // Define partition configuration (shared by both tables)
    Map<String, Object> partitionConfig = new HashMap<>();
    partitionConfig.put("style", "hive");  // Use Hive-style partitioning (key=value)

    // Define partition columns
    List<Map<String, Object>> columnDefs = new ArrayList<>();

    Map<String, Object> cikCol = new HashMap<>();
    cikCol.put("name", "cik");
    cikCol.put("type", "VARCHAR");
    columnDefs.add(cikCol);

    Map<String, Object> filingTypeCol = new HashMap<>();
    filingTypeCol.put("name", "filing_type");
    filingTypeCol.put("type", "VARCHAR");
    columnDefs.add(filingTypeCol);

    Map<String, Object> yearCol = new HashMap<>();
    yearCol.put("name", "year");
    yearCol.put("type", "INTEGER");
    columnDefs.add(yearCol);

    partitionConfig.put("columnDefinitions", columnDefs);

    // Define financial_line_items as a partitioned table
    Map<String, Object> financialLineItems = new HashMap<>();
    financialLineItems.put("name", "financial_line_items");
    financialLineItems.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_facts.parquet");
    financialLineItems.put("partitions", partitionConfig);
    partitionedTables.add(financialLineItems);

    // Define filing_contexts as a partitioned table
    Map<String, Object> filingContexts = new HashMap<>();
    filingContexts.put("name", "filing_contexts");
    filingContexts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_contexts.parquet");
    filingContexts.put("partitions", partitionConfig);
    partitionedTables.add(filingContexts);

    // Define mda_sections as a partitioned table for MD&A paragraphs
    Map<String, Object> mdaSections = new HashMap<>();
    mdaSections.put("name", "mda_sections");
    mdaSections.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_mda.parquet");
    mdaSections.put("partitions", partitionConfig);
    partitionedTables.add(mdaSections);

    // Define xbrl_relationships as a partitioned table
    Map<String, Object> xbrlRelationships = new HashMap<>();
    xbrlRelationships.put("name", "xbrl_relationships");
    xbrlRelationships.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_relationships.parquet");
    xbrlRelationships.put("partitions", partitionConfig);
    partitionedTables.add(xbrlRelationships);

    // Define insider_transactions table for Forms 3, 4, 5
    Map<String, Object> insiderTransactions = new HashMap<>();
    insiderTransactions.put("name", "insider_transactions");
    insiderTransactions.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_insider.parquet");
    insiderTransactions.put("partitions", partitionConfig);
    partitionedTables.add(insiderTransactions);

    // Define earnings_transcripts table for 8-K exhibits
    Map<String, Object> earningsTranscripts = new HashMap<>();
    earningsTranscripts.put("name", "earnings_transcripts");
    earningsTranscripts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_earnings.parquet");
    earningsTranscripts.put("partitions", partitionConfig);
    partitionedTables.add(earningsTranscripts);

    // Add partitioned tables to operand
    mutableOperand.put("partitionedTables", partitionedTables);

    // Set the directory for FileSchemaFactory to search
    File parquetDir = new File(cacheHome, "sec-parquet");
    mutableOperand.put("directory", parquetDir.getAbsolutePath());

    LOGGER.info("Pre-defined {} partitioned table patterns", partitionedTables.size());

    // Ensure the parquet directory exists
    File secParquetDir = new File(cacheHome, "sec-parquet");

    if (secParquetDir.exists() && secParquetDir.isDirectory()) {
      LOGGER.info("Using SEC parquet cache directory: {}", secParquetDir.getAbsolutePath());

      // Debug: List all .parquet files
      LOGGER.info("DEBUG: Listing all .parquet files in directory:");
      File[] parquetFiles = secParquetDir.listFiles((dir, fileName) -> fileName.endsWith(".parquet"));
      if (parquetFiles != null && parquetFiles.length > 0) {
        for (File f : parquetFiles) {
          LOGGER.info("DEBUG: Found table file: " + f.getName() + " (size=" + f.length() + ")");
        }
      } else {
        LOGGER.warn("DEBUG: No .parquet files found in " + secParquetDir.getAbsolutePath());
      }
    } else {
      // Parquet dir doesn't exist yet, use the base cache directory
      mutableOperand.put("directory", cacheHome);
      LOGGER.info("SEC parquet directory not ready yet, using base: {}", cacheHome);
    }

    // Use parquet for SEC adapter, but pass through original engine to FileSchema
    String originalEngine = (String) operand.get("executionEngine");
    mutableOperand.put("executionEngine", "parquet");
    if (originalEngine != null) {
      mutableOperand.put("secExecutionEngine", originalEngine);
    }

    // Now delegate to FileSchemaFactory to create the actual schema
    // with our pre-defined tables and configured directory
    LOGGER.info("Delegating to FileSchemaFactory with modified operand");
    return FileSchemaFactory.INSTANCE.create(parentSchema, name, mutableOperand);
  }


  private boolean shouldDownloadData(Map<String, Object> operand) {
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    Boolean useMockData = (Boolean) operand.get("useMockData");
    boolean result = (autoDownload != null && autoDownload) || (useMockData != null && useMockData);
    System.out.println("DEBUG: shouldDownloadData: autoDownload=" + autoDownload + ", useMockData=" + useMockData + ", result=" + result);
    return result;
  }

  private void addToManifest(File baseDirectory, File xbrlFile) {
    try {
      // Extract CIK and accession from file path
      // Path structure: baseDir/sec-cache/raw/{cik}/{accession}/{file}
      File accessionDir = xbrlFile.getParentFile();
      File cikDir = accessionDir.getParentFile();

      String cik = cikDir.getName();
      String accession = accessionDir.getName();
      // Format accession with dashes
      if (accession.length() == 18) {
        accession = accession.substring(0, 10) + "-" +
                   accession.substring(10, 12) + "-" +
                   accession.substring(12);
      }

      // For now use generic form/date - would need to parse from XBRL to get actual values
      String manifestKey = cik + "|" + accession + "|PROCESSED|" + System.currentTimeMillis();

      File manifestFile = new File(baseDirectory, "processed_filings.manifest");
      synchronized (SecSchemaFactory.class) {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
          pw.println(manifestKey);
        }
        LOGGER.debug("Added to manifest after Parquet conversion: " + manifestKey);
      }
    } catch (Exception e) {
      LOGGER.debug("Could not update manifest: " + e.getMessage());
    }
  }

  private void rebuildManifestFromExistingFiles(File baseDirectory) {
    // Rebuild manifest from existing files on startup
    File manifestFile = new File(baseDirectory, "processed_filings.manifest");
    if (manifestFile.exists()) {
      try {
        long count = Files.lines(manifestFile.toPath()).count();
        LOGGER.info("Manifest already exists with {} entries", count);
        return;
      } catch (Exception e) {
        LOGGER.warn("Could not read manifest: " + e.getMessage());
      }
    }

    LOGGER.info("Building manifest from existing files...");
    Set<String> processedFilings = new HashSet<>();

    File secCacheDir = new File(baseDirectory, "sec-cache");
    if (secCacheDir.exists() && secCacheDir.isDirectory()) {
      File rawDir = new File(secCacheDir, "raw");
      if (rawDir.exists()) {
        // Scan all CIK directories
        File[] cikDirs = rawDir.listFiles(File::isDirectory);
        if (cikDirs != null) {
          for (File cikDir : cikDirs) {
            String cik = cikDir.getName();
            // Scan all accession directories
            File[] accessionDirs = cikDir.listFiles(File::isDirectory);
            if (accessionDirs != null) {
              for (File accessionDir : accessionDirs) {
                String accession = accessionDir.getName();
                // Format accession with dashes
                if (accession.length() == 18) {
                  accession = accession.substring(0, 10) + "-" +
                             accession.substring(10, 12) + "-" +
                             accession.substring(12);
                }

                // Check for HTML files (indicates a filing was downloaded)
                File[] htmlFiles = accessionDir.listFiles((dir, name) -> name.endsWith(".htm") || name.endsWith(".html"));
                if (htmlFiles != null && htmlFiles.length > 0) {
                  // For now, add with generic form/date - actual values would need parsing
                  String manifestKey = cik + "|" + accession + "|UNKNOWN|UNKNOWN";
                  processedFilings.add(manifestKey);
                }
              }
            }
          }
        }
      }
    }

    // Write manifest
    if (!processedFilings.isEmpty()) {
      try (PrintWriter pw = new PrintWriter(manifestFile)) {
        for (String filing : processedFilings) {
          pw.println(filing);
        }
        LOGGER.info("Created manifest with {} existing filings", processedFilings.size());
      } catch (Exception e) {
        LOGGER.warn("Could not create manifest: " + e.getMessage());
      }
    }
  }

  private void downloadSecData(Map<String, Object> operand) {
    System.out.println("DEBUG: downloadSecData() called - STARTING SEC DATA DOWNLOAD");
    LOGGER.info("downloadSecData() called - STARTING SEC DATA DOWNLOAD");
    try {
      // Check if directory is specified in config, otherwise use default
      String configuredDir = (String) operand.get("directory");
      if (configuredDir == null) {
        configuredDir = (String) operand.get("cacheDirectory");
      }
      String cacheHome = configuredDir != null ? configuredDir : SEC_DATA_HOME;

      // XBRL files are immutable - once downloaded, they never change
      // Use configured cache directory
      File baseDir = new File(cacheHome, "sec-data");
      baseDir.mkdirs();

      LOGGER.info("Using SEC cache directory: {}", baseDir.getAbsolutePath());

      // Update operand to use our cache directory
      operand.put("directory", baseDir.getAbsolutePath());

      // Check if we should use mock data instead of downloading
      Boolean useMockData = (Boolean) operand.get("useMockData");
      if (useMockData != null && useMockData) {
        LOGGER.info("Using mock data for testing");
        List<String> ciks = getCiksFromConfig(operand);
        int startYear = (Integer) operand.getOrDefault("startYear", 2020);
        int endYear = (Integer) operand.getOrDefault("endYear", 2023);
        LOGGER.info("DEBUG: Calling createSecTablesFromXbrl for mock data");
        LOGGER.info("DEBUG: baseDir=" + baseDir.getAbsolutePath());
        createSecTablesFromXbrl(baseDir, ciks, startYear, endYear);
        LOGGER.info("DEBUG: Finished createSecTablesFromXbrl for mock data");
        return;
      }

      // Get CIKs to download
      List<String> ciks = getCiksFromConfig(operand);
      LOGGER.debug("CIKs: {}", ciks);
      List<String> filingTypes = getFilingTypes(operand);
      LOGGER.debug("Filing types: {}", filingTypes);
      int startYear = (Integer) operand.getOrDefault("startYear", 2020);
      int endYear = (Integer) operand.getOrDefault("endYear", 2023);
      LOGGER.debug("Year range: {} - {}", startYear, endYear);

      // Use SEC storage provider to download filings
      SecHttpStorageProvider provider = SecHttpStorageProvider.forEdgar();

      for (String cik : ciks) {
        downloadCikFilings(provider, cik, filingTypes, startYear, endYear, baseDir);
      }

      // Wait for all downloads to complete before proceeding to conversion
      if (!downloadFutures.isEmpty()) {
        LOGGER.info("Waiting for " + downloadFutures.size() + " download tasks to complete...");
        CompletableFuture<Void> allDownloads =
            CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0]));
        try {
          allDownloads.get(45, TimeUnit.MINUTES); // Wait up to 45 minutes for downloads
          LOGGER.info("All downloads completed: " + completedDownloads.get() + " files");
        } catch (Exception e) {
          LOGGER.warn("Some downloads may have failed: " + e.getMessage());
        }
      }

      // Create all SEC tables from XBRL data (with parallel conversion)
      LOGGER.info("Creating SEC tables from XBRL data");
      createSecTablesFromXbrl(baseDir, ciks, startYear, endYear);

    } catch (Exception e) {
      LOGGER.error("Error in downloadSecData", e);
      LOGGER.warn("Failed to download SEC data: " + e.getMessage());
    } finally {
      // Shutdown thread pools
      shutdownExecutors();
    }
  }

  private void downloadCikFilings(SecHttpStorageProvider provider, String cik,
      List<String> filingTypes, int startYear, int endYear, File baseDir) {
    try {
      // Normalize CIK
      String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));

      // Create CIK directory in the configured cache location
      // Use the baseDir passed in from downloadSecData
      File cikDir = new File(baseDir, normalizedCik);
      cikDir.mkdirs();

      // Download submissions metadata first (with rate limiting)
      String submissionsUrl =
          String.format("https://data.sec.gov/submissions/CIK%s.json", normalizedCik);
      File submissionsFile = new File(cikDir, "submissions.json");

      // Apply rate limiting
      try {
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get()); // Use dynamic rate limit delay
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      try (InputStream is = provider.openInputStream(submissionsUrl)) {
        try (FileOutputStream fos = new FileOutputStream(submissionsFile)) {
          byte[] buffer = new byte[8192];
          int bytesRead;
          while ((bytesRead = is.read(buffer)) != -1) {
            fos.write(buffer, 0, bytesRead);
          }
        }
        LOGGER.info("Downloaded submissions metadata for CIK " + normalizedCik);
      } finally {
        rateLimiter.release();
      }

      // Parse submissions.json to get filing details
      ObjectMapper mapper = new ObjectMapper();
      JsonNode submissionsJson = mapper.readTree(submissionsFile);
      JsonNode filings = submissionsJson.get("filings");

      if (filings == null || !filings.has("recent")) {
        LOGGER.warn("No recent filings found in submissions for CIK " + normalizedCik);
        return;
      }

      JsonNode recent = filings.get("recent");
      JsonNode accessionNumbers = recent.get("accessionNumber");
      JsonNode filingDates = recent.get("filingDate");
      JsonNode forms = recent.get("form");
      JsonNode primaryDocuments = recent.get("primaryDocument");

      if (accessionNumbers == null || !accessionNumbers.isArray()) {
        LOGGER.warn("No accession numbers found for CIK " + normalizedCik);
        return;
      }

      // Collect all filings to download
      List<FilingToDownload> filingsToDownload = new ArrayList<>();
      for (int i = 0; i < accessionNumbers.size(); i++) {
        String accession = accessionNumbers.get(i).asText();
        String filingDate = filingDates.get(i).asText();
        String form = forms.get(i).asText();
        String primaryDoc = primaryDocuments.get(i).asText();

        // Check if filing type matches and is in date range
        // Normalize form comparison (SEC uses "10K", config uses "10-K")
        String normalizedForm = form.replace("-", "");
        boolean matchesType = filingTypes.stream()
            .anyMatch(type -> type.replace("-", "").equalsIgnoreCase(normalizedForm));
        if (!matchesType) {
          continue;
        }

        int filingYear = Integer.parseInt(filingDate.substring(0, 4));
        if (filingYear < startYear || filingYear > endYear) {
          continue;
        }

        filingsToDownload.add(
            new FilingToDownload(normalizedCik, accession,
            primaryDoc, form, filingDate, cikDir));
      }

      // Download filings in parallel with rate limiting
      totalDownloads.addAndGet(filingsToDownload.size());
      LOGGER.info("Scheduling " + filingsToDownload.size() + " filings for download for CIK " + normalizedCik);

      for (FilingToDownload filing : filingsToDownload) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          downloadFilingDocumentWithRateLimit(provider, filing);
          int completed = completedDownloads.incrementAndGet();
          if (completed % 10 == 0) {
            LOGGER.info("Download progress: " + completed + "/" + totalDownloads.get() + " filings");
          }
        }, downloadExecutor);
        downloadFutures.add(future);
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download filings for CIK " + cik + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Helper class to hold filing download information
  private static class FilingToDownload {
    final String cik;
    final String accession;
    final String primaryDoc;
    final String form;
    final String filingDate;
    final File cikDir;

    FilingToDownload(String cik, String accession, String primaryDoc,
        String form, String filingDate, File cikDir) {
      this.cik = cik;
      this.accession = accession;
      this.primaryDoc = primaryDoc;
      this.form = form;
      this.filingDate = filingDate;
      this.cikDir = cikDir;
    }
  }

  private void downloadFilingDocumentWithRateLimit(SecHttpStorageProvider provider, FilingToDownload filing) {
    int maxAttempts = 3;
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        // Apply rate limiting with current delay
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get());
        try {
          downloadFilingDocument(provider, filing.cik, filing.accession,
              filing.primaryDoc, filing.form, filing.filingDate, filing.cikDir);
          break; // Success - exit retry loop
        } finally {
          rateLimiter.release();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Download interrupted for " + filing.accession);
        break;
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("429")) {
          // Rate limit hit - dynamically expand rate limit delay
          rateLimitHits.incrementAndGet();
          long currentDelay = currentRateLimitDelayMs.get();
          long newDelay = Math.min(currentDelay + 50, MAX_RATE_LIMIT_DELAY_MS); // Increase by 50ms up to max
          currentRateLimitDelayMs.set(newDelay);
          LOGGER.warn("Rate limit hit for " + filing.accession +
              ". Expanding rate limit delay from " + currentDelay + "ms to " + newDelay + "ms. Waiting 10 seconds...");
          try {
            Thread.sleep(10000); // Wait 10 seconds for rate limit
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          attempt++;
        } else {
          LOGGER.warn("Download failed for " + filing.accession + ": " + e.getMessage());
          break;
        }
      }
    }

    if (attempt >= maxAttempts) {
      LOGGER.error("Failed to download " + filing.accession + " after " + maxAttempts + " attempts");
    }
  }

  private void downloadFilingDocument(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) {
    try {
      // Check manifest first to see if this filing was already fully processed
      File manifestFile = new File(cikDir.getParentFile(), "processed_filings.manifest");
      String manifestKey = cik + "|" + accession + "|" + form + "|" + filingDate;

      // Quick check if already in manifest (meaning fully downloaded AND converted to Parquet)
      if (manifestFile.exists()) {
        try {
          Set<String> processedFilings = new HashSet<>(Files.readAllLines(manifestFile.toPath()));
          if (processedFilings.contains(manifestKey)) {
            LOGGER.debug("Filing fully processed (in manifest), skipping: " + form + " " + filingDate);
            return;
          }
        } catch (Exception e) {
          LOGGER.debug("Could not read manifest: " + e.getMessage());
        }
      }

      // Not in manifest - need to do detailed verification

      // Create accession directory
      String accessionClean = accession.replace("-", "");
      File accessionDir = new File(cikDir, accessionClean);
      accessionDir.mkdirs();

      // Download both HTML (for preview) and XBRL (for data extraction)
      boolean needHtml = false;
      boolean needXbrl = false;

      // Check if HTML file exists (for human-readable preview)
      File htmlFile = new File(accessionDir, primaryDoc);
      if (!htmlFile.exists() || htmlFile.length() == 0) {
        needHtml = true;
      }

      // Check if XBRL file exists (for structured data)
      String xbrlDoc = primaryDoc.replace(".htm", "_htm.xml");
      File xbrlFile = new File(accessionDir, xbrlDoc);
      File xbrlNotFoundMarker = new File(accessionDir, xbrlDoc + ".notfound");
      // Only need XBRL if: file doesn't exist AND we haven't already marked it as not found
      if ((!xbrlFile.exists() || xbrlFile.length() == 0) && !xbrlNotFoundMarker.exists()) {
        needXbrl = true;
      }

      // TEMPORARY DEBUG: Also check if Parquet conversion was successful
      boolean needParquetReprocessing = false;
      String year = String.valueOf(java.time.LocalDate.parse(filingDate).getYear());
      File parquetDir = new File(cikDir.getParentFile().getParentFile(), "sec-parquet");
      File cikParquetDir = new File(parquetDir, "cik=" + cik);
      // Need to include filing_type in the path
      File filingTypeDir = new File(cikParquetDir, "filing_type=" + form.replace("-", ""));
      File yearDir = new File(filingTypeDir, "year=" + year);
      // The actual filename uses cik_filingDate pattern, not accession number
      File factsFile = new File(yearDir, String.format("%s_%s_facts.parquet", cik, filingDate));

      if (!factsFile.exists() || factsFile.length() == 0) {
        needParquetReprocessing = true;
        LOGGER.debug("Missing or empty Parquet file: " + factsFile.getPath());
      }

      if (!needHtml && !needXbrl && !needParquetReprocessing) {
        LOGGER.info("Filing already fully cached: " + form + " " + filingDate);
        return; // Both files already downloaded and converted
      }

      if (needParquetReprocessing) {
        LOGGER.info("Parquet reprocessing needed for: " + form + " " + filingDate);
        // Force reprocessing by treating as if we need XBRL (even if files exist)
        // This ensures the conversion will run again
        needXbrl = true;

        // If XBRL file already exists, we need to schedule it for conversion
        // since it won't be downloaded again
        if (xbrlFile.exists() && xbrlFile.length() > 0) {
          LOGGER.info("Scheduling existing XBRL file for re-conversion: " + xbrlFile.getName());
          // Add to a list that will be processed later
          if (!scheduledForReconversion.contains(xbrlFile)) {
            scheduledForReconversion.add(xbrlFile);
          }
        }
      }

      // Download HTML file first if needed (for preview and iXBRL check)
      if (needHtml) {
        String htmlUrl =
            String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
            cik, accessionClean, primaryDoc);

        try (InputStream is = provider.openInputStream(htmlUrl)) {
          try (FileOutputStream fos = new FileOutputStream(htmlFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
              fos.write(buffer, 0, bytesRead);
            }
          }
          LOGGER.info("Downloaded HTML filing: " + form + " " + filingDate + " (" + primaryDoc + ")");
        } catch (Exception e) {
          LOGGER.info("Could not download HTML: " + e.getMessage());
          return; // Can't proceed without HTML
        }
      }

      // Check if HTML contains inline XBRL (iXBRL) - if it does, we don't need separate XBRL
      boolean hasInlineXbrl = false;
      if (htmlFile.exists()) {
        try {
          // Quick check for iXBRL markers in the first 10KB of the HTML
          byte[] headerBytes = new byte[10240];
          try (FileInputStream fis = new FileInputStream(htmlFile)) {
            int bytesRead = fis.read(headerBytes);
            if (bytesRead > 0) {
              String header = new String(headerBytes, 0, bytesRead);
              hasInlineXbrl = header.contains("xmlns:ix=") ||
                             header.contains("http://www.xbrl.org/2013/inlineXBRL") ||
                             header.contains("<ix:") ||
                             header.contains("iXBRL");
              if (hasInlineXbrl) {
                LOGGER.info("HTML file contains inline XBRL (iXBRL), skipping separate XBRL download: " + primaryDoc);
                // Create marker to avoid checking XBRL in future
                if (!xbrlNotFoundMarker.exists()) {
                  xbrlNotFoundMarker.createNewFile();
                }
                return; // No need to download separate XBRL
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not check HTML for iXBRL: " + e.getMessage());
        }
      }

      // Download XBRL file if needed (only if HTML doesn't have iXBRL)
      if (needXbrl && !hasInlineXbrl) {
        // Check if we already know this XBRL doesn't exist
        if (xbrlNotFoundMarker.exists()) {
          LOGGER.debug("Skipping XBRL download - already marked as not found: " + xbrlDoc);
        } else {
          String xbrlUrl =
              String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
              cik, accessionClean, xbrlDoc);

          try (InputStream is = provider.openInputStream(xbrlUrl)) {
            try (FileOutputStream fos = new FileOutputStream(xbrlFile)) {
              byte[] buffer = new byte[8192];
              int bytesRead;
              while ((bytesRead = is.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
              }
            }
            LOGGER.info("Downloaded XBRL filing: " + form + " " + filingDate + " (" + xbrlDoc + ")");
          } catch (Exception e) {
            // XBRL doesn't exist for this filing - create marker to avoid retrying
            LOGGER.info("XBRL not available for " + form + " " + filingDate + ", will use HTML with inline XBRL");
            try {
              xbrlNotFoundMarker.createNewFile();
              LOGGER.debug("Created .notfound marker for: " + xbrlDoc);
            } catch (IOException ioe) {
              LOGGER.debug("Could not create .notfound marker: " + ioe.getMessage());
            }
          }
        }
      }

      // Don't add to manifest here - only add after Parquet conversion is complete
      // The manifest should only contain fully processed filings (downloaded + converted)
      LOGGER.debug("Filing downloaded, but not adding to manifest until Parquet conversion complete");

    } catch (Exception e) {
      LOGGER.warn("Failed to download filing " + accession + ": " + e.getMessage());
    }
  }

  private void downloadInlineXbrl(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) throws Exception {
    // Some filings use inline XBRL embedded in HTML
    // Try to download the HTML and extract XBRL data
    String accessionClean = accession.replace("-", "");
    File accessionDir = new File(cikDir, accessionClean);

    File outputFile = new File(accessionDir, primaryDoc);

    // Check if file already exists - XBRL files are immutable
    if (outputFile.exists() && outputFile.length() > 0) {
      LOGGER.info("Inline XBRL filing already cached: " + form + " " + filingDate + " (" + primaryDoc + ")");
      return;
    }

    String htmlUrl =
        String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
        cik, accessionClean, primaryDoc);
    try (InputStream is = provider.openInputStream(htmlUrl)) {
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1) {
          fos.write(buffer, 0, bytesRead);
        }
      }
    }

    LOGGER.info("Downloaded inline XBRL (HTML): " + form + " " + filingDate + " (" + primaryDoc + ")");
  }

  private void createSecTablesFromXbrl(File baseDir, List<String> ciks, int startYear, int endYear) {
    System.out.println("DEBUG: createSecTablesFromXbrl START");
    LOGGER.info("DEBUG: createSecTablesFromXbrl START");
    LOGGER.info("DEBUG: baseDir=" + baseDir.getAbsolutePath());
    LOGGER.info("DEBUG: ciks.size()=" + ciks.size());
    LOGGER.info("DEBUG: startYear=" + startYear + ", endYear=" + endYear);

    try {
      // baseDir is already the sec-data directory, don't nest another level
      File secRawDir = baseDir;
      // Parquet directory is a sibling of sec-data
      File secParquetDir = new File(baseDir.getParentFile(), "sec-parquet");
      secParquetDir.mkdirs();

      LOGGER.info("DEBUG: secRawDir=" + secRawDir.getAbsolutePath() + " exists=" + secRawDir.exists());
      LOGGER.info("DEBUG: secParquetDir=" + secParquetDir.getAbsolutePath() + " exists=" + secParquetDir.exists());
      LOGGER.info("Processing XBRL files from " + secRawDir + " to create Parquet tables");

      // Collect all XBRL files to convert
      List<File> xbrlFilesToConvert = new ArrayList<>();

      // Process all downloaded XBRL files
      for (String cik : ciks) {
        String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));
        File cikDir = new File(secRawDir, normalizedCik);

        if (!cikDir.exists()) {
          LOGGER.info("No data directory found for CIK " + normalizedCik);
          continue;
        }

        // Process each filing in the CIK directory
        File[] accessionDirs = cikDir.listFiles(File::isDirectory);
        if (accessionDirs != null) {
          for (File accessionDir : accessionDirs) {
            // Find XBRL and HTML files in this accession directory
            File[] xbrlFiles = accessionDir.listFiles((dir, name) ->
                name.endsWith("_htm.xml") || name.endsWith(".xml") ||
                name.endsWith(".htm") || name.endsWith(".html"));

            if (xbrlFiles != null) {
              for (File xbrlFile : xbrlFiles) {
                xbrlFilesToConvert.add(xbrlFile);
              }
            }
          }
        }
      }

      // Add any files scheduled for reconversion (missing Parquet files)
      if (!scheduledForReconversion.isEmpty()) {
        LOGGER.info("Adding " + scheduledForReconversion.size() + " existing XBRL files for reconversion");
        xbrlFilesToConvert.addAll(scheduledForReconversion);
        scheduledForReconversion.clear();
      }

      // Convert XBRL files in parallel
      totalConversions.set(xbrlFilesToConvert.size());
      LOGGER.info("DEBUG: Found " + xbrlFilesToConvert.size() + " XBRL files to convert");
      LOGGER.info("Scheduling " + xbrlFilesToConvert.size() + " XBRL files for conversion");

      for (File xbrlFile : xbrlFilesToConvert) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            XbrlToParquetConverter converter = new XbrlToParquetConverter();
            List<File> outputFiles = converter.convert(xbrlFile, secParquetDir, null);
            LOGGER.info("Converted " + xbrlFile.getName() + " to " +
                outputFiles.size() + " Parquet files");

            // Add to manifest after successful conversion
            addToManifest(xbrlFile.getParentFile().getParentFile().getParentFile(), xbrlFile);
          } catch (Exception e) {
            LOGGER.info("Failed to convert " + xbrlFile.getName() + ": " + e.getMessage());
          }
          int completed = completedConversions.incrementAndGet();
          if (completed % 10 == 0) {
            LOGGER.info("Conversion progress: " + completed + "/" + totalConversions.get() + " files");
          }
        }, conversionExecutor);
        conversionFutures.add(future);
      }

      // Wait for all conversions to complete
      CompletableFuture<Void> allConversions =
          CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0]));
      allConversions.get(30, TimeUnit.MINUTES); // Wait up to 30 minutes

      LOGGER.info("Processed " + completedConversions.get() + " XBRL files into Parquet tables in: " + secParquetDir);

      LOGGER.info("DEBUG: Checking what was created in secParquetDir after conversion");
      File[] afterConversion = secParquetDir.listFiles();
      if (afterConversion != null) {
        LOGGER.info("DEBUG: Found " + afterConversion.length + " items in secParquetDir");
        for (File f : afterConversion) {
          LOGGER.info("DEBUG: - " + f.getName() + " (isDir=" + f.isDirectory() + ", size=" + f.length() + ")");
        }
      }

    } catch (Exception e) {
      LOGGER.error("Failed to create SEC tables from XBRL", e);
      throw new RuntimeException("Failed to create SEC tables", e);
    }
  }


  // REMOVED: consolidateFactsIntoFinancialLineItems method
  // The FileSchema will automatically discover individual *_facts.parquet files
  // No consolidation needed - each filing remains in its own partition











  private void createSecFilingsTable(File baseDir, Map<String, Object> operand) {
    try {
      File secRawDir = new File(baseDir, "sec-raw");
      File secParquetDir = new File(baseDir, "sec-parquet");
      secParquetDir.mkdirs();

      if (!secRawDir.exists() || !secRawDir.isDirectory()) {
        LOGGER.warn("No sec-raw directory found: " + secRawDir);
        return;
      }

      // Create Avro schema for SEC filings metadata
      org.apache.avro.Schema schema = SchemaBuilder.record("SecFiling")
          .fields()
          .requiredString("cik")
          .requiredString("accession_number")
          .requiredString("filing_type")
          .requiredString("filing_date")
          .optionalString("primary_document")
          .optionalString("company_name")
          .optionalString("period_of_report")
          .optionalString("acceptance_datetime")
          .optionalLong("file_size")
          .requiredInt("fiscal_year")
          .endRecord();

      List<GenericRecord> allRecords = new ArrayList<>();
      ObjectMapper mapper = new ObjectMapper();

      // Process each CIK directory
      File[] cikDirs = secRawDir.listFiles(File::isDirectory);
      if (cikDirs != null) {
        for (File cikDir : cikDirs) {
          File submissionsFile = new File(cikDir, "submissions.json");
          if (!submissionsFile.exists()) {
            continue;
          }

          try {
            JsonNode submissionsJson = mapper.readTree(submissionsFile);
            String cik = cikDir.getName();
            String companyName = submissionsJson.path("name").asText("");

            JsonNode filings = submissionsJson.get("filings");
            if (filings == null || !filings.has("recent")) {
              continue;
            }

            JsonNode recent = filings.get("recent");
            JsonNode accessionNumbers = recent.get("accessionNumber");
            JsonNode filingDates = recent.get("filingDate");
            JsonNode forms = recent.get("form");
            JsonNode primaryDocuments = recent.get("primaryDocument");
            JsonNode periodsOfReport = recent.get("reportDate");
            JsonNode acceptanceDatetimes = recent.get("acceptanceDateTime");
            JsonNode fileSizes = recent.get("size");

            if (accessionNumbers == null || !accessionNumbers.isArray()) {
              continue;
            }

            for (int i = 0; i < accessionNumbers.size(); i++) {
              GenericRecord record = new GenericData.Record(schema);
              record.put("cik", cik);
              record.put("accession_number", accessionNumbers.get(i).asText());
              record.put("filing_type", forms.get(i).asText());
              record.put("filing_date", filingDates.get(i).asText());
              record.put("primary_document", primaryDocuments.get(i).asText(""));
              record.put("company_name", companyName);
              record.put("period_of_report", periodsOfReport != null && i < periodsOfReport.size()
                  ? periodsOfReport.get(i).asText("") : "");
              record.put("acceptance_datetime", acceptanceDatetimes != null && i < acceptanceDatetimes.size()
                  ? acceptanceDatetimes.get(i).asText("") : "");
              record.put("file_size", fileSizes != null && i < fileSizes.size()
                  ? fileSizes.get(i).asLong(0L) : 0L);

              String filingDate = filingDates.get(i).asText();
              int fiscalYear = filingDate.length() >= 4 ?
                  Integer.parseInt(filingDate.substring(0, 4)) : 0;
              record.put("fiscal_year", fiscalYear);

              allRecords.add(record);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to process submissions for CIK " + cikDir.getName() + ": " + e.getMessage());
          }
        }
      }

      // Write consolidated SEC filings table
      File outputFile = new File(secParquetDir, "sec_filings.parquet");
      @SuppressWarnings("deprecation")
      ParquetWriter<GenericRecord> writer = AvroParquetWriter
          .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.toURI()))
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build();

      try {
        for (GenericRecord record : allRecords) {
          writer.write(record);
        }
      } finally {
        writer.close();
      }

      LOGGER.info("Created SEC filings table with " + allRecords.size() + " records: " + outputFile);

    } catch (Exception e) {
      LOGGER.warn("Failed to create SEC filings table: " + e.getMessage());
      e.printStackTrace();
    }
  }


  private List<File> findXbrlFiles(File directory) {
    List<File> xbrlFiles = new ArrayList<>();
    File[] files = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          xbrlFiles.addAll(findXbrlFiles(file));
        } else if (file.getName().endsWith(".xml") || file.getName().endsWith(".xbrl")) {
          xbrlFiles.add(file);
        } else if (file.getName().endsWith(".htm") || file.getName().endsWith(".html")) {
          // Include HTML files - XbrlToParquetConverter can handle inline XBRL
          xbrlFiles.add(file);
        }
      }
    }

    return xbrlFiles;
  }

  private List<String> getCiksFromConfig(Map<String, Object> operand) {
    Object ciks = operand.get("ciks");
    if (ciks instanceof List) {
      List<String> cikList = new ArrayList<>();
      for (Object cik : (List<?>) ciks) {
        cikList.addAll(CikRegistry.resolveCiks(cik.toString()));
      }
      return cikList;
    } else if (ciks instanceof String) {
      return CikRegistry.resolveCiks((String) ciks);
    }
    return new ArrayList<>();
  }

  private List<String> getFilingTypes(Map<String, Object> operand) {
    Object types = operand.get("filingTypes");
    if (types instanceof List) {
      return (List<String>) types;
    }
    // Return empty list to signal "download all filing types"
    // EdgarDownloader will interpret empty list as "all types"
    return new ArrayList<>();
  }
}
