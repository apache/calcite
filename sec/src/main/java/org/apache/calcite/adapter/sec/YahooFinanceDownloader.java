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

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Downloads historical stock price data from Yahoo Finance and stores it in Parquet format.
 * Implements rate limiting and caching to avoid redundant downloads.
 */
public class YahooFinanceDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(YahooFinanceDownloader.class);
  
  private static final String YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v7/finance/download/";
  private static final String USER_AGENT = "Apache Calcite SEC Adapter (apache-calcite@apache.org)";
  
  // Rate limiting configuration
  private static final long DEFAULT_RATE_LIMIT_MS = 500;
  private static final long MAX_RATE_LIMIT_MS = 5000;
  private static final int MAX_PARALLEL_DOWNLOADS = 3;
  private static final int MAX_RETRY_ATTEMPTS = 3;
  
  private final AtomicLong currentRateLimitMs = new AtomicLong(DEFAULT_RATE_LIMIT_MS);
  private final AtomicInteger rateLimitHits = new AtomicInteger(0);
  private final Semaphore rateLimiter = new Semaphore(1);
  private final ExecutorService downloadExecutor = Executors.newFixedThreadPool(MAX_PARALLEL_DOWNLOADS);
  
  // Avro schema for stock price records
  // Note: ticker and year are partition columns from directory structure,
  // so they are NOT included in the Parquet file. CIK is included as a regular column for joins.
  private static final String STOCK_PRICE_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"StockPrice\","
      + "\"fields\": ["
      + "{\"name\": \"cik\", \"type\": \"string\"},"
      + "{\"name\": \"date\", \"type\": \"string\"},"
      + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"adj_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null}"
      + "]"
      + "}";
  
  private final Schema priceSchema = new Schema.Parser().parse(STOCK_PRICE_SCHEMA);
  
  /**
   * Downloads stock prices for multiple tickers and CIKs.
   */
  public void downloadStockPrices(File baseDir, List<TickerCikPair> tickerCikPairs, 
      int startYear, int endYear) {
    LOGGER.info("Starting stock price downloads for {} tickers from {} to {}", 
        tickerCikPairs.size(), startYear, endYear);
    
    File stockPricesDir = new File(baseDir, "stock_prices");
    stockPricesDir.mkdirs();
    
    File manifestFile = new File(stockPricesDir, "downloaded.manifest");
    Map<String, Long> manifestData = loadManifestWithTimestamps(manifestFile);
    
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger completedCount = new AtomicInteger(0);
    int totalDownloads = tickerCikPairs.size() * (endYear - startYear + 1);
    int currentYear = LocalDate.now().getYear();
    long todayStartMillis = LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    
    for (TickerCikPair pair : tickerCikPairs) {
      for (int year = startYear; year <= endYear; year++) {
        String manifestKey = pair.ticker + "|" + year;
        
        // Check if we should skip this download
        if (manifestData.containsKey(manifestKey)) {
          long downloadTimestamp = manifestData.get(manifestKey);
          
          if (year < currentYear) {
            // Past years: never re-download (data is complete)
            LOGGER.debug("Skipping completed year: {} for {}", year, pair.ticker);
            completedCount.incrementAndGet();
            continue;
          } else if (year == currentYear) {
            // Current year: only re-download if last download was on a previous day
            if (downloadTimestamp >= todayStartMillis) {
              LOGGER.debug("Skipping current year {} for {} - already downloaded today", year, pair.ticker);
              completedCount.incrementAndGet();
              continue;
            } else {
              LOGGER.info("Re-downloading current year {} for {} - data may be stale", year, pair.ticker);
            }
          }
        }
        
        final int downloadYear = year;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          downloadTickerYear(stockPricesDir, pair, downloadYear, manifestFile);
          int completed = completedCount.incrementAndGet();
          if (completed % 10 == 0) {
            LOGGER.info("Stock price download progress: {}/{}", completed, totalDownloads);
          }
        }, downloadExecutor);
        futures.add(future);
      }
    }
    
    // Wait for all downloads to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    downloadExecutor.shutdown();
    
    LOGGER.info("Completed stock price downloads. Rate limit hits: {}", rateLimitHits.get());
  }
  
  /**
   * Downloads stock prices for a specific ticker and year.
   */
  private void downloadTickerYear(File stockPricesDir, TickerCikPair pair, int year, 
      File manifestFile) {
    LocalDate startDate = LocalDate.of(year, 1, 1);
    LocalDate endDate = LocalDate.of(year, 12, 31);
    
    // Create partitioned directory structure: ticker=*/year=*/
    File tickerDir = new File(stockPricesDir, "ticker=" + pair.ticker);
    File yearDir = new File(tickerDir, "year=" + year);
    yearDir.mkdirs();
    
    File parquetFile = new File(yearDir, pair.ticker + "_" + year + "_prices.parquet");
    
    // Try download with retries
    int attempt = 0;
    while (attempt < MAX_RETRY_ATTEMPTS) {
      try {
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitMs.get());
        
        try {
          List<StockPriceRecord> prices = fetchYahooData(pair.ticker, startDate, endDate);
          if (!prices.isEmpty()) {
            writeParquetFile(parquetFile, prices, pair.ticker, pair.cik, year);
            updateManifest(manifestFile, pair.ticker, year);
            LOGGER.debug("Downloaded {} price records for {} in {}", 
                prices.size(), pair.ticker, year);
          } else {
            LOGGER.warn("No price data available for {} in {}", pair.ticker, year);
          }
          break; // Success
        } finally {
          rateLimiter.release();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Download interrupted for {} year {}", pair.ticker, year);
        break;
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("429")) {
          handleRateLimitHit();
          attempt++;
        } else if (e.getMessage() != null && e.getMessage().contains("404")) {
          LOGGER.debug("Ticker {} not found or delisted, skipping", pair.ticker);
          break;
        } else {
          LOGGER.warn("Failed to download {} for year {}: {}", 
              pair.ticker, year, e.getMessage());
          attempt++;
          if (attempt < MAX_RETRY_ATTEMPTS) {
            try {
              Thread.sleep(1000 * attempt); // Exponential backoff
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }
    }
  }
  
  /**
   * Fetches stock price data from Yahoo Finance.
   */
  private List<StockPriceRecord> fetchYahooData(String ticker, LocalDate startDate, 
      LocalDate endDate) throws IOException {
    try {
    long period1 = startDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
    long period2 = endDate.atTime(23, 59, 59).toEpochSecond(ZoneOffset.UTC);
    
    String urlString = String.format("%s%s?period1=%d&period2=%d&interval=1d&events=history",
        YAHOO_BASE_URL, ticker, period1, period2);
    
    URL url = new java.net.URI(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(10000);
    
    int responseCode = conn.getResponseCode();
    if (responseCode == 429) {
      throw new IOException("429 Rate limit hit");
    } else if (responseCode == 404) {
      throw new IOException("404 Ticker not found");
    } else if (responseCode != 200) {
      throw new IOException("HTTP error code: " + responseCode);
    }
    
    List<StockPriceRecord> prices = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String line = reader.readLine(); // Skip header
      while ((line = reader.readLine()) != null) {
        StockPriceRecord record = parseCSVLine(line);
        if (record != null) {
          prices.add(record);
        }
      }
    }
    
    return prices;
    } catch (java.net.URISyntaxException e) {
      throw new IOException("Invalid URL: " + e.getMessage(), e);
    }
  }
  
  /**
   * Parses a CSV line from Yahoo Finance.
   * Format: Date,Open,High,Low,Close,Adj Close,Volume
   */
  private StockPriceRecord parseCSVLine(String line) {
    try {
      String[] parts = line.split(",");
      if (parts.length < 7) return null;
      
      StockPriceRecord record = new StockPriceRecord();
      record.date = parts[0];
      record.open = parseDouble(parts[1]);
      record.high = parseDouble(parts[2]);
      record.low = parseDouble(parts[3]);
      record.close = parseDouble(parts[4]);
      record.adjClose = parseDouble(parts[5]);
      record.volume = parseLong(parts[6]);
      
      return record;
    } catch (Exception e) {
      LOGGER.debug("Failed to parse CSV line: {}", line);
      return null;
    }
  }
  
  private Double parseDouble(String value) {
    if (value == null || value.equals("null") || value.isEmpty()) {
      return null;
    }
    return Double.parseDouble(value);
  }
  
  private Long parseLong(String value) {
    if (value == null || value.equals("null") || value.isEmpty()) {
      return null;
    }
    return Long.parseLong(value);
  }
  
  /**
   * Writes stock price data to a Parquet file.
   */
  @SuppressWarnings("deprecation")
  private void writeParquetFile(File parquetFile, List<StockPriceRecord> prices, 
      String ticker, String cik, int year) throws IOException {
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new Path(parquetFile.getAbsolutePath()))
        .withSchema(priceSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (StockPriceRecord price : prices) {
        GenericRecord record = new GenericData.Record(priceSchema);
        // Include CIK as regular column; ticker and year come from directory structure
        record.put("cik", cik);
        record.put("date", price.date);
        record.put("open", price.open);
        record.put("high", price.high);
        record.put("low", price.low);
        record.put("close", price.close);
        record.put("adj_close", price.adjClose);
        record.put("volume", price.volume);
        
        writer.write(record);
      }
    }
  }
  
  /**
   * Handles rate limit hits by increasing the delay.
   */
  private void handleRateLimitHit() {
    rateLimitHits.incrementAndGet();
    long currentDelay = currentRateLimitMs.get();
    long newDelay = Math.min(currentDelay * 2, MAX_RATE_LIMIT_MS);
    currentRateLimitMs.set(newDelay);
    LOGGER.warn("Rate limit hit. Increasing delay from {}ms to {}ms", currentDelay, newDelay);
    
    try {
      Thread.sleep(10000); // Wait 10 seconds after rate limit
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Loads the manifest of already downloaded ticker-year combinations.
   * Format: ticker|year or ticker|year|timestamp for current year entries
   */
  private Map<String, Long> loadManifestWithTimestamps(File manifestFile) {
    Map<String, Long> downloaded = new HashMap<>();
    if (manifestFile.exists()) {
      try {
        for (String line : Files.readAllLines(manifestFile.toPath())) {
          String[] parts = line.split("\\|");
          if (parts.length >= 2) {
            String key = parts[0] + "|" + parts[1];
            long timestamp = parts.length >= 3 ? Long.parseLong(parts[2]) : 0L;
            downloaded.put(key, timestamp);
          }
        }
      } catch (IOException | NumberFormatException e) {
        LOGGER.warn("Failed to load manifest: {}", e.getMessage());
      }
    }
    return downloaded;
  }
  
  /**
   * Updates the manifest with a newly downloaded ticker-year.
   * For current year, includes timestamp for staleness checking.
   */
  private synchronized void updateManifest(File manifestFile, String ticker, int year) {
    try {
      int currentYear = LocalDate.now().getYear();
      String entry;
      if (year == currentYear) {
        // For current year, include timestamp for staleness checking
        entry = ticker + "|" + year + "|" + System.currentTimeMillis();
      } else {
        // For past years, no timestamp needed (data is complete)
        entry = ticker + "|" + year;
      }
      Files.write(manifestFile.toPath(), 
          (entry + System.lineSeparator()).getBytes(),
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);
    } catch (IOException e) {
      LOGGER.warn("Failed to update manifest: {}", e.getMessage());
    }
  }
  
  /**
   * Simple holder for ticker-CIK pairs.
   */
  public static class TickerCikPair {
    public final String ticker;
    public final String cik;
    
    public TickerCikPair(String ticker, String cik) {
      this.ticker = ticker;
      this.cik = cik;
    }
  }
  
  /**
   * Internal class for stock price records.
   */
  private static class StockPriceRecord {
    String date;
    Double open;
    Double high;
    Double low;
    Double close;
    Double adjClose;
    Long volume;
  }
}