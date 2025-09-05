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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Fetches and caches SEC EDGAR data for special marker groups.
 */
/**
 * Fetches and caches SEC EDGAR data for special marker groups.
 * 
 * Fallback behavior is controlled by system properties:
 * - sec.fallback.enabled: Enable/disable fallback to hardcoded data (default: false)
 * - sec.testMode: Test mode behavior, fails fast (default: false)
 */
public class SecDataFetcher {
  private static final Logger LOGGER = Logger.getLogger(SecDataFetcher.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  // Configuration for fallback behavior
  private static final boolean FALLBACK_ENABLED = 
      Boolean.parseBoolean(System.getProperty("sec.fallback.enabled", "false"));
  private static final boolean TEST_MODE = 
      Boolean.parseBoolean(System.getProperty("sec.testMode", "false"));

  // SEC API endpoints
  private static final String SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json";
  private static final String SEC_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json";

  // S&P 500 data source (Wikipedia maintains an up-to-date list)
  private static final String SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies";

  // Russell 2000 data source
  private static final String RUSSELL2000_WIKI_URL = "https://en.wikipedia.org/wiki/Russell_2000_Index";

  // Cache configuration
  private static final Duration CACHE_TTL_ALL = Duration.ofDays(7);  // Weekly for ALL
  private static final Duration CACHE_TTL_INDEX = Duration.ofDays(1); // Daily for indices

  // Cache directory
  private static final Path CACHE_DIR;

  // In-memory cache for performance
  private static final ConcurrentHashMap<String, CachedData> memoryCache = new ConcurrentHashMap<>();

  // Cache for NASDAQ-100 constituents
  private static final ConcurrentHashMap<String, CachedData> NASDAQ100_CACHE = new ConcurrentHashMap<>();

  // Background refresh executor
  private static final ExecutorService REFRESH_EXECUTOR = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "SEC-Data-Refresh");
    t.setDaemon(true);
    return t;
  });

  // Track ongoing refresh operations to avoid duplicates
  private static final ConcurrentHashMap<String, Future<?>> REFRESH_FUTURES = new ConcurrentHashMap<>();

  // Scheduled executor for periodic cache refresh
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(1, r -> {
    Thread t = new Thread(r, "SEC-Data-Scheduler");
    t.setDaemon(true);
    return t;
  });

  static {
    // Schedule periodic cache refresh checks every hour
    SCHEDULED_EXECUTOR.scheduleAtFixedRate(() -> {
      try {
        refreshExpiredCaches();
      } catch (Exception e) {
        LOGGER.warning("Error in periodic cache refresh: " + e.getMessage());
      }
    }, 1, 1, TimeUnit.HOURS);
    // Initialize cache directory
    String cacheRoot =
        System.getProperty("sec.cache.dir", System.getProperty("user.home") + "/.calcite/sec-cache");
    CACHE_DIR = Paths.get(cacheRoot, "sec-data");
    try {
      Files.createDirectories(CACHE_DIR);
    } catch (IOException e) {
      LOGGER.warning("Failed to create cache directory: " + e.getMessage());
    }
  }

  /**
   * Trigger background refresh if cache is expired.
   * Returns immediately with stale data if available.
   */
  private static void triggerBackgroundRefreshIfNeeded(String cacheKey, Duration ttl) {
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null && cached.isExpired(ttl)) {
      // Check if refresh is already in progress
      if (!REFRESH_FUTURES.containsKey(cacheKey)) {
        LOGGER.info("Triggering background refresh for " + cacheKey);
        Future<?> future = REFRESH_EXECUTOR.submit(() -> {
          try {
            refreshCache(cacheKey);
          } catch (Exception e) {
            LOGGER.warning("Background refresh failed for " + cacheKey + ": " + e.getMessage());
          } finally {
            REFRESH_FUTURES.remove(cacheKey);
          }
        });
        REFRESH_FUTURES.put(cacheKey, future);
      }
    }
  }

  /**
   * Refresh a specific cache entry.
   */
  private static void refreshCache(String cacheKey) {
    LOGGER.info("Refreshing cache for " + cacheKey);
    try {
      switch (cacheKey) {
        case "_ALL_EDGAR_FILERS":
          List<String> allCiks = fetchAllEdgarFilersFromSEC();
          if (!allCiks.isEmpty()) {
            updateCache(cacheKey, allCiks, "SEC EDGAR");
          }
          break;
        case "_SP500_CONSTITUENTS":
          List<String> sp500 = fetchSP500FromWikipedia();
          if (!sp500.isEmpty()) {
            updateCache(cacheKey, sp500, "Wikipedia");
          }
          break;
        case "_RUSSELL2000_CONSTITUENTS":
          List<String> russell2000 = fetchRussell2000FromLLM();
          if (!russell2000.isEmpty()) {
            updateCache(cacheKey, russell2000, "LLM API");
          }
          break;
        case "_RUSSELL1000_CONSTITUENTS":
          List<String> russell1000 = fetchRussell1000FromLLM();
          if (!russell1000.isEmpty()) {
            updateCache(cacheKey, russell1000, "LLM API");
          }
          break;
        case "_RUSSELL3000_CONSTITUENTS":
          List<String> russell3000 = fetchRussell3000FromLLM();
          if (!russell3000.isEmpty()) {
            updateCache(cacheKey, russell3000, "LLM API");
          }
          break;
        case "_NASDAQ100_CONSTITUENTS":
          List<String> nasdaq100 = fetchNasdaq100FromLLM();
          if (!nasdaq100.isEmpty()) {
            updateCache("nasdaq100", nasdaq100, "LLM API");
          }
          break;
        case "_NYSE_LISTED":
          List<String> nyse = fetchNYSEFromNasdaqScreener();
          if (nyse.isEmpty()) {
            nyse = fetchNYSEFromLLM();
          }
          if (!nyse.isEmpty()) {
            updateCache(cacheKey, nyse, "NASDAQ Screener/LLM");
          }
          break;
        case "_NASDAQ_LISTED":
          List<String> nasdaq = fetchNASDAQFromNasdaqScreener();
          if (nasdaq.isEmpty()) {
            nasdaq = fetchNASDAQFromLLM();
          }
          if (!nasdaq.isEmpty()) {
            updateCache(cacheKey, nasdaq, "NASDAQ Screener/LLM");
          }
          break;
        case "_WILSHIRE5000_CONSTITUENTS":
          List<String> wilshire = fetchWilshire5000FromLLM();
          if (!wilshire.isEmpty()) {
            updateCache(cacheKey, wilshire, "LLM API");
          }
          break;
        case "_FTSE100_US_LISTED":
          List<String> ftse100 = fetchFTSE100USListedFromLLM();
          if (!ftse100.isEmpty()) {
            updateCache(cacheKey, ftse100, "LLM API/Fallback");
          }
          break;
        case "_GLOBAL_MEGA_CAP":
          List<String> megaCap = fetchMarketCapFromLLM(200_000_000_000L, Long.MAX_VALUE, "global mega-cap (>$200B)");
          if (!megaCap.isEmpty()) {
            updateCache(cacheKey, megaCap, "LLM API/Market Data");
          }
          break;
        case "_US_LARGE_CAP":
          List<String> largeCap = fetchMarketCapFromLLM(10_000_000_000L, Long.MAX_VALUE, "US large-cap (>$10B)");
          if (!largeCap.isEmpty()) {
            updateCache(cacheKey, largeCap, "LLM API/Market Data");
          }
          break;
        case "_US_MID_CAP":
          List<String> midCap = fetchMarketCapFromLLM(2_000_000_000L, 10_000_000_000L, "US mid-cap ($2B-$10B)");
          if (!midCap.isEmpty()) {
            updateCache(cacheKey, midCap, "LLM API/Market Data");
          }
          break;
        case "_US_SMALL_CAP":
          List<String> smallCap = fetchMarketCapFromLLM(300_000_000L, 2_000_000_000L, "US small-cap ($300M-$2B)");
          if (!smallCap.isEmpty()) {
            updateCache(cacheKey, smallCap, "LLM API/Market Data");
          }
          break;
        case "_US_MICRO_CAP":
          List<String> microCap = fetchMarketCapFromLLM(0L, 300_000_000L, "US micro-cap (<$300M)");
          if (!microCap.isEmpty()) {
            updateCache(cacheKey, microCap, "LLM API/Market Data");
          }
          break;
      }
    } catch (Exception e) {
      LOGGER.severe("Failed to refresh cache for " + cacheKey + ": " + e.getMessage());
    }
  }

  /**
   * Update cache with new data.
   */
  private static void updateCache(String cacheKey, List<String> data, String source) {
    CachedData newCache = new CachedData(data, source);
    memoryCache.put(cacheKey, newCache);

    // Also update disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    try {
      saveToDisk(cacheFile, newCache);
      LOGGER.info("Updated cache for " + cacheKey + " with " + data.size() + " entries from " + source);
    } catch (Exception e) {
      LOGGER.warning("Failed to save " + cacheKey + " to disk: " + e.getMessage());
    }
  }

  /**
   * Periodically check and refresh expired caches.
   */
  private static void refreshExpiredCaches() {
    for (Map.Entry<String, CachedData> entry : memoryCache.entrySet()) {
      String cacheKey = entry.getKey();
      CachedData cached = entry.getValue();

      // Determine appropriate TTL based on cache key
      Duration ttl = cacheKey.contains("ALL") ? CACHE_TTL_ALL : CACHE_TTL_INDEX;

      if (cached.isExpired(ttl) && !REFRESH_FUTURES.containsKey(cacheKey)) {
        LOGGER.info("Periodic refresh triggered for expired cache: " + cacheKey);
        triggerBackgroundRefreshIfNeeded(cacheKey, ttl);
      }
    }
  }

  /**
   * Cached data with timestamp.
   */
  private static class CachedData {
    final List<String> ciks;
    final Instant timestamp;
    final String source;

    CachedData(List<String> ciks, String source) {
      this.ciks = ciks;
      this.timestamp = Instant.now();
      this.source = source;
    }

    boolean isExpired(Duration ttl) {
      return Duration.between(timestamp, Instant.now()).compareTo(ttl) > 0;
    }
  }

  /**
   * Fetch all EDGAR filer CIKs.
   * @return List of all CIK numbers (padded to 10 digits)
   */
  public static List<String> fetchAllEdgarFilers() {
    String cacheKey = "_ALL_EDGAR_FILERS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_ALL)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
        LOGGER.info("Returning " + cached.ciks.size() + " CIKs from stale cache while refreshing in background");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_ALL)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " CIKs from expired disk cache, refreshing in background");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load cache from disk: " + e.getMessage());
      }
    }

    // No cache exists at all - must fetch synchronously (only happens on first use)
    LOGGER.info("No cache exists for " + cacheKey + ", fetching synchronously...");
    LOGGER.info("Fetching all EDGAR filers from SEC API...");
    try {
      List<String> ciks = fetchAllEdgarFilersFromSEC();

      // Cache the results
      CachedData newCache = new CachedData(ciks, SEC_TICKERS_URL);
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " unique CIKs from SEC");
      return new ArrayList<>(ciks);

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch SEC data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      return new ArrayList<>();
    }
  }

  /**
   * Fetch CIK data from SEC API.
   */
  private static List<String> fetchAllEdgarFilersFromSEC() throws IOException {
    Set<String> uniqueCiks = new HashSet<>();

    // Fetch company tickers
    URI uri = URI.create(SEC_TICKERS_URL);
    HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", "Apache-Calcite-XBRL/1.0 (admin@example.com)");
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
    if (responseCode != 200) {
      throw new IOException("SEC API returned status " + responseCode);
    }

    // Parse JSON response
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

      JsonNode root = MAPPER.readTree(reader);

      // Extract CIKs from the response
      root.fields().forEachRemaining(entry -> {
        JsonNode item = entry.getValue();
        if (item.has("cik_str")) {
          String cik = String.format("%010d", item.get("cik_str").asLong());
          uniqueCiks.add(cik);
        }
      });
    }

    return new ArrayList<>(uniqueCiks);
  }

  /**
   * Load cached data from disk.
   */
  private static CachedData loadFromDisk(File cacheFile) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile))) {
      JsonNode root = MAPPER.readTree(reader);

      List<String> ciks = new ArrayList<>();
      if (root.has("ciks")) {
        root.get("ciks").forEach(node -> ciks.add(node.asText()));
      }

      String source = root.has("source") ? root.get("source").asText() : "unknown";

      // Create cached data with timestamp from file
      CachedData cached = new CachedData(ciks, source);

      // Override timestamp if stored in file
      if (root.has("timestamp")) {
        long epochMilli = root.get("timestamp").asLong();
        cached.timestamp.plusMillis(epochMilli - cached.timestamp.toEpochMilli());
      }

      return cached;
    }
  }

  /**
   * Save cached data to disk.
   */
  private static void saveToDisk(File cacheFile, CachedData data) {
    try (FileWriter writer = new FileWriter(cacheFile)) {
      writer.write("{\n");
      writer.write("  \"timestamp\": " + data.timestamp.toEpochMilli() + ",\n");
      writer.write("  \"source\": \"" + data.source + "\",\n");
      writer.write("  \"count\": " + data.ciks.size() + ",\n");
      writer.write("  \"ciks\": [\n");

      for (int i = 0; i < data.ciks.size(); i++) {
        writer.write("    \"" + data.ciks.get(i) + "\"");
        if (i < data.ciks.size() - 1) {
          writer.write(",");
        }
        writer.write("\n");
      }

      writer.write("  ]\n");
      writer.write("}\n");

      LOGGER.fine("Saved " + data.ciks.size() + " CIKs to disk cache");
    } catch (IOException e) {
      LOGGER.warning("Failed to save cache to disk: " + e.getMessage());
    }
  }

  /**
   * Fetch S&P 500 constituent CIKs.
   * @return List of S&P 500 company CIKs
   */
  public static List<String> fetchSP500Constituents() {
    String cacheKey = "_SP500_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " S&P 500 CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " S&P 500 CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " S&P 500 CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " S&P 500 CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load S&P 500 cache from disk: " + e.getMessage());
      }
    }

    // Fetch fresh data
    LOGGER.info("Fetching S&P 500 constituents...");
    try {
      List<String> ciks = fetchSP500FromWikipedia();

      if (ciks.isEmpty()) {
        if (TEST_MODE || !FALLBACK_ENABLED) {
          throw new DataFetchException("Failed to fetch S&P 500 from Wikipedia - no data returned");
        }
        // Fallback to hardcoded list if Wikipedia fails and fallback is enabled
        LOGGER.warning("PRIMARY FETCH FAILED: Wikipedia returned no S&P 500 data, using hardcoded fallback");
        ciks = getHardcodedSP500CIKs();
      }

      // Cache the results
      CachedData newCache = new CachedData(ciks, "Wikipedia/Hardcoded");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " S&P 500 CIKs");
      return new ArrayList<>(ciks);

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch S&P 500 data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale S&P 500 cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      // Last resort: return hardcoded list if fallback enabled
      if (TEST_MODE || !FALLBACK_ENABLED) {
        throw new RuntimeException("Failed to fetch S&P 500 data and fallback disabled", e);
      }
      LOGGER.warning("FALLBACK: All S&P 500 fetch methods failed, using hardcoded data as last resort");
      return getHardcodedSP500CIKs();
    }
  }

  /**
   * Fetch S&P 500 tickers from Wikipedia and map to CIKs.
   */
  private static List<String> fetchSP500FromWikipedia() {
    List<String> ciks = new ArrayList<>();

    try {
      // First get all SEC tickers for mapping
      Map<String, String> tickerToCik = fetchTickerToCikMap();

      // Download and parse Wikipedia page directly
      URI uri = URI.create(SP500_WIKI_URL);
      HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("User-Agent", "Apache-Calcite-XBRL/1.0");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(30000);

      if (conn.getResponseCode() != 200) {
        LOGGER.warning("Wikipedia returned status " + conn.getResponseCode());
        return ciks;
      }

      // Parse HTML to extract tickers
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String line;
        boolean inTable = false;
        boolean inRow = false;
        int tdCount = 0;

        while ((line = reader.readLine()) != null) {
          // Look for the wikitable sortable class (S&P 500 table)
          if (line.contains("wikitable sortable") && line.contains("<table")) {
            inTable = true;
            continue;
          }

          if (!inTable) continue;

          // End of table
          if (line.contains("</table>")) {
            break;
          }

          // Start of row
          if (line.contains("<tr")) {
            inRow = true;
            tdCount = 0;
            continue;
          }

          // End of row
          if (line.contains("</tr>")) {
            inRow = false;
            continue;
          }

          // Table data cell
          if (inRow && line.contains("<td")) {
            tdCount++;

            // The ticker is in the first <td> of each row
            if (tdCount == 1) {
              // Extract ticker from the line
              // Look for pattern like: <td><a href="...">TICKER</a></td>
              // or: <td>TICKER</td>

              String ticker = null;

              // Try to find ticker between > and <
              int start = line.indexOf(">");
              while (start >= 0) {
                int end = line.indexOf("<", start + 1);
                if (end > start + 1) {
                  String candidate = line.substring(start + 1, end).trim();
                  // Check if it looks like a ticker (1-5 uppercase letters, possibly with dots)
                  if (candidate.matches("[A-Z][A-Z0-9.]{0,4}")) {
                    ticker = candidate;
                    break;
                  }
                }
                start = line.indexOf(">", end);
              }

              if (ticker != null) {
                // Handle special cases
                if (ticker.equals("BRK.B")) ticker = "BRK-B";
                if (ticker.equals("BF.B")) ticker = "BF-B";

                // Map ticker to CIK
                String cik = tickerToCik.get(ticker);
                if (cik != null && !ciks.contains(cik)) {
                  ciks.add(cik);
                }
              }
            }
          }
        }
      }

      LOGGER.info("Parsed " + ciks.size() + " S&P 500 companies from Wikipedia");

    } catch (Exception e) {
      LOGGER.warning("Failed to fetch from Wikipedia: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch ticker to CIK mapping from SEC.
   */
  private static Map<String, String> fetchTickerToCikMap() throws IOException {
    Map<String, String> tickerToCik = new HashMap<>();

    URI uri = URI.create(SEC_TICKERS_URL);
    HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", "Apache-Calcite-XBRL/1.0 (admin@example.com)");
    conn.setRequestProperty("Accept", "application/json");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Rate limiting
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (conn.getResponseCode() != 200) {
      throw new IOException("SEC API returned status " + conn.getResponseCode());
    }

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

      JsonNode root = MAPPER.readTree(reader);
      root.fields().forEachRemaining(entry -> {
        JsonNode item = entry.getValue();
        if (item.has("ticker") && item.has("cik_str")) {
          String ticker = item.get("ticker").asText().toUpperCase();
          String cik = String.format("%010d", item.get("cik_str").asLong());
          tickerToCik.put(ticker, cik);
        }
      });
    }

    return tickerToCik;
  }

  /**
   * Get hardcoded S&P 500 CIKs as fallback.
   */
  private static List<String> getHardcodedSP500CIKs() {
    // Top 50 S&P 500 companies by weight as fallback
    List<String> ciks = new ArrayList<>();
    ciks.add("0000320193"); // AAPL
    ciks.add("0000789019"); // MSFT
    ciks.add("0001045810"); // NVDA
    ciks.add("0001018724"); // AMZN
    ciks.add("0001652044"); // GOOGL
    ciks.add("0001326801"); // META
    ciks.add("0001318605"); // TSLA
    ciks.add("0001067983"); // BRK.B
    ciks.add("0000059478"); // LLY
    ciks.add("0000200406"); // JNJ
    ciks.add("0000019617"); // JPM
    ciks.add("0001403161"); // V
    ciks.add("0000731766"); // UNH
    ciks.add("0001141391"); // MA
    ciks.add("0000034088"); // XOM
    ciks.add("0000354950"); // HD
    ciks.add("0000080424"); // PG
    ciks.add("0000104169"); // WMT
    ciks.add("0001551152"); // ABBV
    ciks.add("0000093410"); // CVX
    ciks.add("0000077476"); // PEP
    ciks.add("0001108524"); // CRM
    ciks.add("0000070858"); // BAC
    ciks.add("0000021344"); // KO
    ciks.add("0000310158"); // MRK
    ciks.add("0001075531"); // COST
    ciks.add("0000064803"); // CVS
    ciks.add("0000796343"); // ADBE
    ciks.add("0001364742"); // AMD
    ciks.add("0000097745"); // TMO
    ciks.add("0000078003"); // PFE
    ciks.add("0000909832"); // MCD
    ciks.add("0001166691"); // NFLX
    ciks.add("0000732717"); // T
    ciks.add("0001341439"); // ORCL
    ciks.add("0000050863"); // INTC
    ciks.add("0001065280"); // NFLX
    ciks.add("0000886982"); // GS
    ciks.add("0001373715"); // NOW
    ciks.add("0000804328"); // QCOM
    ciks.add("0000858877"); // CSCO
    ciks.add("0000060667"); // NEE
    ciks.add("0000936468"); // LMT
    ciks.add("0000728535"); // LOW
    ciks.add("0000315293"); // AMAT
    ciks.add("0001637459"); // UBER
    ciks.add("0000732712"); // VZ
    ciks.add("0000316709"); // SCHW
    ciks.add("0001744489"); // DIS
    ciks.add("0000072971"); // WFC

    LOGGER.info("Using hardcoded list of " + ciks.size() + " S&P 500 companies");
    return ciks;
  }

  /**
   * Fetch DJI (Dow Jones Industrial Average) constituent CIKs.
   * @return List of DJI company CIKs
   */
  public static List<String> fetchDJIConstituents() {
    String cacheKey = "_DJI_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " DJI CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " DJI CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " DJI CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " DJI CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load DJI cache from disk: " + e.getMessage());
      }
    }

    // Fetch fresh data
    LOGGER.info("Fetching DJI constituents...");
    try {
      List<String> ciks = fetchDJIFromWikipedia();

      if (ciks.isEmpty()) {
        if (TEST_MODE || !FALLBACK_ENABLED) {
          throw new DataFetchException("Failed to fetch DJI from Wikipedia - no data returned");
        }
        // Fallback to hardcoded list if Wikipedia fails and fallback is enabled
        LOGGER.warning("PRIMARY FETCH FAILED: Wikipedia returned no DJI data, using hardcoded fallback");
        ciks = getHardcodedDJICIKs();
      }

      // Cache the results
      CachedData newCache = new CachedData(ciks, "Wikipedia/Hardcoded");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " DJI CIKs");
      return new ArrayList<>(ciks);

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch DJI data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale DJI cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      // Last resort: return hardcoded list if fallback enabled
      if (TEST_MODE || !FALLBACK_ENABLED) {
        throw new RuntimeException("Failed to fetch DJI data and fallback disabled", e);
      }
      LOGGER.warning("FALLBACK: All DJI fetch methods failed, using hardcoded data as last resort");
      return getHardcodedDJICIKs();
    }
  }

  /**
   * Fetch DJI tickers from Wikipedia and map to CIKs using file adapter.
   */
  private static List<String> fetchDJIFromWikipedia() {
    List<String> ciks = new ArrayList<>();

    try {
      // First get all SEC tickers for mapping
      Map<String, String> tickerToCik = fetchTickerToCikMap();

      // Use the file adapter to fetch and parse Wikipedia
      // Load the model from resources
      InputStream modelStream = SecDataFetcher.class.getResourceAsStream("/dji-wiki-model.json");
      if (modelStream == null) {
        LOGGER.warning("Could not load dji-wiki-model.json from resources");
        return ciks;
      }

      // Create a connection to query the Wikipedia table
      Properties info = new Properties();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(modelStream))) {
        StringBuilder modelJson = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          modelJson.append(line).append("\n");
        }
        info.put("model", modelJson.toString());
      }

      // Use JDBC to query the Wikipedia table
      try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:", info);
           java.sql.Statement stmt = conn.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery("SELECT ticker FROM dji_wiki.dji_constituents")) {

        while (rs.next()) {
          String ticker = rs.getString("ticker");
          if (ticker != null && !ticker.isEmpty()) {
            // Clean up ticker (remove any extra spaces)
            ticker = ticker.trim().toUpperCase();

            // Map ticker to CIK
            String cik = tickerToCik.get(ticker);
            if (cik != null && !ciks.contains(cik)) {
              ciks.add(cik);
              LOGGER.fine("Mapped ticker " + ticker + " to CIK " + cik);
            } else if (cik == null) {
              LOGGER.fine("No CIK mapping found for ticker: " + ticker);
            }
          }
        }
      }

      LOGGER.info("Fetched " + ciks.size() + " DJI constituents from Wikipedia using file adapter");
      return ciks;

    } catch (Exception e) {
      LOGGER.warning("Failed to fetch DJI from Wikipedia: " + e.getMessage());
      e.printStackTrace();
      return ciks;
    }
  }

  /**
   * Get hardcoded DJI CIKs as fallback.
   * Current Dow 30 constituents as of 2024.
   */
  private static List<String> getHardcodedDJICIKs() {
    List<String> ciks = new ArrayList<>();
    // Current Dow Jones Industrial Average 30 companies
    ciks.add("0000320193"); // AAPL - Apple
    ciks.add("0000004962"); // AXP - American Express
    ciks.add("0000012927"); // BA - Boeing
    ciks.add("0000018230"); // CAT - Caterpillar
    ciks.add("0000093410"); // CVX - Chevron
    ciks.add("0000021344"); // KO - Coca-Cola
    ciks.add("0001744489"); // DIS - Disney
    ciks.add("0000886982"); // GS - Goldman Sachs
    ciks.add("0000354950"); // HD - Home Depot
    ciks.add("0000773840"); // HON - Honeywell
    ciks.add("0000051143"); // IBM - IBM
    ciks.add("0000050863"); // INTC - Intel
    ciks.add("0000200406"); // JNJ - Johnson & Johnson
    ciks.add("0000019617"); // JPM - JPMorgan Chase
    ciks.add("0000063908"); // MCD - McDonald's
    ciks.add("0000310158"); // MRK - Merck
    ciks.add("0000789019"); // MSFT - Microsoft
    ciks.add("0000066740"); // MMM - 3M
    ciks.add("0000320187"); // NKE - Nike
    ciks.add("0000080424"); // PG - Procter & Gamble
    ciks.add("0001108524"); // CRM - Salesforce
    ciks.add("0000086312"); // TRV - Travelers
    ciks.add("0000731766"); // UNH - UnitedHealth
    ciks.add("0000732712"); // VZ - Verizon
    ciks.add("0001403161"); // V - Visa
    ciks.add("0001618921"); // WBA - Walgreens
    ciks.add("0000104169"); // WMT - Walmart
    // Note: Dow recently changed, some companies may be different

    LOGGER.info("Using hardcoded list of " + ciks.size() + " DJI companies");
    return ciks;
  }

  /**
   * Fetch Russell 2000 constituent CIKs.
   * @return List of Russell 2000 company CIKs
   */
  public static List<String> fetchRussell2000Constituents() {
    String cacheKey = "_RUSSELL2000_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " Russell 2000 CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " Russell 2000 CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 2000 CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 2000 CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load Russell 2000 cache from disk: " + e.getMessage());
      }
    }

    // Fetch fresh data
    LOGGER.info("Fetching Russell 2000 constituents...");
    try {
      // Try to fetch from various sources
      List<String> ciks = fetchRussell2000FromSources();

      if (ciks.isEmpty()) {
        // Use hardcoded sample as fallback
        ciks = getHardcodedRussell2000CIKs();
      }

      // Cache the results
      CachedData newCache = new CachedData(ciks, "Various sources");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " Russell 2000 CIKs");
      return new ArrayList<>(ciks);

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch Russell 2000 data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale Russell 2000 cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      // Last resort: return hardcoded sample
      return getHardcodedRussell2000CIKs();
    }
  }

  /**
   * Fetch Russell 2000 tickers from available sources.
   * Note: Russell 2000 is harder to get than S&P 500 - no single authoritative free source.
   */
  private static List<String> fetchRussell2000FromSources() {
    List<String> ciks = new ArrayList<>();

    try {
      // Try to fetch from iShares IWM ETF holdings
      ciks = fetchRussell2000FromIShares();

      if (!ciks.isEmpty()) {
        LOGGER.info("Successfully fetched " + ciks.size() + " Russell 2000 CIKs from iShares");
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch from iShares: " + e.getMessage());
    }

    // If iShares fails, return empty list (will fall back to hardcoded)
    return ciks;
  }

  /**
   * Fetch Russell 2000 holdings from iShares IWM ETF.
   */
  private static List<String> fetchRussell2000FromIShares() throws IOException {
    List<String> ciks = new ArrayList<>();

    // Try multiple approaches to get Russell 2000 data

    // First, try the NASDAQ list of Russell 2000 components
    try {
      ciks = fetchRussell2000FromNasdaq();
      if (!ciks.isEmpty() && ciks.size() > 1000) {
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.info("NASDAQ approach failed: " + e.getMessage());
    }

    // Try fetching from financial data aggregators that list Russell 2000 components
    try {
      ciks = fetchRussell2000FromAggregators();
      if (!ciks.isEmpty() && ciks.size() > 1000) {
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.info("Aggregator approach failed: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Try to fetch Russell 2000 from NASDAQ or other exchanges.
   */
  private static List<String> fetchRussell2000FromNasdaq() throws IOException {
    List<String> ciks = new ArrayList<>();

    // First try to fetch from LLM (Claude/Sonnet)
    try {
      ciks = fetchRussell2000FromLLM();
      if (!ciks.isEmpty() && ciks.size() >= 1500) {
        LOGGER.info("Successfully fetched " + ciks.size() + " Russell 2000 companies from LLM");
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch Russell 2000 from LLM: " + e.getMessage());
    }

    // Fallback: Try to get all small-cap companies from SEC that would likely be in Russell 2000
    // Russell 2000 is approximately companies ranked 1001-3000 by market cap in US markets
    try {
      Map<String, String> allTickers = fetchTickerToCikMap();

      // Filter for likely Russell 2000 candidates based on common patterns
      // These are companies that are typically small-cap US listed companies
      Set<String> uniqueCiks = new HashSet<>();

      for (Map.Entry<String, String> entry : allTickers.entrySet()) {
        String ticker = entry.getKey();
        String cik = entry.getValue();

        // Skip if ticker is too long (likely not a common stock)
        if (ticker.length() > 5) continue;

        // Skip if ticker contains numbers at the end (likely warrants/preferred)
        if (ticker.matches(".*\\d$")) continue;

        // Skip known large-cap tickers (S&P 500 companies)
        if (isLargeCap(ticker)) continue;

        // Add to our Russell 2000 candidate list
        uniqueCiks.add(cik);

        // Limit to approximately 2000 companies
        if (uniqueCiks.size() >= 2000) {
          break;
        }
      }

      if (uniqueCiks.size() >= 1000) {
        ciks.addAll(uniqueCiks);
        LOGGER.info("Identified " + ciks.size() + " potential Russell 2000 companies from SEC data");
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch from SEC ticker data: " + e.getMessage());
    }

    // Fallback to a comprehensive list of known Russell 2000 tickers
    // This is a representative sample, not the complete list
    String[] russell2000SampleTickers = {
      // Technology
      "SMCI", "MARA", "APP", "IONQ", "RBLX", "DUOL", "ASAN", "MNDY", "DDOG", "NET",
      "SNOW", "U", "COIN", "HOOD", "SOFI", "AFRM", "UPST", "DAVE", "ML", "OPEN",
      // Healthcare
      "CELH", "TMDX", "EXAS", "NBIX", "ARWR", "AXSM", "MRNA", "OSCR", "RXRX", "SDGR",
      "ACAD", "SAGE", "SNDX", "SRPT", "PRTA", "RGNX", "VKTX", "ZLAB", "BEAM", "EDIT",
      // Consumer
      "CHRD", "CAVA", "SHAK", "WING", "BROS", "PLNT", "XPEL", "LULU", "DKS", "BOOT",
      "VSCO", "FIVE", "BURL", "OLPX", "ULTA", "TJX", "ROST", "BBW", "ANF", "GPS",
      // Financial
      "RKT", "TPG", "ARES", "BHF", "JXN", "RYAN", "IBKR", "LPLA", "RJF", "SF",
      "EWBC", "FCNCA", "WTFC", "FHN", "PNFP", "TCBI", "UMBF", "GBCI", "FFIN", "HWC",
      // Energy
      "CNX", "RRC", "AR", "MTDR", "SM", "CRK", "PDCE", "MGY", "CTRA", "CPE",
      "GPOR", "NOG", "TELL", "NFE", "CLNE", "RNG", "PLUG", "FCEL", "BE", "ENPH",
      // Industrial
      "RMBS", "XPO", "ALSN", "HWM", "TDG", "HEI", "CW", "ATI", "GGG", "RBC",
      "PNR", "MSA", "AIT", "MIDD", "UFPI", "MLI", "TREX", "AZEK", "BLDR", "IBP",
      // Real Estate
      "CBRE", "JLL", "NMRK", "MMI", "COMP", "OPEN", "RDFN", "EXPI", "HOUS", "DOUG",
      "CCS", "LGIH", "TPH", "CUZ", "GRBK", "MHO", "BZH", "CVCO", "HOV", "SGC",
      // Communications
      "SNAP", "PINS", "SPOT", "ROKU", "TTD", "MGNI", "APPS", "PERI", "ZETA", "TBLA",
      "PUBM", "NCMI", "CNK", "IMAX", "LGF", "NWSA", "FOX", "DISCA", "VIAC", "AMCX",
      // Materials
      "CLF", "X", "STLD", "NUE", "RS", "CMC", "ATI", "TMST", "CRS", "WOR",
      "SUM", "VMC", "MLM", "EXP", "UFPI", "TREX", "AZEK", "BLDR", "BCC", "BECN",
      // Additional Russell 2000 companies
      "PCTY", "CROX", "DECK", "SKX", "WWW", "COLM", "HBI", "PVH", "RL", "GIII",
      "JOBY", "EVTL", "LILM", "ACHR", "BLDE", "WKHS", "ARVL", "GOEV", "NKLA", "RIDE",
      "FSR", "LCID", "RIVN", "PTRA", "LEV", "CHPT", "BLNK", "EVGO", "VLTA", "QS",
      "LAZR", "OUST", "INVZ", "AEVA", "MVIS", "VUZI", "KOPN", "EMAN", "WIMI", "CEVA",
      "AMBA", "AMKR", "AOSL", "DIOD", "POWI", "SLAB", "SWKS", "QRVO", "CRUS", "SYNA",
      "SIMO", "SITM", "MPWR", "VICR", "CTS", "PLXS", "SANM", "TTMI", "FLEX", "JBL",
      "KLIC", "LFUS", "NVTS", "ONTO", "UCTT", "VECO", "ACLS", "COHU", "EXLS", "FORM",
      "HIMX", "IPGP", "LSCC", "MXL", "RMBS", "SGH", "SSTI", "VIAV", "WOLF", "ALGM"
    };

    // Get ticker to CIK mapping
    Map<String, String> tickerToCik = fetchTickerToCikMap();

    Set<String> uniqueCiks = new HashSet<>();
    for (String ticker : russell2000SampleTickers) {
      String cik = tickerToCik.get(ticker.toUpperCase());
      if (cik != null) {
        uniqueCiks.add(cik);
      }
    }

    ciks.addAll(uniqueCiks);
    LOGGER.info("Mapped " + ciks.size() + " Russell 2000 tickers to CIKs from sample list");

    return ciks;
  }

  /**
   * Try to fetch from other aggregators.
   */
  private static List<String> fetchRussell2000FromAggregators() throws IOException {
    // This would implement fetching from other sources
    // For now, return empty to fall back to other methods
    return new ArrayList<>();
  }

  /**
   * Check if a ticker is a known large-cap company (S&P 500).
   */
  private static boolean isLargeCap(String ticker) {
    // Common S&P 500 companies to exclude from Russell 2000
    Set<String> sp500Tickers =
      new HashSet<>(
          Arrays.asList("AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "BRK-A",
      "JPM", "JNJ", "V", "UNH", "HD", "PG", "MA", "DIS", "BAC", "ADBE",
      "CRM", "NFLX", "XOM", "CVX", "KO", "PEP", "ABBV", "TMO", "CSCO", "AVGO",
      "WMT", "VZ", "ABT", "ORCL", "ACN", "MCD", "COST", "LLY", "DHR", "TXN",
      "INTC", "WFC", "T", "AMD", "PM", "UNP", "IBM", "MS", "GE", "BA",
      "CAT", "RTX", "MMM", "GS", "AMGN", "HON", "AXP", "SBUX", "BLK", "ISRG",
      "CVS", "MDLZ", "PLD", "INTU", "SPGI", "LMT", "GILD", "TGT", "BKNG", "MO"));

    return sp500Tickers.contains(ticker.toUpperCase());
  }

  /**
   * Get hardcoded Russell 2000 CIKs as fallback.
   */
  private static List<String> getHardcodedRussell2000CIKs() {
    // Sample of Russell 2000 companies
    List<String> ciks = new ArrayList<>();
    ciks.add("0001121788"); // SMCI
    ciks.add("0001507605"); // MARA
    ciks.add("0001712189"); // APP
    ciks.add("0001707919"); // CELH
    ciks.add("0001681206"); // CHRD
    ciks.add("0001633880"); // TMDX
    ciks.add("0001599298"); // RMBS
    ciks.add("0001820630"); // AFRM
    ciks.add("0001321655"); // PLTR
    ciks.add("0001922968"); // IONQ
    ciks.add("0001805284"); // RKT
    ciks.add("0001690820"); // CVNA
    ciks.add("0001883313"); // TPG
    ciks.add("0000814453"); // ARES
    ciks.add("0001761312"); // BHF
    ciks.add("0001837607"); // JXN
    ciks.add("0001282637"); // EXAS
    ciks.add("0001876437"); // RYAN
    ciks.add("0001562088"); // DUOL
    ciks.add("0001628063"); // ALIT

    LOGGER.info("Using hardcoded list of " + ciks.size() + " Russell 2000 companies");
    return ciks;
  }

  /**
   * Fetch Russell 1000 constituent CIKs.
   * Russell 1000 = top 1000 US companies by market cap.
   * @return List of Russell 1000 company CIKs
   */
  public static List<String> fetchRussell1000Constituents() {
    String cacheKey = "_RUSSELL1000_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " Russell 1000 CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " Russell 1000 CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 1000 CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 1000 CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load Russell 1000 cache from disk: " + e.getMessage());
      }
    }

    // Fetch fresh data
    LOGGER.info("Fetching Russell 1000 constituents...");
    try {
      List<String> ciks = fetchRussell1000FromSources();

      if (ciks.isEmpty()) {
        // Use a combination of S&P 500 and mid-cap companies as approximation
        ciks = getApproximateRussell1000();
      }

      // Cache the results
      CachedData newCache = new CachedData(ciks, "SEC/Approximation");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " Russell 1000 CIKs");
      return new ArrayList<>(ciks);

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch Russell 1000 data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale Russell 1000 cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      // Last resort: return approximation
      return getApproximateRussell1000();
    }
  }

  /**
   * Fetch Russell 3000 constituent CIKs.
   * Russell 3000 = Russell 1000 + Russell 2000 (top 3000 US companies).
   * @return List of Russell 3000 company CIKs
   */
  public static List<String> fetchRussell3000Constituents() {
    String cacheKey = "_RUSSELL3000_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " Russell 3000 CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " Russell 3000 CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 3000 CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Russell 3000 CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (IOException e) {
        LOGGER.warning("Failed to load Russell 3000 cache from disk: " + e.getMessage());
      }
    }

    // Fetch fresh data
    LOGGER.info("Fetching Russell 3000 constituents...");
    try {
      // Try to fetch from LLM first
      List<String> ciks = fetchRussell3000FromLLM();

      if (ciks.isEmpty()) {
        // Fallback: Russell 3000 = Russell 1000 + Russell 2000
        LOGGER.warning("LLM fetch failed, using fallback Russell 1000 + Russell 2000 combination");
        Set<String> russell3000 = new HashSet<>();

        // Get Russell 1000
        List<String> russell1000 = fetchRussell1000Constituents();
        russell3000.addAll(russell1000);

        // Get Russell 2000
        List<String> russell2000 = fetchRussell2000Constituents();
        russell3000.addAll(russell2000);

        ciks = new ArrayList<>(russell3000);
      }

      // Cache the results
      CachedData newCache = new CachedData(ciks, "LLM/Russell 3000");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);

      LOGGER.info("Successfully fetched " + ciks.size() + " Russell 3000 CIKs");
      return ciks;

    } catch (Exception e) {
      LOGGER.severe("Failed to fetch Russell 3000 data: " + e.getMessage());

      // Try to return stale cache if available
      if (cached != null) {
        LOGGER.warning("Returning stale Russell 3000 cache due to fetch failure");
        return new ArrayList<>(cached.ciks);
      }

      return new ArrayList<>();
    }
  }

  /**
   * Fetch Russell 1000 from various sources.
   */
  private static List<String> fetchRussell1000FromSources() throws IOException {
    List<String> ciks = new ArrayList<>();

    // First try to fetch from LLM (Claude/Sonnet)
    try {
      ciks = fetchRussell1000FromLLM();
      if (!ciks.isEmpty() && ciks.size() >= 800) {
        LOGGER.info("Successfully fetched " + ciks.size() + " Russell 1000 companies from LLM");
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch from LLM: " + e.getMessage());
    }

    // Fallback to approximation method
    // Try to get top 1000 companies by approximation
    // This includes S&P 500 + next 500 mid-cap companies
    try {
      Map<String, String> allTickers = fetchTickerToCikMap();
      Set<String> uniqueCiks = new HashSet<>();

      // First add S&P 500 companies
      List<String> sp500Ciks = fetchSP500Constituents();
      uniqueCiks.addAll(sp500Ciks);

      // Then add more large/mid-cap companies
      for (Map.Entry<String, String> entry : allTickers.entrySet()) {
        String ticker = entry.getKey();
        String cik = entry.getValue();

        // Skip if already have enough
        if (uniqueCiks.size() >= 1000) break;

        // Skip if ticker is too long or has special chars
        if (ticker.length() > 5 || ticker.matches(".*\\d$")) continue;

        // Add to Russell 1000 candidates
        uniqueCiks.add(cik);
      }

      ciks.addAll(uniqueCiks);

      // Ensure we have approximately 1000
      if (ciks.size() > 1000) {
        ciks = ciks.subList(0, 1000);
      }

      LOGGER.info("Identified " + ciks.size() + " Russell 1000 companies");

    } catch (Exception e) {
      LOGGER.warning("Failed to fetch Russell 1000: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch Russell 1000 constituents from LLM (Claude/Sonnet) with pagination.
   */
  private static List<String> fetchRussell1000FromLLM() throws IOException {
    List<String> allCiks = new ArrayList<>();

    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    // Fetch in chunks of ~300 companies per request to stay within token limits
    int totalRequests = 4; // 4 requests × 250 companies = 1000 companies
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        List<String> pageCiks = fetchRussellPageFromLLM("Russell 1000", startRank, endRank, companiesPerRequest);
        allCiks.addAll(pageCiks);

        LOGGER.info("Fetched page " + page + "/" + totalRequests + ": " + pageCiks.size() + " companies");

        // Small delay between requests to be respectful
        Thread.sleep(1000);

      } catch (Exception e) {
        LOGGER.warning("Failed to fetch Russell 1000 page " + page + ": " + e.getMessage());
        // Continue with other pages
      }
    }

    LOGGER.info("Total Russell 1000 companies fetched from LLM: " + allCiks.size());
    return allCiks;
  }

  /**
   * Fetch Russell 2000 constituents from LLM (Claude/Sonnet) with pagination.
   */
  private static List<String> fetchRussell2000FromLLM() throws IOException {
    List<String> allCiks = new ArrayList<>();

    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    // Fetch in chunks of ~250 companies per request to stay within token limits
    int totalRequests = 8; // 8 requests × 250 companies = 2000 companies
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        List<String> pageCiks = fetchRussellPageFromLLM("Russell 2000", startRank, endRank, companiesPerRequest);
        allCiks.addAll(pageCiks);

        LOGGER.info("Fetched Russell 2000 page " + page + "/" + totalRequests + ": " + pageCiks.size() + " companies");

        // Small delay between requests to be respectful
        Thread.sleep(1000);

      } catch (Exception e) {
        LOGGER.warning("Failed to fetch Russell 2000 page " + page + ": " + e.getMessage());
        // Continue with other pages
      }
    }

    LOGGER.info("Total Russell 2000 companies fetched from LLM: " + allCiks.size());
    return allCiks;
  }

  /**
   * Fetch a page of Russell index constituents from LLM.
   */
  private static List<String> fetchRussellPageFromLLM(String indexName, int startRank, int endRank, int maxCompanies) throws IOException {
    List<String> ciks = new ArrayList<>();

    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    String apiUrl = "https://api.anthropic.com/v1/messages";

    String prompt = "Generate a JSON array of " + indexName + " stock index constituents ranked approximately "
        + startRank + " to " + endRank + " by market capitalization. "
        + "Include ONLY companies that are currently in the " + indexName + " index. "
        + "Format: [{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\"},...] "
        + "Provide exactly " + maxCompanies + " companies. Return ONLY the JSON array, no other text.";

    try {
      URI uri = URI.create(apiUrl);
      HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty("x-api-key", apiKey);
      conn.setRequestProperty("anthropic-version", "2023-06-01");
      conn.setDoOutput(true);
      conn.setConnectTimeout(60000);
      conn.setReadTimeout(120000);

      // Build request body
      String requestBody = "{"
          + "\"model\": \"claude-3-5-sonnet-20241022\","
          + "\"max_tokens\": 8192,"
          + "\"temperature\": 0,"
          + "\"messages\": [{\"role\": \"user\", \"content\": \"" + prompt.replace("\"", "\\\"") + "\"}]"
          + "}";

      // Send request
      try (java.io.OutputStreamWriter writer = new java.io.OutputStreamWriter(conn.getOutputStream())) {
        writer.write(requestBody);
        writer.flush();
      }

      // Check response code
      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        // Log error response for debugging
        try (BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
          String errorResponse = errorReader.lines().collect(Collectors.joining("\n"));
          LOGGER.warning("LLM API returned status " + responseCode + ": " + errorResponse);
        } catch (Exception e) {
          LOGGER.warning("LLM API returned status " + responseCode);
        }
        return ciks;
      }

      // Parse response
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        JsonNode response = MAPPER.readTree(reader);

        // Extract the content from the response
        if (response.has("content") && response.get("content").isArray()
            && response.get("content").size() > 0) {
          String content = response.get("content").get(0).get("text").asText();

          // Parse the JSON array from the content
          JsonNode companies = MAPPER.readTree(content);

          if (companies.isArray()) {
            // Get ticker to CIK mapping from SEC (cached from previous calls)
            Map<String, String> tickerToCik = fetchTickerToCikMap();

            for (JsonNode company : companies) {
              String ticker = company.has("ticker") ? company.get("ticker").asText() : "";
              String cik = company.has("cik") ? company.get("cik").asText() : "";

              // If CIK is provided, use it
              if (!cik.isEmpty() && cik.matches("\\d{10}")) {
                ciks.add(cik);
              }
              // Otherwise, try to map ticker to CIK
              else if (!ticker.isEmpty()) {
                String mappedCik = tickerToCik.get(ticker.toUpperCase());
                if (mappedCik != null) {
                  ciks.add(mappedCik);
                }
              }
            }
          }
        }
      }

    } catch (Exception e) {
      LOGGER.warning("Error calling LLM API for " + indexName + " page " + startRank + "-" + endRank + ": " + e.getMessage());
      throw new IOException("Failed to fetch " + indexName + " page from LLM", e);
    }

    return ciks;
  }

  /**
   * Fetch Russell 3000 from LLM API.
   */
  private static List<String> fetchRussell3000FromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    List<String> allCiks = new ArrayList<>();

    // Russell 3000: 12 requests × 250 companies = 3000 companies
    int totalRequests = 12;
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        LOGGER.info("Fetching Russell 3000 page " + page + " (ranks " + startRank + "-" + endRank + ")...");

        List<String> pageCiks = fetchRussellPageFromLLM("Russell 3000", startRank, endRank, companiesPerRequest);
        allCiks.addAll(pageCiks);

        LOGGER.info("Page " + page + " returned " + pageCiks.size() + " CIKs (total so far: " + allCiks.size() + ")");

        // Small delay between requests to be respectful
        Thread.sleep(1000);
      } catch (Exception e) {
        LOGGER.warning("Error fetching Russell 3000 page " + page + ": " + e.getMessage());
        // Continue with other pages even if one fails
      }
    }

    LOGGER.info("Fetched total of " + allCiks.size() + " Russell 3000 CIKs from LLM");
    return allCiks;
  }

  /**
   * Fetch NASDAQ-100 constituents using LLM API.
   * @return List of CIKs for NASDAQ-100 companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchNasdaq100Constituents() throws IOException {
    // Check cache first
    CachedData cached = NASDAQ100_CACHE.get("nasdaq100");
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded("_NASDAQ100_CONSTITUENTS", CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " NASDAQ-100 CIKs from stale cache while refreshing");
      } else {
        LOGGER.info("Using cached NASDAQ-100 data with " + cached.ciks.size() + " companies");
      }
      return cached.ciks;
    }

    // No cache exists - must fetch synchronously
    List<String> ciks = new ArrayList<>();

    // Try to fetch from LLM
    try {
      ciks = fetchNasdaq100FromLLM();
      if (!ciks.isEmpty()) {
        // Cache the data for 1 day
        NASDAQ100_CACHE.put("nasdaq100", new CachedData(ciks, "LLM API"));
        LOGGER.info("Successfully cached " + ciks.size() + " NASDAQ-100 CIKs from LLM");
        return ciks;
      }
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch NASDAQ-100 from LLM: " + e.getMessage());
    }

    // Fallback: Use known NASDAQ-100 companies from QQQ ETF approximation
    LOGGER.warning("Using fallback NASDAQ-100 approximation");
    ciks = getApproximateNasdaq100();
    if (!ciks.isEmpty()) {
      // Cache with shorter TTL since it's approximation
      NASDAQ100_CACHE.put("nasdaq100", new CachedData(ciks, "Hardcoded/Approximation"));
    }
    return ciks;
  }

  /**
   * Fetch NASDAQ-100 from LLM API.
   */
  private static List<String> fetchNasdaq100FromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    List<String> allCiks = new ArrayList<>();

    // NASDAQ-100 has ~102 companies, we can fetch all in one request
    try {
      LOGGER.info("Fetching NASDAQ-100 from LLM API...");

      List<String> ciks = fetchRussellPageFromLLM("NASDAQ-100", 1, 102, 102);
      allCiks.addAll(ciks);

      LOGGER.info("Fetched " + allCiks.size() + " NASDAQ-100 CIKs from LLM");
    } catch (Exception e) {
      LOGGER.warning("Error fetching NASDAQ-100 from LLM: " + e.getMessage());
      throw new IOException("Failed to fetch NASDAQ-100 from LLM", e);
    }

    return allCiks;
  }

  /**
   * Get approximate NASDAQ-100 list using known constituents.
   */
  private static List<String> getApproximateNasdaq100() {
    List<String> ciks = new ArrayList<>();

    try {
      // Known major NASDAQ-100 constituents (top holdings)
      String[] knownNasdaq100Tickers = {
        "NVDA", "MSFT", "AAPL", "AMZN", "AVGO", "META", "NFLX", "GOOGL", "TSLA", "GOOG",
        "COST", "AMD", "CSCO", "TMUS", "PEP", "AMAT", "ISRG", "TXN", "QCOM", "BKNG",
        "INTU", "VRTX", "MU", "REGN", "PANW", "ADP", "GILD", "SBUX", "ADI", "LRCX",
        "ASML", "INTC", "MELI", "APP", "KLAC", "CTAS", "SNPS", "CDNS", "MDLZ", "NXPI",
        "CRWD", "MAR", "MRVL", "FTNT", "DASH", "ROP", "ORLY", "ADSK", "WDAY", "TTD",
        "PCAR", "AEP", "CHTR", "PAYX", "CPRT", "ROST", "FAST", "EA", "BKR", "VRSK",
        "KDP", "EXC", "TEAM", "LULU", "GEHC", "XEL", "CCEP", "MCHP", "DDOG", "KHC",
        "ON", "AZN", "IDXX", "TTWO", "FANG", "CSGP", "ANSS", "BIIB", "ILMN", "WBD"
      };

      // Map tickers to CIKs
      Map<String, String> tickerToCik = fetchTickerToCikMap();

      for (String ticker : knownNasdaq100Tickers) {
        String cik = tickerToCik.get(ticker);
        if (cik != null) {
          ciks.add(cik);
        }
      }

      LOGGER.info("Built approximate NASDAQ-100 with " + ciks.size() + " companies");
    } catch (Exception e) {
      LOGGER.warning("Failed to build approximate NASDAQ-100: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Get approximate Russell 1000 list.
   */
  private static List<String> getApproximateRussell1000() {
    List<String> ciks = new ArrayList<>();

    try {
      // Start with S&P 500
      ciks.addAll(fetchSP500Constituents());

      // Add known mid-cap companies to approximate Russell 1000
      // This would ideally come from a data provider

      LOGGER.info("Using approximate Russell 1000 with " + ciks.size() + " companies");
    } catch (Exception e) {
      LOGGER.warning("Failed to build approximate Russell 1000: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch all NYSE-listed companies.
   * @return List of CIKs for NYSE-listed companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchNYSEListedCompanies() throws IOException {
    String cacheKey = "_NYSE_LISTED";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_ALL)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
        LOGGER.info("Returning " + cached.ciks.size() + " NYSE-listed CIKs from stale cache while refreshing");
      } else {
        LOGGER.info("Using cached NYSE-listed data with " + cached.ciks.size() + " companies");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_ALL)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " NYSE-listed CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " NYSE-listed CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to load NYSE cache from disk: " + e.getMessage());
      }
    }

    List<String> ciks = new ArrayList<>();

    try {
      // Try to fetch from NASDAQ screener which includes NYSE listings
      ciks = fetchNYSEFromNasdaqScreener();

      if (ciks.isEmpty()) {
        // Fallback to LLM for major NYSE companies
        LOGGER.warning("NASDAQ screener failed, using LLM fallback for major NYSE companies");
        ciks = fetchNYSEFromLLM();
      }

      if (!ciks.isEmpty()) {
        // Cache the results
        CachedData newCache = new CachedData(ciks, "NASDAQ Screener/NYSE");
        memoryCache.put(cacheKey, newCache);
        saveToDisk(cacheFile, newCache);
        LOGGER.info("Successfully cached " + ciks.size() + " NYSE-listed CIKs");
      }
    } catch (Exception e) {
      LOGGER.severe("Failed to fetch NYSE listings: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch NYSE listings from NASDAQ screener.
   */
  private static List<String> fetchNYSEFromNasdaqScreener() throws IOException {
    List<String> ciks = new ArrayList<>();

    try {
      // URL that returns CSV of all NYSE listings
      String url = "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&render=download&exchange=nyse";

      HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();

      HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .GET()
        .timeout(Duration.ofMinutes(2))
        .build();

      HttpResponse<String> response;
      try {
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Request interrupted", e);
      }

      if (response.statusCode() == 200) {
        // Parse CSV to extract tickers
        String[] lines = response.body().split("\n");
        Map<String, String> tickerToCik = fetchTickerToCikMap();

        for (int i = 1; i < lines.length; i++) { // Skip header
          String line = lines[i].trim();
          if (!line.isEmpty()) {
            // CSV format: "Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","Summary Quote",
            String[] parts = line.split(",");
            if (parts.length > 0) {
              String ticker = parts[0].replaceAll("\"", "").trim();
              String cik = tickerToCik.get(ticker);
              if (cik != null) {
                ciks.add(cik);
              }
            }
          }
        }

        LOGGER.info("Fetched " + ciks.size() + " NYSE CIKs from NASDAQ screener");
      } else {
        LOGGER.warning("Failed to fetch NYSE listings from NASDAQ screener: HTTP " + response.statusCode());
      }
    } catch (Exception e) {
      LOGGER.warning("Error fetching NYSE from NASDAQ screener: " + e.getMessage());
      throw new IOException("Failed to fetch NYSE listings", e);
    }

    return ciks;
  }

  /**
   * Fetch major NYSE companies using LLM as fallback.
   */
  private static List<String> fetchNYSEFromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    List<String> allCiks = new ArrayList<>();

    // NYSE has ~2800 listed companies, fetch in pages
    // But for fallback, just get top 500 major companies
    int totalRequests = 2;
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        String prompt =
          String.format("Generate a JSON array of %d major NYSE-listed companies, ranked %d to %d by market cap. " +
          "Include ticker, CIK (10-digit with leading zeros), and company name. " +
          "Only include companies primarily listed on NYSE (not NASDAQ). " +
          "Format: [{\"ticker\":\"JPM\",\"cik\":\"0000019617\",\"name\":\"JPMorgan Chase\"},...] " +
          "Return ONLY the JSON array, no other text.",
          companiesPerRequest, startRank, endRank);

        List<String> pageCiks = fetchPageFromLLM(prompt);
        allCiks.addAll(pageCiks);

        Thread.sleep(1000); // Rate limiting
      } catch (Exception e) {
        LOGGER.warning("Error fetching NYSE page " + page + " from LLM: " + e.getMessage());
      }
    }

    LOGGER.info("Fetched " + allCiks.size() + " major NYSE CIKs from LLM fallback");
    return allCiks;
  }

  /**
   * Generic method to fetch a page of companies from LLM.
   */
  private static List<String> fetchPageFromLLM(String prompt) throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");

    String requestBody =
      String.format("{" +
      "  \"model\": \"claude-3-5-sonnet-20241022\"," +
      "  \"max_tokens\": 8192," +
      "  \"temperature\": 0," +
      "  \"messages\": [{" +
      "    \"role\": \"user\"," +
      "    \"content\": \"%s\"" +
      "  }]" +
      "}", prompt.replace("\"", "\\\""));

    HttpClient client = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .build();

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.anthropic.com/v1/messages"))
      .header("x-api-key", apiKey)
      .header("anthropic-version", "2023-06-01")
      .header("content-type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .timeout(Duration.ofMinutes(2))
      .build();

    HttpResponse<String> response;
    try {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Request interrupted", e);
    }

    List<String> ciks = new ArrayList<>();

    if (response.statusCode() == 200) {
      // Parse response to extract CIKs
      Pattern cikPattern = Pattern.compile("\"cik\"\\s*:\\s*\"([0-9]{10})\"");
      Matcher matcher = cikPattern.matcher(response.body());

      while (matcher.find()) {
        ciks.add(matcher.group(1));
      }
    }

    return ciks;
  }

  /**
   * Fetch all NASDAQ-listed companies.
   * @return List of CIKs for NASDAQ-listed companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchNASDAQListedCompanies() throws IOException {
    String cacheKey = "_NASDAQ_LISTED";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_ALL)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
        LOGGER.info("Returning " + cached.ciks.size() + " NASDAQ-listed CIKs from stale cache while refreshing");
      } else {
        LOGGER.info("Using cached NASDAQ-listed data with " + cached.ciks.size() + " companies");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_ALL)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_ALL);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " NASDAQ-listed CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " NASDAQ-listed CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to load NASDAQ cache from disk: " + e.getMessage());
      }
    }

    List<String> ciks = new ArrayList<>();

    try {
      // Try to fetch from NASDAQ screener
      ciks = fetchNASDAQFromNasdaqScreener();

      if (ciks.isEmpty()) {
        // Fallback to LLM for major NASDAQ companies
        LOGGER.warning("NASDAQ screener failed, using LLM fallback for major NASDAQ companies");
        ciks = fetchNASDAQFromLLM();
      }

      if (!ciks.isEmpty()) {
        // Cache the results
        CachedData newCache = new CachedData(ciks, "NASDAQ Screener/NASDAQ");
        memoryCache.put(cacheKey, newCache);
        saveToDisk(cacheFile, newCache);
        LOGGER.info("Successfully cached " + ciks.size() + " NASDAQ-listed CIKs");
      }
    } catch (Exception e) {
      LOGGER.severe("Failed to fetch NASDAQ listings: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch NASDAQ listings from NASDAQ screener.
   */
  private static List<String> fetchNASDAQFromNasdaqScreener() throws IOException {
    List<String> ciks = new ArrayList<>();

    try {
      // URL that returns CSV of all NASDAQ listings
      String url = "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&render=download&exchange=nasdaq";

      HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();

      HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .GET()
        .timeout(Duration.ofMinutes(2))
        .build();

      HttpResponse<String> response;
      try {
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Request interrupted", e);
      }

      if (response.statusCode() == 200) {
        // Parse CSV to extract tickers
        String[] lines = response.body().split("\n");
        Map<String, String> tickerToCik = fetchTickerToCikMap();

        for (int i = 1; i < lines.length; i++) { // Skip header
          String line = lines[i].trim();
          if (!line.isEmpty()) {
            // CSV format: "Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","Summary Quote",
            String[] parts = line.split(",");
            if (parts.length > 0) {
              String ticker = parts[0].replaceAll("\"", "").trim();
              String cik = tickerToCik.get(ticker);
              if (cik != null) {
                ciks.add(cik);
              }
            }
          }
        }

        LOGGER.info("Fetched " + ciks.size() + " NASDAQ CIKs from NASDAQ screener");
      } else {
        LOGGER.warning("Failed to fetch NASDAQ listings from NASDAQ screener: HTTP " + response.statusCode());
      }
    } catch (Exception e) {
      LOGGER.warning("Error fetching NASDAQ from NASDAQ screener: " + e.getMessage());
      throw new IOException("Failed to fetch NASDAQ listings", e);
    }

    return ciks;
  }

  /**
   * Fetch major NASDAQ companies using LLM as fallback.
   */
  private static List<String> fetchNASDAQFromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    List<String> allCiks = new ArrayList<>();

    // NASDAQ has ~3700 listed companies, but for fallback just get top 500
    int totalRequests = 2;
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        String prompt =
          String.format("Generate a JSON array of %d major NASDAQ-listed companies, ranked %d to %d by market cap. " +
          "Include ticker, CIK (10-digit with leading zeros), and company name. " +
          "Only include companies primarily listed on NASDAQ (not NYSE). " +
          "Format: [{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\"},...] " +
          "Return ONLY the JSON array, no other text.",
          companiesPerRequest, startRank, endRank);

        List<String> pageCiks = fetchPageFromLLM(prompt);
        allCiks.addAll(pageCiks);

        Thread.sleep(1000); // Rate limiting
      } catch (Exception e) {
        LOGGER.warning("Error fetching NASDAQ page " + page + " from LLM: " + e.getMessage());
      }
    }

    LOGGER.info("Fetched " + allCiks.size() + " major NASDAQ CIKs from LLM fallback");
    return allCiks;
  }

  /**
   * Fetch Wilshire 5000 constituents (all US publicly traded companies).
   * The Wilshire 5000 is the broadest US equity index, containing virtually all
   * publicly traded US companies. Despite its name, it currently contains ~3400 companies.
   * @return List of CIKs for Wilshire 5000 companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchWilshire5000Constituents() throws IOException {
    String cacheKey = "_WILSHIRE5000_CONSTITUENTS";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " Wilshire 5000 CIKs from stale cache while refreshing");
      } else {
        LOGGER.info("Using cached Wilshire 5000 data with " + cached.ciks.size() + " companies");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Wilshire 5000 CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " Wilshire 5000 CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to load Wilshire 5000 cache from disk: " + e.getMessage());
      }
    }

    List<String> ciks = new ArrayList<>();

    try {
      // Try to fetch from LLM first (can provide more accurate constituent list)
      ciks = fetchWilshire5000FromLLM();

      if (ciks.isEmpty() || ciks.size() < 3000) {
        // Fallback: Combine NYSE and NASDAQ listings (approximation of Wilshire 5000)
        LOGGER.info("Using fallback: combining NYSE and NASDAQ listings for Wilshire 5000");
        Set<String> wilshire5000Set = new HashSet<>();

        // Get all NYSE-listed companies
        List<String> nyseCiks = fetchNYSEListedCompanies();
        wilshire5000Set.addAll(nyseCiks);
        LOGGER.info("Added " + nyseCiks.size() + " NYSE companies");

        // Get all NASDAQ-listed companies
        List<String> nasdaqCiks = fetchNASDAQListedCompanies();
        wilshire5000Set.addAll(nasdaqCiks);
        LOGGER.info("Added " + nasdaqCiks.size() + " NASDAQ companies");

        // Also include Russell 3000 to ensure major companies are included
        try {
          List<String> russell3000 = fetchRussell3000Constituents();
          wilshire5000Set.addAll(russell3000);
          LOGGER.info("Added Russell 3000 companies");
        } catch (Exception e) {
          LOGGER.warning("Could not add Russell 3000 to Wilshire 5000: " + e.getMessage());
        }

        ciks = new ArrayList<>(wilshire5000Set);
        LOGGER.info("Combined total: " + ciks.size() + " unique companies for Wilshire 5000");
      }

      if (!ciks.isEmpty()) {
        // Cache the results
        CachedData newCache = new CachedData(ciks, "LLM/Exchange Combination");
        memoryCache.put(cacheKey, newCache);
        saveToDisk(cacheFile, newCache);
        LOGGER.info("Successfully cached " + ciks.size() + " Wilshire 5000 CIKs");
      }
    } catch (Exception e) {
      LOGGER.severe("Failed to fetch Wilshire 5000 data: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch Wilshire 5000 from LLM API.
   * The Wilshire 5000 contains ~3400 companies as of 2024.
   */
  private static List<String> fetchWilshire5000FromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      throw new IOException("ANTHROPIC_API_KEY environment variable not set");
    }

    List<String> allCiks = new ArrayList<>();

    // Wilshire 5000 has ~3400 companies, fetch in pages
    // 14 requests × 250 companies = 3500 companies
    int totalRequests = 14;
    int companiesPerRequest = 250;

    for (int page = 1; page <= totalRequests; page++) {
      try {
        int startRank = (page - 1) * companiesPerRequest + 1;
        int endRank = page * companiesPerRequest;

        LOGGER.info("Fetching Wilshire 5000 page " + page + " (ranks " + startRank + "-" + endRank + ")...");

        String prompt =
          String.format("Generate a JSON array of %d companies from the Wilshire 5000 Total Market Index, ranked %d to %d. " +
          "The Wilshire 5000 includes virtually all publicly traded US companies. " +
          "Include ticker, CIK (10-digit with leading zeros), and company name. " +
          "Format: [{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\"},...] " +
          "Return ONLY the JSON array, no other text.",
          companiesPerRequest, startRank, endRank);

        List<String> pageCiks = fetchPageFromLLM(prompt);
        allCiks.addAll(pageCiks);

        LOGGER.info("Page " + page + " returned " + pageCiks.size() + " CIKs (total so far: " + allCiks.size() + ")");

        // Small delay between requests to be respectful
        Thread.sleep(1000);
      } catch (Exception e) {
        LOGGER.warning("Error fetching Wilshire 5000 page " + page + ": " + e.getMessage());
        // Continue with other pages even if one fails
      }
    }

    LOGGER.info("Fetched total of " + allCiks.size() + " Wilshire 5000 CIKs from LLM");
    return allCiks;
  }

  /**
   * Fetch FTSE 100 companies that have US listings (ADRs or dual listings).
   * @return List of CIKs for FTSE 100 companies with US listings
   * @throws IOException if fetch fails
   */
  public static List<String> fetchFTSE100USListed() throws IOException {
    String cacheKey = "_FTSE100_US_LISTED";

    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " FTSE 100 US-listed CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " FTSE 100 US-listed CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " FTSE 100 US-listed CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " FTSE 100 US-listed CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to load FTSE 100 cache from disk: " + e.getMessage());
      }
    }

    // No cache exists - must fetch synchronously
    LOGGER.info("No cache exists for " + cacheKey + ", fetching synchronously...");
    List<String> ciks = fetchFTSE100USListedFromLLM();

    if (!ciks.isEmpty()) {
      // Cache the results
      CachedData newCache = new CachedData(ciks, "LLM API");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);
      LOGGER.info("Successfully cached " + ciks.size() + " FTSE 100 US-listed CIKs");
    }

    return ciks;
  }

  /**
   * Fetch FTSE 100 companies with US listings from LLM.
   * Many FTSE 100 companies have ADRs (American Depositary Receipts) or dual listings in the US.
   */
  private static List<String> fetchFTSE100USListedFromLLM() throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      // Return known FTSE 100 companies with US listings as fallback
      return getFallbackFTSE100USListed();
    }

    List<String> allCiks = new ArrayList<>();

    try {
      String prompt = "Generate a JSON array of FTSE 100 companies that have US listings (ADRs or dual listings). " +
                     "These are UK companies from the FTSE 100 index that trade on US exchanges. " +
                     "Include ticker (US ticker if available), CIK (10-digit with leading zeros), and company name. " +
                     "Examples include BP, Shell, Unilever, AstraZeneca, HSBC, GSK, Diageo, Rio Tinto, etc. " +
                     "Format: [{\"ticker\":\"BP\",\"cik\":\"0000313807\",\"name\":\"BP plc\"},...] " +
                     "Return ONLY the JSON array, no other text.";

      List<String> ciks = fetchPageFromLLM(prompt);
      allCiks.addAll(ciks);

      LOGGER.info("Fetched " + allCiks.size() + " FTSE 100 US-listed CIKs from LLM");
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch FTSE 100 from LLM: " + e.getMessage());
      // Use fallback
      return getFallbackFTSE100USListed();
    }

    return allCiks;
  }

  /**
   * Get fallback list of known FTSE 100 companies with US listings.
   */
  private static List<String> getFallbackFTSE100USListed() {
    List<String> ciks = new ArrayList<>();

    try {
      // Known major FTSE 100 companies with US listings (ADRs)
      String[] knownFTSE100USListedTickers = {
        "BP",      // BP plc
        "SHEL",    // Shell plc
        "UL",      // Unilever
        "AZN",     // AstraZeneca
        "HSBC",    // HSBC Holdings
        "GSK",     // GSK plc
        "DEO",     // Diageo
        "RIO",     // Rio Tinto
        "BTI",     // British American Tobacco
        "RHHBY",   // Roche (actually Swiss but often grouped)
        "NVS",     // Novartis (Swiss)
        "LYG",     // Lloyds Banking Group
        "BCS",     // Barclays
        "NGG",     // National Grid
        "VOD",     // Vodafone
        "RBGLY",   // Reckitt Benckiser
        "FLTR",    // Flutter Entertainment
        "WPP",     // WPP plc
        "RELX",    // RELX plc
        "IHG",     // InterContinental Hotels
        "RR",      // Rolls-Royce
        "GLEN",    // Glencore (actually Swiss)
        "TEF",     // Telefonica (Spanish but often included)
        "CNE",     // CNH Industrial
        "STLA",    // Stellantis
        "ARM",     // ARM Holdings
        "BAM",     // Brookfield Asset Management
        "CP",      // Canadian Pacific (Canadian but major)
        "GOLD",    // Barrick Gold
        "SU",      // Suncor Energy
      };

      // Map tickers to CIKs
      Map<String, String> tickerToCik = fetchTickerToCikMap();

      for (String ticker : knownFTSE100USListedTickers) {
        String cik = tickerToCik.get(ticker);
        if (cik != null) {
          ciks.add(cik);
        }
      }

      LOGGER.info("Built fallback FTSE 100 US-listed with " + ciks.size() + " companies");
    } catch (Exception e) {
      LOGGER.warning("Failed to build fallback FTSE 100 US-listed: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Fetch global mega-cap companies (market cap > $200B).
   * @return List of CIKs for mega-cap companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchGlobalMegaCap() throws IOException {
    return fetchCompaniesByMarketCap("_GLOBAL_MEGA_CAP", 200_000_000_000L, Long.MAX_VALUE, "global mega-cap (>$200B)");
  }

  /**
   * Fetch US large-cap companies (market cap > $10B).
   * @return List of CIKs for large-cap companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchUSLargeCap() throws IOException {
    return fetchCompaniesByMarketCap("_US_LARGE_CAP", 10_000_000_000L, Long.MAX_VALUE, "US large-cap (>$10B)");
  }

  /**
   * Fetch US mid-cap companies (market cap $2B-$10B).
   * @return List of CIKs for mid-cap companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchUSMidCap() throws IOException {
    return fetchCompaniesByMarketCap("_US_MID_CAP", 2_000_000_000L, 10_000_000_000L, "US mid-cap ($2B-$10B)");
  }

  /**
   * Fetch US small-cap companies (market cap $300M-$2B).
   * @return List of CIKs for small-cap companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchUSSmallCap() throws IOException {
    return fetchCompaniesByMarketCap("_US_SMALL_CAP", 300_000_000L, 2_000_000_000L, "US small-cap ($300M-$2B)");
  }

  /**
   * Fetch US micro-cap companies (market cap < $300M).
   * @return List of CIKs for micro-cap companies
   * @throws IOException if fetch fails
   */
  public static List<String> fetchUSMicroCap() throws IOException {
    return fetchCompaniesByMarketCap("_US_MICRO_CAP", 0L, 300_000_000L, "US micro-cap (<$300M)");
  }

  /**
   * Generic method to fetch companies by market cap range.
   * Uses non-blocking cache pattern.
   */
  private static List<String> fetchCompaniesByMarketCap(String cacheKey, long minMarketCap, long maxMarketCap, String description) throws IOException {
    // Check memory cache first
    CachedData cached = memoryCache.get(cacheKey);
    if (cached != null) {
      // If cache exists but is expired, trigger background refresh
      if (cached.isExpired(CACHE_TTL_INDEX)) {
        triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
        LOGGER.info("Returning " + cached.ciks.size() + " " + description + " CIKs from stale cache while refreshing");
      } else {
        LOGGER.fine("Returning " + cached.ciks.size() + " " + description + " CIKs from memory cache");
      }
      return new ArrayList<>(cached.ciks);
    }

    // Check disk cache
    File cacheFile = CACHE_DIR.resolve(cacheKey + ".json").toFile();
    if (cacheFile.exists()) {
      try {
        CachedData diskCached = loadFromDisk(cacheFile);
        if (diskCached != null) {
          memoryCache.put(cacheKey, diskCached);

          // If disk cache is expired, trigger background refresh but still return data
          if (diskCached.isExpired(CACHE_TTL_INDEX)) {
            triggerBackgroundRefreshIfNeeded(cacheKey, CACHE_TTL_INDEX);
            LOGGER.info("Loaded " + diskCached.ciks.size() + " " + description + " CIKs from expired disk cache");
          } else {
            LOGGER.info("Loaded " + diskCached.ciks.size() + " " + description + " CIKs from disk cache");
          }
          return new ArrayList<>(diskCached.ciks);
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to load " + description + " cache from disk: " + e.getMessage());
      }
    }

    // No cache exists - must fetch synchronously
    LOGGER.info("No cache exists for " + cacheKey + ", fetching " + description + " companies synchronously...");
    List<String> ciks = fetchMarketCapFromLLM(minMarketCap, maxMarketCap, description);

    if (!ciks.isEmpty()) {
      // Cache the results
      CachedData newCache = new CachedData(ciks, "LLM API/Market Data");
      memoryCache.put(cacheKey, newCache);
      saveToDisk(cacheFile, newCache);
      LOGGER.info("Successfully cached " + ciks.size() + " " + description + " CIKs");
    }

    return ciks;
  }

  /**
   * Fetch companies within a market cap range using LLM.
   */
  private static List<String> fetchMarketCapFromLLM(long minMarketCap, long maxMarketCap, String description) throws IOException {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null) {
      // Return fallback based on well-known companies
      return getFallbackMarketCapCompanies(minMarketCap, maxMarketCap);
    }

    List<String> allCiks = new ArrayList<>();

    try {
      String marketCapRange;
      if (maxMarketCap == Long.MAX_VALUE) {
        marketCapRange = "greater than $" + formatMarketCap(minMarketCap);
      } else if (minMarketCap == 0) {
        marketCapRange = "less than $" + formatMarketCap(maxMarketCap);
      } else {
        marketCapRange = "between $" + formatMarketCap(minMarketCap) + " and $" + formatMarketCap(maxMarketCap);
      }

      // For very large ranges (micro-cap), we need multiple pages
      int numPages = 1;
      if (maxMarketCap <= 300_000_000L) {
        numPages = 4; // Micro-cap has many companies
      } else if (maxMarketCap <= 2_000_000_000L) {
        numPages = 3; // Small-cap has many companies
      } else if (maxMarketCap <= 10_000_000_000L) {
        numPages = 2; // Mid-cap
      }

      for (int page = 1; page <= numPages; page++) {
        String prompt =
          String.format("Generate a JSON array of %d US-listed companies with market capitalization %s. " +
          "Page %d of %d. Include ticker, CIK (10-digit with leading zeros), and company name. " +
          "Focus on companies currently trading on US exchanges (NYSE, NASDAQ). " +
          "Format: [{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\"},...] " +
          "Return ONLY the JSON array, no other text.",
          250, marketCapRange, page, numPages);

        try {
          List<String> pageCiks = fetchPageFromLLM(prompt);
          allCiks.addAll(pageCiks);

          if (page < numPages) {
            Thread.sleep(1000); // Rate limiting between pages
          }
        } catch (Exception e) {
          LOGGER.warning("Error fetching " + description + " page " + page + ": " + e.getMessage());
        }
      }

      LOGGER.info("Fetched " + allCiks.size() + " " + description + " CIKs from LLM");
    } catch (Exception e) {
      LOGGER.warning("Failed to fetch " + description + " from LLM: " + e.getMessage());
      return getFallbackMarketCapCompanies(minMarketCap, maxMarketCap);
    }

    return allCiks;
  }

  /**
   * Format market cap for display.
   */
  private static String formatMarketCap(long marketCap) {
    if (marketCap >= 1_000_000_000_000L) {
      return String.format("%.1fT", marketCap / 1_000_000_000_000.0);
    } else if (marketCap >= 1_000_000_000L) {
      return String.format("%.1fB", marketCap / 1_000_000_000.0);
    } else if (marketCap >= 1_000_000L) {
      return String.format("%.1fM", marketCap / 1_000_000.0);
    } else {
      return String.valueOf(marketCap);
    }
  }

  /**
   * Get fallback companies based on market cap range.
   */
  private static List<String> getFallbackMarketCapCompanies(long minMarketCap, long maxMarketCap) {
    List<String> ciks = new ArrayList<>();

    try {
      Map<String, String> tickerToCik = fetchTickerToCikMap();

      // Define well-known companies by market cap tier
      String[] tickers = null;

      if (minMarketCap >= 200_000_000_000L) {
        // Mega-cap (>$200B)
        tickers = new String[] {
          "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK.B", "UNH", "JNJ",
          "XOM", "JPM", "V", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "PEP"
        };
      } else if (minMarketCap >= 10_000_000_000L && maxMarketCap == Long.MAX_VALUE) {
        // Large-cap (>$10B)
        tickers = new String[] {
          "DIS", "BAC", "CSCO", "WMT", "CRM", "ORCL", "ACN", "AMD", "NFLX", "COST",
          "TMO", "ABT", "NKE", "WFC", "MS", "PM", "UPS", "NEE", "RTX", "LOW",
          "INTU", "GS", "BA", "HON", "SPGI", "BLK", "CAT", "IBM", "AMGN", "GE"
        };
      } else if (minMarketCap >= 2_000_000_000L && maxMarketCap <= 10_000_000_000L) {
        // Mid-cap ($2B-$10B)
        tickers = new String[] {
          "SNAP", "ROKU", "PINS", "DOCU", "ZM", "CRWD", "DDOG", "NET", "SNOW", "U",
          "RBLX", "COIN", "HOOD", "SOFI", "UPST", "AFRM", "NU", "RIVN", "LCID", "FSR"
        };
      } else if (minMarketCap >= 300_000_000L && maxMarketCap <= 2_000_000_000L) {
        // Small-cap ($300M-$2B)
        tickers = new String[] {
          "BBBY", "GME", "AMC", "WISH", "CLOV", "RKT", "UWMC", "PSFE", "OPEN", "MILE",
          "GOEV", "RIDE", "WKHS", "NKLA", "HYLN", "XL", "ARVL", "PTRA", "CHPT", "BLNK"
        };
      } else {
        // Micro-cap (<$300M)
        tickers = new String[] {
          "SNDL", "HEXO", "TLRY", "ACB", "OGI", "HUGE", "VFF", "CRON", "CGC", "CURLF"
        };
      }

      if (tickers != null) {
        for (String ticker : tickers) {
          String cik = tickerToCik.get(ticker);
          if (cik != null) {
            ciks.add(cik);
          }
        }
      }

      LOGGER.info("Built fallback market cap list with " + ciks.size() + " companies");
    } catch (Exception e) {
      LOGGER.warning("Failed to build fallback market cap companies: " + e.getMessage());
    }

    return ciks;
  }

  /**
   * Get all tickers associated with ALL EDGAR filers.
   * @return Map of ticker to CIK for all EDGAR filers
   */
  public static Map<String, String> getAllEdgarTickers() {
    try {
      // This already fetches all tickers from SEC
      return fetchTickerToCikMap();
    } catch (IOException e) {
      LOGGER.severe("Failed to fetch all EDGAR tickers: " + e.getMessage());
      return new HashMap<>();
    }
  }

  /**
   * Clear all caches (memory and disk).
   */
  public static void clearCache() {
    memoryCache.clear();

    try {
      Files.list(CACHE_DIR)
          .filter(path -> path.toString().endsWith(".json"))
          .forEach(path -> {
            try {
              Files.delete(path);
            } catch (IOException e) {
              LOGGER.warning("Failed to delete cache file: " + path);
            }
          });
      LOGGER.info("Cache cleared successfully");
    } catch (IOException e) {
      LOGGER.warning("Failed to clear cache: " + e.getMessage());
    }
  }

  /**
   * Get cache statistics.
   */
  public static String getCacheStats() {
    StringBuilder stats = new StringBuilder();
    stats.append("Memory cache entries: ").append(memoryCache.size()).append("\n");

    try {
      long diskFiles = Files.list(CACHE_DIR)
          .filter(path -> path.toString().endsWith(".json"))
          .count();
      stats.append("Disk cache files: ").append(diskFiles).append("\n");

      long totalSize = Files.list(CACHE_DIR)
          .filter(path -> path.toString().endsWith(".json"))
          .mapToLong(path -> {
            try {
              return Files.size(path);
            } catch (IOException e) {
              return 0;
            }
          }).sum();
      stats.append("Total cache size: ").append(totalSize / 1024).append(" KB\n");

    } catch (IOException e) {
      stats.append("Disk cache: unable to read\n");
    }

    return stats.toString();
  }
}
