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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Monitors SEC RSS feeds for new filings and triggers debounced refreshes.
 * This only serves as a refresh trigger - RSS data is not stored as a table.
 * When new filings are detected, it calls the existing downloadSecData() method.
 */
public class RSSRefreshMonitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RSSRefreshMonitor.class);
  
  private final Map<String, Object> operand;
  private final ScheduledExecutorService scheduler;
  private final int checkIntervalMinutes;
  private final int debounceMinutes;
  private final int maxDebounceMinutes;
  
  // Debouncing state
  private final Set<String> lastSeenFilings = new HashSet<>();
  private final Set<String> pendingFilings = new HashSet<>();
  private ScheduledFuture<?> debouncedRefresh;
  private long firstTriggerTime = 0;
  private volatile boolean shutdown = false;
  
  // RSS URL patterns
  private static final String SEC_RSS_RECENT = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom";
  private static final String SEC_RSS_COMPANY = "https://data.sec.gov/rss?cik=%s";
  
  // Pattern to extract filing identifiers from RSS
  private static final Pattern FILING_ID_PATTERN = Pattern.compile("accession[Nn]umber=(\\d{10}-\\d{2}-\\d{6})");

  public RSSRefreshMonitor(Map<String, Object> operand) {
    this.operand = operand;
    this.scheduler = Executors.newScheduledThreadPool(1);
    
    Map<String, Object> refreshConfig = getRefreshConfig(operand);
    this.checkIntervalMinutes = getIntConfig(refreshConfig, "checkIntervalMinutes", 5);
    this.debounceMinutes = getIntConfig(refreshConfig, "debounceMinutes", 10);
    this.maxDebounceMinutes = getIntConfig(refreshConfig, "maxDebounceMinutes", 30);
  }

  public void start() {
    if (!isEnabled()) {
      LOGGER.info("RSS refresh monitoring is disabled");
      return;
    }

    LOGGER.info("Starting RSS refresh monitor - check every {} min, debounce {} min, max {} min", 
               checkIntervalMinutes, debounceMinutes, maxDebounceMinutes);
    
    scheduler.scheduleAtFixedRate(this::checkForUpdates, 0, checkIntervalMinutes, TimeUnit.MINUTES);
  }

  public void shutdown() {
    shutdown = true;
    if (debouncedRefresh != null && !debouncedRefresh.isDone()) {
      debouncedRefresh.cancel(false);
    }
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void checkForUpdates() {
    if (shutdown) {
      return;
    }

    try {
      Set<String> currentFilings = fetchRSSFilings();
      Set<String> newFilings = new HashSet<>(currentFilings);
      newFilings.removeAll(lastSeenFilings);
      
      if (newFilings.isEmpty()) {
        // NO TRIGGERS = NO PROCESSING
        return;
      }

      LOGGER.info("Detected {} new filings from RSS", newFilings.size());
      
      synchronized (this) {
        pendingFilings.addAll(newFilings);
        long now = System.currentTimeMillis();

        if (firstTriggerTime == 0) {
          firstTriggerTime = now;
          LOGGER.info("First new filing detected, starting debounce timer");
        }

        // Cancel existing timer (debounce behavior - wait for quiet period)
        if (debouncedRefresh != null && !debouncedRefresh.isDone()) {
          debouncedRefresh.cancel(false);
        }

        // Check if we've hit max wait time
        long timeSinceFirst = now - firstTriggerTime;
        if (timeSinceFirst >= TimeUnit.MINUTES.toMillis(maxDebounceMinutes)) {
          LOGGER.info("Max debounce time reached after {} minutes, executing refresh immediately", 
                     timeSinceFirst / 60000);
          executeRefresh();
        } else {
          // Schedule refresh for debounce period from now
          long delay = TimeUnit.MINUTES.toMillis(debounceMinutes);
          debouncedRefresh = scheduler.schedule(() -> {
            executeRefresh();
          }, delay, TimeUnit.MILLISECONDS);

          LOGGER.info("Debounce timer reset, {} pending filings will refresh in {} minutes", 
                     pendingFilings.size(), debounceMinutes);
        }

        // Update last seen filings
        lastSeenFilings.addAll(newFilings);
      }
      
    } catch (Exception e) {
      LOGGER.warn("Error checking RSS feeds: {}", e.getMessage());
    }
  }

  private void executeRefresh() {
    if (shutdown || pendingFilings.isEmpty()) {
      return;
    }

    LOGGER.info("Executing refresh for {} accumulated filings after quiet period", 
               pendingFilings.size());

    try {
      // Trigger the existing download process - it will use manifest to deduplicate
      SecSchemaFactory.triggerDownload(operand);
      LOGGER.info("RSS-triggered refresh completed successfully");
    } catch (Exception e) {
      LOGGER.error("Error during RSS-triggered refresh: {}", e.getMessage(), e);
    }

    // Reset state
    synchronized (this) {
      pendingFilings.clear();
      firstTriggerTime = 0;
      debouncedRefresh = null;
    }
  }

  private Set<String> fetchRSSFilings() throws Exception {
    Set<String> filings = new HashSet<>();
    
    // Fetch recent filings RSS
    filings.addAll(fetchRSSFeed(SEC_RSS_RECENT));
    
    // If specific CIKs are configured, also fetch company-specific RSS
    if (operand.containsKey("ciks")) {
      Object ciksObj = operand.get("ciks");
      if (ciksObj instanceof String[]) {
        String[] ciks = (String[]) ciksObj;
        for (String cik : ciks) {
          try {
            String url = String.format(SEC_RSS_COMPANY, cik);
            filings.addAll(fetchRSSFeed(url));
          } catch (Exception e) {
            LOGGER.debug("Error fetching RSS for CIK {}: {}", cik, e.getMessage());
          }
        }
      }
    }
    
    return filings;
  }

  private Set<String> fetchRSSFeed(String rssUrl) throws Exception {
    Set<String> filings = new HashSet<>();
    
    @SuppressWarnings("deprecation")
    HttpURLConnection conn = (HttpURLConnection) new URL(rssUrl).openConnection();
    conn.setRequestProperty("User-Agent", "Calcite-SEC-Adapter/1.0");
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(30000);
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        Matcher matcher = FILING_ID_PATTERN.matcher(line);
        while (matcher.find()) {
          filings.add(matcher.group(1));
        }
      }
    }
    
    return filings;
  }

  private boolean isEnabled() {
    Map<String, Object> refreshConfig = getRefreshConfig(operand);
    return refreshConfig != null && 
           getBooleanConfig(refreshConfig, "enabled", false);
  }

  private Map<String, Object> getRefreshConfig(Map<String, Object> operand) {
    if (operand.containsKey("refreshMonitoring")) {
      return (Map<String, Object>) operand.get("refreshMonitoring");
    }
    return null;
  }

  private int getIntConfig(Map<String, Object> config, String key, int defaultValue) {
    if (config != null && config.containsKey(key)) {
      return Integer.parseInt(config.get(key).toString());
    }
    return defaultValue;
  }

  private boolean getBooleanConfig(Map<String, Object> config, String key, boolean defaultValue) {
    if (config != null && config.containsKey(key)) {
      return Boolean.parseBoolean(config.get(key).toString());
    }
    return defaultValue;
  }
}