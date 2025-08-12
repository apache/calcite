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
package org.apache.calcite.adapter.file.converters;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration for HTML crawler behavior.
 */
public class CrawlerConfiguration {
  private boolean enabled = false;
  private int maxDepth = 0; // Default: no crawling beyond initial page
  private @Nullable Pattern linkPattern; // Regex for allowed links
  private Set<String> allowedFileExtensions = new HashSet<>();
  private Set<String> allowedDomains = new HashSet<>();
  private Duration requestDelay = Duration.ofSeconds(1);
  private int maxPages = 100;
  private boolean followExternalLinks = false;
  private Set<String> excludePatterns = new HashSet<>();
  
  // Content size limits
  private long maxHtmlSize = 10L * 1024 * 1024; // 10MB for HTML pages
  private long maxDataFileSize = 100L * 1024 * 1024; // 100MB for data files
  private Map<String, Long> extensionSizeLimits = new HashMap<>();
  private boolean enforceContentLengthHeader = true;
  
  // Caching settings
  private Duration dataFileCacheTTL = Duration.ofHours(1);
  private Duration htmlCacheTTL = Duration.ofMinutes(30);
  private boolean honorHttpCacheHeaders = true;
  
  // Refresh settings
  private @Nullable Duration refreshInterval;
  
  public CrawlerConfiguration() {
    // Initialize default extension size limits
    extensionSizeLimits.put("html", 10L * 1024 * 1024);    // 10MB
    extensionSizeLimits.put("csv", 50L * 1024 * 1024);     // 50MB
    extensionSizeLimits.put("xlsx", 100L * 1024 * 1024);   // 100MB
    extensionSizeLimits.put("xls", 100L * 1024 * 1024);    // 100MB
    extensionSizeLimits.put("docx", 50L * 1024 * 1024);    // 50MB
    extensionSizeLimits.put("pptx", 100L * 1024 * 1024);   // 100MB
    extensionSizeLimits.put("pdf", 25L * 1024 * 1024);     // 25MB
    extensionSizeLimits.put("json", 20L * 1024 * 1024);    // 20MB
    extensionSizeLimits.put("parquet", 200L * 1024 * 1024); // 200MB
    
    // Default allowed file extensions for data files
    allowedFileExtensions.add("csv");
    allowedFileExtensions.add("xlsx");
    allowedFileExtensions.add("xls");
    allowedFileExtensions.add("json");
    allowedFileExtensions.add("tsv");
    allowedFileExtensions.add("parquet");
    allowedFileExtensions.add("docx");
    allowedFileExtensions.add("pptx");
  }
  
  /**
   * Creates a configuration from a map of options.
   */
  public static CrawlerConfiguration fromMap(Map<String, Object> options) {
    CrawlerConfiguration config = new CrawlerConfiguration();
    
    if (options.containsKey("enabled")) {
      config.setEnabled(Boolean.parseBoolean(options.get("enabled").toString()));
    }
    
    if (options.containsKey("maxDepth")) {
      config.setMaxDepth(Integer.parseInt(options.get("maxDepth").toString()));
    }
    
    if (options.containsKey("linkPattern")) {
      config.setLinkPattern(Pattern.compile(options.get("linkPattern").toString()));
    }
    
    if (options.containsKey("maxPages")) {
      config.setMaxPages(Integer.parseInt(options.get("maxPages").toString()));
    }
    
    if (options.containsKey("followExternalLinks")) {
      config.setFollowExternalLinks(Boolean.parseBoolean(options.get("followExternalLinks").toString()));
    }
    
    if (options.containsKey("maxHtmlSize")) {
      config.setMaxHtmlSize(parseSize(options.get("maxHtmlSize").toString()));
    }
    
    if (options.containsKey("maxDataFileSize")) {
      config.setMaxDataFileSize(parseSize(options.get("maxDataFileSize").toString()));
    }
    
    if (options.containsKey("dataFileCacheTTL")) {
      config.setDataFileCacheTTL(parseDuration(options.get("dataFileCacheTTL").toString()));
    }
    
    if (options.containsKey("refreshInterval")) {
      config.setRefreshInterval(parseDuration(options.get("refreshInterval").toString()));
    }
    
    if (options.containsKey("allowedDomains")) {
      Object domains = options.get("allowedDomains");
      if (domains instanceof String) {
        config.addAllowedDomain((String) domains);
      } else if (domains instanceof Iterable) {
        for (Object domain : (Iterable<?>) domains) {
          config.addAllowedDomain(domain.toString());
        }
      }
    }
    
    return config;
  }
  
  private static long parseSize(String sizeStr) {
    sizeStr = sizeStr.trim().toUpperCase();
    if (sizeStr.endsWith("KB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024;
    } else if (sizeStr.endsWith("MB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024 * 1024;
    } else if (sizeStr.endsWith("GB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024 * 1024 * 1024;
    }
    return Long.parseLong(sizeStr);
  }
  
  private static Duration parseDuration(String durationStr) {
    durationStr = durationStr.trim().toLowerCase();
    String[] parts = durationStr.split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid duration format: " + durationStr);
    }
    
    long value = Long.parseLong(parts[0]);
    String unit = parts[1];
    
    if (unit.startsWith("second")) {
      return Duration.ofSeconds(value);
    } else if (unit.startsWith("minute")) {
      return Duration.ofMinutes(value);
    } else if (unit.startsWith("hour")) {
      return Duration.ofHours(value);
    } else if (unit.startsWith("day")) {
      return Duration.ofDays(value);
    }
    
    throw new IllegalArgumentException("Unknown duration unit: " + unit);
  }
  
  // Getters and setters
  
  public boolean isEnabled() {
    return enabled;
  }
  
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
  
  public int getMaxDepth() {
    return maxDepth;
  }
  
  public void setMaxDepth(int maxDepth) {
    this.maxDepth = maxDepth;
  }
  
  public @Nullable Pattern getLinkPattern() {
    return linkPattern;
  }
  
  public void setLinkPattern(@Nullable Pattern linkPattern) {
    this.linkPattern = linkPattern;
  }
  
  public Set<String> getAllowedFileExtensions() {
    return allowedFileExtensions;
  }
  
  public void addAllowedFileExtension(String extension) {
    this.allowedFileExtensions.add(extension.toLowerCase());
  }
  
  public Set<String> getAllowedDomains() {
    return allowedDomains;
  }
  
  public void addAllowedDomain(String domain) {
    this.allowedDomains.add(domain.toLowerCase());
  }
  
  public Duration getRequestDelay() {
    return requestDelay;
  }
  
  public void setRequestDelay(Duration requestDelay) {
    this.requestDelay = requestDelay;
  }
  
  public int getMaxPages() {
    return maxPages;
  }
  
  public void setMaxPages(int maxPages) {
    this.maxPages = maxPages;
  }
  
  public boolean isFollowExternalLinks() {
    return followExternalLinks;
  }
  
  public void setFollowExternalLinks(boolean followExternalLinks) {
    this.followExternalLinks = followExternalLinks;
  }
  
  public long getMaxHtmlSize() {
    return maxHtmlSize;
  }
  
  public void setMaxHtmlSize(long maxHtmlSize) {
    this.maxHtmlSize = maxHtmlSize;
  }
  
  public long getMaxDataFileSize() {
    return maxDataFileSize;
  }
  
  public void setMaxDataFileSize(long maxDataFileSize) {
    this.maxDataFileSize = maxDataFileSize;
  }
  
  public Long getSizeLimitForExtension(String extension) {
    return extensionSizeLimits.getOrDefault(extension.toLowerCase(), maxDataFileSize);
  }
  
  public void setSizeLimitForExtension(String extension, long sizeLimit) {
    this.extensionSizeLimits.put(extension.toLowerCase(), sizeLimit);
  }
  
  public Duration getDataFileCacheTTL() {
    return dataFileCacheTTL;
  }
  
  public void setDataFileCacheTTL(Duration dataFileCacheTTL) {
    this.dataFileCacheTTL = dataFileCacheTTL;
  }
  
  public Duration getHtmlCacheTTL() {
    return htmlCacheTTL;
  }
  
  public void setHtmlCacheTTL(Duration htmlCacheTTL) {
    this.htmlCacheTTL = htmlCacheTTL;
  }
  
  public boolean isHonorHttpCacheHeaders() {
    return honorHttpCacheHeaders;
  }
  
  public void setHonorHttpCacheHeaders(boolean honorHttpCacheHeaders) {
    this.honorHttpCacheHeaders = honorHttpCacheHeaders;
  }
  
  public @Nullable Duration getRefreshInterval() {
    return refreshInterval;
  }
  
  public void setRefreshInterval(@Nullable Duration refreshInterval) {
    this.refreshInterval = refreshInterval;
  }
  
  public boolean isEnforceContentLengthHeader() {
    return enforceContentLengthHeader;
  }
  
  public void setEnforceContentLengthHeader(boolean enforceContentLengthHeader) {
    this.enforceContentLengthHeader = enforceContentLengthHeader;
  }
}