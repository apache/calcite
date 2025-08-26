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

import org.apache.calcite.adapter.file.converters.HtmlLinkCache.ExtractedLinks;
import org.apache.calcite.adapter.file.converters.HtmlTableScanner.TableInfo;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Crawler that discovers and processes HTML tables and linked data files.
 */
public class HtmlCrawler {
  private static final Logger LOGGER = Logger.getLogger(HtmlCrawler.class.getName());

  /**
   * Result of a crawl operation.
   */
  public static class CrawlResult {
    private final Map<String, List<TableInfo>> htmlTables;
    private final Map<String, File> dataFiles;
    private final Set<String> visitedUrls;
    private final Set<String> failedUrls;

    public CrawlResult() {
      this.htmlTables = new HashMap<>();
      this.dataFiles = new HashMap<>();
      this.visitedUrls = new HashSet<>();
      this.failedUrls = new HashSet<>();
    }

    public void addHtmlTables(String url, List<TableInfo> tables) {
      htmlTables.put(url, tables);
    }

    public void addDataFile(String url, File file) {
      dataFiles.put(url, file);
    }

    public void markVisited(String url) {
      visitedUrls.add(url);
    }

    public void markFailed(String url) {
      failedUrls.add(url);
    }

    public Map<String, List<TableInfo>> getHtmlTables() {
      return htmlTables;
    }

    public Map<String, File> getDataFiles() {
      return dataFiles;
    }

    public Set<String> getVisitedUrls() {
      return visitedUrls;
    }

    public Set<String> getFailedUrls() {
      return failedUrls;
    }

    public int getTotalTablesFound() {
      return htmlTables.values().stream().mapToInt(List::size).sum();
    }

    public int getTotalDataFilesFound() {
      return dataFiles.size();
    }
  }

  /**
   * Metadata cache for tracking processed resources.
   */
  private static class ResourceMetadata {
    final String contentHash;
    final long contentLength;
    final @Nullable String etag;
    final @Nullable String lastModified;
    final long processedAt;

    ResourceMetadata(String contentHash, long contentLength,
                    @Nullable String etag, @Nullable String lastModified) {
      this.contentHash = contentHash;
      this.contentLength = contentLength;
      this.etag = etag;
      this.lastModified = lastModified;
      this.processedAt = System.currentTimeMillis();
    }

    boolean isExpired(long ttlMillis) {
      return System.currentTimeMillis() - processedAt > ttlMillis;
    }
  }

  private final CrawlerConfiguration config;
  private final HtmlLinkCache linkCache;
  private final Map<String, ResourceMetadata> processedDataFiles;
  private final File tempDir;

  public HtmlCrawler(CrawlerConfiguration config) throws IOException {
    this.config = config;
    this.linkCache = new HtmlLinkCache(config);
    this.processedDataFiles = new ConcurrentHashMap<>();
    this.tempDir = Files.createTempDirectory("html-crawler-").toFile();
    this.tempDir.deleteOnExit();
  }

  /**
   * Crawls from a starting URL and discovers tables and data files.
   */
  public CrawlResult crawl(String startUrl) throws IOException {
    CrawlResult result = new CrawlResult();

    if (!config.isEnabled()) {
      // Just process the single page
      processSinglePage(startUrl, result);
      return result;
    }

    // Breadth-first crawl
    Set<String> visited = new HashSet<>();
    LinkedHashSet<String> currentLevel = new LinkedHashSet<>();
    currentLevel.add(startUrl);

    for (int depth = 0; depth <= config.getMaxDepth() && !currentLevel.isEmpty(); depth++) {
      LinkedHashSet<String> nextLevel = new LinkedHashSet<>();

      for (String url : currentLevel) {
        if (visited.contains(url)) {
          continue;
        }

        if (visited.size() >= config.getMaxPages()) {
          LOGGER.warning("Reached maximum page limit of " + config.getMaxPages());
          return result;
        }

        visited.add(url);
        result.markVisited(url);

        try {
          // Add delay between requests
          if (visited.size() > 1) {
            Thread.sleep(config.getRequestDelay().toMillis());
          }

          // Get links from page (uses cache)
          ExtractedLinks links = linkCache.getLinks(url);

          // Process HTML tables if configured
          if (config.isGenerateTablesFromHtml() && !links.getTables().isEmpty()) {
            result.addHtmlTables(url, links.getTables());
            LOGGER.info("Found " + links.getTables().size() + " tables in " + url);
          }

          // Process data file links
          for (String dataLink : links.getDataFileLinks()) {
            processDataFile(dataLink, result);
          }

          // Add HTML links for next level
          if (depth < config.getMaxDepth()) {
            nextLevel.addAll(links.getHtmlLinks());
          }

        } catch (Exception e) {
          LOGGER.log(Level.WARNING, "Failed to process " + url, e);
          result.markFailed(url);
        }
      }

      currentLevel = nextLevel;
    }

    return result;
  }

  private void processSinglePage(String url, CrawlResult result) throws IOException {
    ExtractedLinks links = linkCache.getLinks(url);

    // Add HTML tables if configured
    if (config.isGenerateTablesFromHtml() && !links.getTables().isEmpty()) {
      result.addHtmlTables(url, links.getTables());
    }

    // Process immediate data file links
    for (String dataLink : links.getDataFileLinks()) {
      processDataFile(dataLink, result);
    }

    result.markVisited(url);
  }

  private void processDataFile(String url, CrawlResult result) {
    try {
      // Check if already processed
      ResourceMetadata cached = processedDataFiles.get(url);
      if (cached != null && !cached.isExpired(config.getDataFileCacheTTL().toMillis())) {
        LOGGER.fine("Skipping already processed data file: " + url);
        return;
      }

      // Check content length before downloading
      if (config.isEnforceContentLengthHeader()) {
        long contentLength = getContentLength(url);
        String extension = getFileExtension(url);
        long maxSize = config.getSizeLimitForExtension(extension);

        if (contentLength > maxSize) {
          LOGGER.warning("Data file too large: " + url + " (" + contentLength + " bytes exceeds limit of " + maxSize + ")");
          return;
        }
      }

      // Download to temp file
      File tempFile = downloadDataFile(url);
      result.addDataFile(url, tempFile);

      // Track as processed
      HttpURLConnection conn;
      try {
        conn = (HttpURLConnection) new URI(url).toURL().openConnection();
      } catch (Exception e) {
        throw new IOException("Invalid URL: " + url, e);
      }
      conn.setRequestMethod("HEAD");
      String etag = conn.getHeaderField("ETag");
      String lastModified = conn.getHeaderField("Last-Modified");
      long contentLength = conn.getContentLengthLong();
      conn.disconnect();

      ResourceMetadata metadata =
          new ResourceMetadata(computeFileHash(tempFile), contentLength, etag, lastModified);
      processedDataFiles.put(url, metadata);

      LOGGER.info("Downloaded data file: " + url + " -> " + tempFile.getName());

    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to process data file: " + url, e);
      result.markFailed(url);
    }
  }

  private File downloadDataFile(String url) throws IOException {
    String fileName = getFileName(url);
    File tempFile = new File(tempDir, fileName);

    try {
      URLConnection connection = new URI(url).toURL().openConnection();
      connection.setConnectTimeout(30000);
      connection.setReadTimeout(30000);

      try (InputStream is = connection.getInputStream();
           FileOutputStream fos = new FileOutputStream(tempFile)) {

        byte[] buffer = new byte[8192];
        int bytesRead;
        long totalBytes = 0;
        String extension = getFileExtension(url);
        long maxSize = config.getSizeLimitForExtension(extension);

        while ((bytesRead = is.read(buffer)) != -1) {
          totalBytes += bytesRead;
          if (totalBytes > maxSize) {
            tempFile.delete();
            throw new IOException("File size exceeds limit: " + maxSize + " bytes");
          }
          fos.write(buffer, 0, bytesRead);
        }
      } finally {
        // Only disconnect if it's an HTTP connection
        if (connection instanceof HttpURLConnection) {
          ((HttpURLConnection) connection).disconnect();
        }
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Invalid URL: " + url, e);
    }

    tempFile.deleteOnExit();
    return tempFile;
  }

  private long getContentLength(String url) throws IOException {
    try {
      URLConnection connection = new URI(url).toURL().openConnection();
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);

      if (connection instanceof HttpURLConnection) {
        HttpURLConnection conn = (HttpURLConnection) connection;
        conn.setRequestMethod("HEAD");
        long length = conn.getContentLengthLong();
        conn.disconnect();
        return length;
      } else {
        // For file:// URLs and other protocols
        long length = connection.getContentLengthLong();
        return length;
      }
    } catch (Exception e) {
      throw new IOException("Invalid URL: " + url, e);
    }
  }

  private String getFileName(String url) {
    String path = url;
    int queryIndex = path.indexOf('?');
    if (queryIndex > 0) {
      path = path.substring(0, queryIndex);
    }

    int lastSlash = path.lastIndexOf('/');
    String name = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;

    if (name.isEmpty()) {
      name = "download";
    }

    // Make unique with timestamp
    int dotIndex = name.lastIndexOf('.');
    if (dotIndex > 0) {
      return name.substring(0, dotIndex) + "_" + System.currentTimeMillis() + name.substring(dotIndex);
    } else {
      return name + "_" + System.currentTimeMillis();
    }
  }

  private String getFileExtension(String url) {
    String path = url;
    int queryIndex = path.indexOf('?');
    if (queryIndex > 0) {
      path = path.substring(0, queryIndex);
    }

    int lastDot = path.lastIndexOf('.');
    if (lastDot > 0 && lastDot < path.length() - 1) {
      return path.substring(lastDot + 1).toLowerCase();
    }
    return "";
  }

  private String computeFileHash(File file) throws IOException {
    // Simple size-based hash for now (could be enhanced with MD5)
    return file.getName() + "_" + file.length();
  }

  /**
   * Cleans up temporary files.
   */
  public void cleanup() {
    if (tempDir != null && tempDir.exists()) {
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
      tempDir.delete();
    }
  }

  /**
   * Gets the link cache for inspection.
   */
  public HtmlLinkCache getLinkCache() {
    return linkCache;
  }
}
