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

import org.apache.calcite.adapter.file.converters.HtmlTableScanner.TableInfo;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Optional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache for extracted links from HTML pages to avoid re-parsing.
 */
public class HtmlLinkCache {
  private static final Logger LOGGER = Logger.getLogger(HtmlLinkCache.class.getName());
  
  /**
   * Cached extraction results from an HTML page.
   */
  public static class ExtractedLinks {
    private final Set<String> dataFileLinks;
    private final Set<String> htmlLinks;
    private final List<TableInfo> tables;
    private final String contentHash;
    private final Instant extractedAt;
    private final @Nullable String etag;
    private final @Nullable String lastModified;
    
    public ExtractedLinks(String url, String htmlContent, CrawlerConfiguration config) throws IOException {
      if (htmlContent == null || htmlContent.isEmpty()) {
        // Handle empty or null content
        this.dataFileLinks = new java.util.HashSet<>();
        this.htmlLinks = new java.util.HashSet<>();
        this.tables = new java.util.ArrayList<>();
        this.contentHash = "";
        this.extractedAt = Instant.now();
        this.etag = null;
        this.lastModified = null;
        return;
      }
      
      Document doc = Jsoup.parse(htmlContent, url);
      this.dataFileLinks = extractDataLinks(doc, url, config);
      this.htmlLinks = extractHtmlLinks(doc, url, config);
      
      // Only extract HTML tables if configured to do so
      if (config.isGenerateTablesFromHtml()) {
        List<TableInfo> allTables = HtmlTableScanner.scanTables(new StringSource(htmlContent, url));
        // Filter tables based on row count constraints
        this.tables = allTables.stream()
            .filter(table -> table.rowCount >= config.getHtmlTableMinRows())
            .filter(table -> table.rowCount <= config.getHtmlTableMaxRows())
            .collect(java.util.stream.Collectors.toList());
      } else {
        this.tables = new java.util.ArrayList<>();
      }
      
      this.contentHash = computeHash(htmlContent);
      this.extractedAt = Instant.now();
      this.etag = null;
      this.lastModified = null;
    }
    
    private ExtractedLinks(Set<String> dataFileLinks, Set<String> htmlLinks, 
                          List<TableInfo> tables, String contentHash,
                          @Nullable String etag, @Nullable String lastModified) {
      this.dataFileLinks = dataFileLinks;
      this.htmlLinks = htmlLinks;
      this.tables = tables;
      this.contentHash = contentHash;
      this.extractedAt = Instant.now();
      this.etag = etag;
      this.lastModified = lastModified;
    }
    
    public ExtractedLinks withHttpMetadata(@Nullable String etag, @Nullable String lastModified) {
      return new ExtractedLinks(dataFileLinks, htmlLinks, tables, contentHash, etag, lastModified);
    }
    
    private static Set<String> extractDataLinks(Document doc, String baseUrl, CrawlerConfiguration config) {
      Set<String> links = new HashSet<>();
      Elements linkElements = doc.select("a[href]");
      
      for (Element link : linkElements) {
        String href = link.attr("abs:href");
        if (href.isEmpty()) {
          href = resolveUrl(baseUrl, link.attr("href"));
        }
        
        if (isDataFile(href, config)) {
          links.add(href);
        }
      }
      
      return links;
    }
    
    private static Set<String> extractHtmlLinks(Document doc, String baseUrl, CrawlerConfiguration config) {
      Set<String> links = new HashSet<>();
      Elements linkElements = doc.select("a[href]");
      
      for (Element link : linkElements) {
        String href = link.attr("abs:href");
        if (href.isEmpty()) {
          href = resolveUrl(baseUrl, link.attr("href"));
        }
        
        if (isHtmlLink(href, baseUrl, config)) {
          links.add(href);
        }
      }
      
      return links;
    }
    
    private static boolean isDataFile(String url, CrawlerConfiguration config) {
      if (url == null || url.isEmpty()) {
        return false;
      }
      
      // Check exclude pattern first
      if (config.getDataFileExcludePattern() != null) {
        if (config.getDataFileExcludePattern().matcher(url).matches()) {
          return false;
        }
      }
      
      // Check include pattern if configured
      if (config.getDataFilePattern() != null) {
        return config.getDataFilePattern().matcher(url).matches();
      }
      
      // Fall back to link pattern if configured
      if (config.getLinkPattern() != null) {
        if (!config.getLinkPattern().matcher(url).matches()) {
          return false;
        }
      }
      
      // Fall back to file extension check if no pattern is configured
      String lowerUrl = url.toLowerCase();
      for (String ext : config.getAllowedFileExtensions()) {
        if (lowerUrl.endsWith("." + ext)) {
          return true;
        }
      }
      
      return false;
    }
    
    private static boolean isHtmlLink(String url, String baseUrl, CrawlerConfiguration config) {
      if (url == null || url.isEmpty()) {
        return false;
      }
      
      // Skip data files
      if (isDataFile(url, config)) {
        return false;
      }
      
      // Skip non-HTTP(S) protocols
      if (!url.startsWith("http://") && !url.startsWith("https://")) {
        return false;
      }
      
      // Skip anchors and javascript
      if (url.contains("#") || url.startsWith("javascript:")) {
        return false;
      }
      
      // Check domain restrictions
      if (!config.isFollowExternalLinks()) {
        try {
          URI base = new URI(baseUrl);
          URI link = new URI(url);
          if (!base.getHost().equalsIgnoreCase(link.getHost())) {
            return false;
          }
        } catch (URISyntaxException e) {
          return false;
        }
      }
      
      // Check allowed domains if configured
      if (!config.getAllowedDomains().isEmpty()) {
        try {
          URI link = new URI(url);
          String host = link.getHost().toLowerCase();
          boolean allowed = false;
          for (String domain : config.getAllowedDomains()) {
            if (host.equals(domain) || host.endsWith("." + domain)) {
              allowed = true;
              break;
            }
          }
          if (!allowed) {
            return false;
          }
        } catch (URISyntaxException e) {
          return false;
        }
      }
      
      return true;
    }
    
    private static String resolveUrl(String base, String relative) {
      try {
        URI baseUri = new URI(base);
        URI resolved = baseUri.resolve(relative);
        return resolved.toString();
      } catch (URISyntaxException e) {
        return relative;
      }
    }
    
    private static String computeHash(String content) {
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(content.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
          sb.append(String.format("%02x", b));
        }
        return sb.toString();
      } catch (NoSuchAlgorithmException e) {
        // Should never happen
        throw new RuntimeException("MD5 algorithm not available", e);
      }
    }
    
    public Set<String> getDataFileLinks() {
      return dataFileLinks;
    }
    
    public Set<String> getHtmlLinks() {
      return htmlLinks;
    }
    
    public List<TableInfo> getTables() {
      return tables;
    }
    
    public String getContentHash() {
      return contentHash;
    }
    
    public boolean isExpired(Duration ttl) {
      return Instant.now().isAfter(extractedAt.plus(ttl));
    }
  }
  
  /**
   * HTTP metadata for change detection.
   */
  private static class HttpMetadata {
    final @Nullable String etag;
    final @Nullable String lastModified;
    final long contentLength;
    
    HttpMetadata(@Nullable String etag, @Nullable String lastModified, long contentLength) {
      this.etag = etag;
      this.lastModified = lastModified;
      this.contentLength = contentLength;
    }
    
    boolean matches(ExtractedLinks cached) {
      if (etag != null && cached.etag != null) {
        return etag.equals(cached.etag);
      }
      if (lastModified != null && cached.lastModified != null) {
        return lastModified.equals(cached.lastModified);
      }
      return false;
    }
  }
  
  private final Map<String, ExtractedLinks> cache = new ConcurrentHashMap<>();
  private final CrawlerConfiguration config;
  
  public HtmlLinkCache(CrawlerConfiguration config) {
    this.config = config;
  }
  
  /**
   * Gets extracted links from a URL, using cache if available and not expired.
   */
  public ExtractedLinks getLinks(String url) throws IOException {
    ExtractedLinks cached = cache.get(url);
    
    // Check if we have cached data
    if (cached != null && !cached.isExpired(config.getHtmlCacheTTL())) {
      // Try to validate with HTTP headers
      try {
        HttpMetadata currentMeta = fetchHttpMetadata(url);
        if (currentMeta.matches(cached)) {
          LOGGER.fine("Using cached links for " + url);
          return cached;
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to fetch HTTP metadata for " + url, e);
      }
    }
    
    // Need to fetch and extract
    LOGGER.info("Fetching and extracting links from " + url);
    String html = downloadHtml(url);
    if (html == null) {
      // Return empty links if content couldn't be downloaded
      return new ExtractedLinks(url, "", config);
    }
    ExtractedLinks links = new ExtractedLinks(url, html, config);
    
    // Try to add HTTP metadata for future validation
    try {
      HttpMetadata meta = fetchHttpMetadata(url);
      links = links.withHttpMetadata(meta.etag, meta.lastModified);
    } catch (IOException e) {
      // Continue without HTTP metadata
    }
    
    cache.put(url, links);
    return links;
  }
  
  /**
   * Clears the cache.
   */
  public void clear() {
    cache.clear();
  }
  
  /**
   * Gets the number of cached entries.
   */
  public int size() {
    return cache.size();
  }
  
  private HttpMetadata fetchHttpMetadata(String url) throws IOException {
    try {
      URLConnection connection = new URI(url).toURL().openConnection();
      
      // Only apply HTTP-specific settings if it's an HTTP(S) connection
      if (connection instanceof HttpURLConnection) {
        HttpURLConnection conn = (HttpURLConnection) connection;
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.connect();
        
        String etag = conn.getHeaderField("ETag");
        String lastModified = conn.getHeaderField("Last-Modified");
        long contentLength = conn.getContentLengthLong();
        
        conn.disconnect();
        
        return new HttpMetadata(etag, lastModified, contentLength);
      } else {
        // For file:// URLs and other protocols, just get basic metadata
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.connect();
        
        long contentLength = connection.getContentLengthLong();
        String lastModified = connection.getHeaderField("Last-Modified");
        
        return new HttpMetadata(null, lastModified, contentLength);
      }
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + url, e);
    }
  }
  
  private String downloadHtml(String url) throws IOException {
    // Check content length first if configured
    if (config.isEnforceContentLengthHeader()) {
      HttpMetadata meta = fetchHttpMetadata(url);
      if (meta.contentLength > config.getMaxHtmlSize()) {
        throw new IOException("HTML file too large: " + meta.contentLength + " bytes exceeds limit of " + config.getMaxHtmlSize());
      }
    }
    
    try {
      URLConnection connection = new URI(url).toURL().openConnection();
      connection.setConnectTimeout(10000);
      connection.setReadTimeout(10000);
      
      try (InputStream is = new SizeLimitedInputStream(connection.getInputStream(), config.getMaxHtmlSize())) {
        byte[] bytes = is.readAllBytes();
        return new String(bytes, StandardCharsets.UTF_8);
      } finally {
        // Only disconnect if it's an HTTP connection
        if (connection instanceof HttpURLConnection) {
          ((HttpURLConnection) connection).disconnect();
        }
      }
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + url, e);
    }
  }
  
  /**
   * Input stream that enforces a size limit.
   */
  private static class SizeLimitedInputStream extends InputStream {
    private final InputStream delegate;
    private final long maxBytes;
    private long bytesRead = 0;
    
    SizeLimitedInputStream(InputStream delegate, long maxBytes) {
      this.delegate = delegate;
      this.maxBytes = maxBytes;
    }
    
    @Override
    public int read() throws IOException {
      if (bytesRead >= maxBytes) {
        throw new IOException("Content length limit exceeded: " + maxBytes);
      }
      int result = delegate.read();
      if (result != -1) {
        bytesRead++;
      }
      return result;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (bytesRead >= maxBytes) {
        throw new IOException("Content length limit exceeded: " + maxBytes);
      }
      int maxRead = (int) Math.min(len, maxBytes - bytesRead);
      int result = delegate.read(b, off, maxRead);
      if (result > 0) {
        bytesRead += result;
      }
      return result;
    }
    
    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
  
  /**
   * Simple string-based Source implementation for testing.
   */
  private static class StringSource implements org.apache.calcite.util.Source {
    private final String content;
    private final String path;
    
    StringSource(String content, String path) {
      this.content = content;
      this.path = path;
    }
    
    @Override
    public URL url() {
      try {
        return new URI(path).toURL();
      } catch (URISyntaxException | MalformedURLException e) {
        return null;
      }
    }
    
    @Override
    public java.io.File file() {
      return null;
    }
    
    @Override
    public String path() {
      return path;
    }
    
    @Override
    public java.io.Reader reader() throws IOException {
      return new java.io.StringReader(content);
    }
    
    @Override
    public InputStream openStream() throws IOException {
      return new java.io.ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
    
    @Override
    public String protocol() {
      try {
        return new URI(path).toURL().getProtocol();
      } catch (URISyntaxException | MalformedURLException e) {
        return "unknown";
      }
    }
    
    @Override
    public org.apache.calcite.util.Source relative(org.apache.calcite.util.Source source) {
      // For our simple use case, just return the source as-is
      return source;
    }
    
    @Override
    public org.apache.calcite.util.Source append(org.apache.calcite.util.Source child) {
      // For our simple use case, just return the child
      return child;
    }
    
    @Override
    public org.apache.calcite.util.Source trim(String suffix) {
      // For our simple use case, return this
      return this;
    }
    
    @Override
    public org.apache.calcite.util.@Nullable Source trimOrNull(String suffix) {
      // For our simple use case, return null
      return null;
    }
    
    @Override
    public java.util.Optional<java.io.File> fileOpt() {
      return java.util.Optional.empty();
    }
  }
}