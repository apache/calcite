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

import org.apache.calcite.adapter.file.storage.HttpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFile;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * SEC-specific HTTP storage provider that extends the file adapter's HttpStorageProvider.
 * Adds SEC EDGAR compliance including proper User-Agent headers and rate limiting.
 */
public class SecHttpStorageProvider extends HttpStorageProvider {
  private static final Logger LOGGER = Logger.getLogger(SecHttpStorageProvider.class.getName());
  private static final String USER_AGENT = "Apache Calcite SEC Adapter (apache-calcite@apache.org)";
  private static final long RATE_LIMIT_MS = 100; // 10 requests per second max

  private long lastRequestTime = 0;
  private final Object rateLimitLock = new Object();

  /**
   * Creates a new SEC HTTP storage provider with default GET method.
   */
  public SecHttpStorageProvider() {
    this("GET", null, createSecHeaders(), null);
  }

  /**
   * Creates a new SEC HTTP storage provider with specified configuration.
   *
   * @param method HTTP method (GET or POST)
   * @param requestBody Request body for POST requests
   * @param headers Additional HTTP headers
   * @param mimeTypeOverride MIME type override
   */
  public SecHttpStorageProvider(String method, @Nullable String requestBody,
      Map<String, String> headers, @Nullable String mimeTypeOverride) {
    super(method, requestBody, mergeHeaders(headers), mimeTypeOverride);
  }

  /**
   * Creates default SEC headers.
   */
  private static Map<String, String> createSecHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("User-Agent", USER_AGENT);
    headers.put("Accept", "application/json, application/xml, text/html");
    return headers;
  }

  /**
   * Merges user headers with SEC-required headers.
   */
  private static Map<String, String> mergeHeaders(Map<String, String> userHeaders) {
    Map<String, String> headers = createSecHeaders();
    if (userHeaders != null) {
      headers.putAll(userHeaders);
    }
    // Ensure User-Agent is always the SEC-compliant one
    headers.put("User-Agent", USER_AGENT);
    return headers;
  }

  /**
   * Apply rate limiting before making requests.
   */
  private void applyRateLimit() throws IOException {
    synchronized (rateLimitLock) {
      long now = System.currentTimeMillis();
      long timeSinceLastRequest = now - lastRequestTime;

      if (timeSinceLastRequest < RATE_LIMIT_MS) {
        try {
          long waitTime = RATE_LIMIT_MS - timeSinceLastRequest;
          LOGGER.fine("Rate limiting: waiting " + waitTime + "ms");
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Rate limiting interrupted", e);
        }
      }

      lastRequestTime = System.currentTimeMillis();
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    int maxRetries = 3;
    IOException lastException = null;

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          LOGGER.info("Retry attempt " + attempt + " for " + path);
          // Exponential backoff
          Thread.sleep(TimeUnit.SECONDS.toMillis((long) Math.pow(2, attempt)));
        }

        // Apply rate limiting before each request
        applyRateLimit();

        return super.openInputStream(path);

      } catch (IOException e) {
        lastException = e;

        // Check if it's a rate limit error (HTTP 429)
        if (e.getMessage() != null && e.getMessage().contains("429")) {
          LOGGER.warning("SEC rate limit hit, waiting 60 seconds...");
          try {
            Thread.sleep(60000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Rate limit wait interrupted", ie);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Retry interrupted", e);
      }
    }

    throw new IOException("Failed to fetch from SEC after " + maxRetries + " attempts",
        lastException);
  }

  /**
   * Creates a storage provider for SEC EDGAR API access.
   */
  public static SecHttpStorageProvider forEdgar() {
    return new SecHttpStorageProvider();
  }

  /**
   * Creates a storage provider for SEC company tickers API.
   */
  public static SecHttpStorageProvider forCompanyTickers() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    return new SecHttpStorageProvider("GET", null, headers, null);
  }
}
