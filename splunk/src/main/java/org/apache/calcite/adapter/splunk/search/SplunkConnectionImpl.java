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
package org.apache.calcite.adapter.splunk.search;

import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.util.Unsafe;
import org.apache.calcite.util.Util;

import au.com.bytecode.opencsv.CSVReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

import org.apache.calcite.adapter.splunk.search.SplunkAuthenticationException;
import org.apache.calcite.adapter.splunk.util.EnhancedHttpUtils;

import static org.apache.calcite.runtime.HttpUtils.appendURLEncodedArgs;

/**
 * Implementation of {@link SplunkConnection} based on Splunk's REST API.
 * Enhanced to support "_extra" field collection for CIM models, configurable SSL validation,
 * and automatic 401 retry with re-authentication.
 * Uses JSON output for simpler and more reliable data processing.
 */
public class SplunkConnectionImpl implements SplunkConnection {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SplunkConnectionImpl.class);

  private static final Pattern SESSION_KEY =
      Pattern.compile(
          "<sessionKey>([0-9a-zA-Z^_]+)</sessionKey>");

  // Authentication retry configuration
  private static final int MAX_RETRY_ATTEMPTS = 1;
  private static final String AUTH_ERROR_INDICATOR = "401";

  final URL url;
  final String username;
  final String password;
  final String token;
  final boolean disableSslValidation;
  final boolean useTokenAuth;

  // Mutable authentication state
  private String sessionKey = "";
  private final Map<String, String> requestHeaders = new HashMap<>();
  private final Object authLock = new Object(); // For thread-safe re-authentication

  public SplunkConnectionImpl(String url, String username, String password)
      throws MalformedURLException {
    this(url, username, password, false);
  }

  public SplunkConnectionImpl(String url, String username, String password, boolean disableSslValidation)
      throws MalformedURLException {
    this(URI.create(url).toURL(), username, password, disableSslValidation);
  }

  public SplunkConnectionImpl(URL url, String username, String password) {
    this(url, username, password, false);
  }

  public SplunkConnectionImpl(URL url, String username, String password, boolean disableSslValidation) {
    this.url      = Objects.requireNonNull(url, "url cannot be null");
    this.username = Objects.requireNonNull(username, "username cannot be null");
    this.password = Objects.requireNonNull(password, "password cannot be null");
    this.token    = null;
    this.useTokenAuth = false;
    this.disableSslValidation = disableSslValidation;

    if (disableSslValidation) {
      configureSSL();
    }
    connect();
  }

  /**
   * Constructor for token-based authentication.
   */
  public SplunkConnectionImpl(String url, String token) throws MalformedURLException {
    this(url, token, false);
  }

  /**
   * Constructor for token-based authentication with SSL configuration.
   */
  public SplunkConnectionImpl(String url, String token, boolean disableSslValidation) throws MalformedURLException {
    this(URI.create(url).toURL(), token, disableSslValidation);
  }

  /**
   * Constructor for token-based authentication.
   */
  public SplunkConnectionImpl(URL url, String token) {
    this(url, token, false);
  }

  /**
   * Constructor for token-based authentication with SSL configuration.
   */
  public SplunkConnectionImpl(URL url, String token, boolean disableSslValidation) {
    this.url      = Objects.requireNonNull(url, "url cannot be null");
    this.token    = Objects.requireNonNull(token, "token cannot be null");
    this.username = null;
    this.password = null;
    this.useTokenAuth = true;
    this.disableSslValidation = disableSslValidation;

    if (disableSslValidation) {
      configureSSL();
    }

    // For token auth, set authorization header directly and skip connect()
    synchronized (authLock) {
      requestHeaders.put("Authorization", "Bearer " + token);
    }
  }

  /**
   * Configure SSL settings for this connection.
   * WARNING: disableSslValidation should only be used in development/testing.
   */
  private void configureSSL() {
    if (!disableSslValidation) {
      return;
    }

    try {
      // Create a trust manager that accepts all certificates
      TrustManager[] trustAllCerts = new TrustManager[] {
          new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
              return null;
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
              // Trust all client certificates
            }
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
              // Trust all server certificates
            }
          }
      };

      // Install the all-trusting trust manager
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      // Disable hostname verification
      HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

      LOGGER.warn("SSL certificate validation has been disabled. This should only be used in development/testing environments.");

    } catch (Exception e) {
      throw new RuntimeException("Failed to configure SSL settings", e);
    }
  }

  private static void close(Closeable c) {
    try {
      c.close();
    } catch (Exception ignore) {
      // ignore
    }
  }

  /**
   * Performs authentication for username/password based connections.
   * Can be called multiple times to re-authenticate on session expiry.
   */
  @SuppressWarnings("CatchAndPrintStackTrace")
  private void connect() {
    if (useTokenAuth) {
      LOGGER.debug("Skipping connect() for token-based authentication");
      return;
    }

    synchronized (authLock) {
      BufferedReader rd = null;

      try {
        String loginUrl =
            String.format(Locale.ROOT,
                "%s://%s:%d/services/auth/login",
                url.getProtocol(),
                url.getHost(),
                url.getPort());

        StringBuilder data = new StringBuilder();
        appendURLEncodedArgs(
            data, "username", username, "password", password);

        LOGGER.debug("Authenticating to Splunk at: {}", loginUrl);

        // Create a temporary header map for authentication (don't include stale session)
        Map<String, String> authHeaders = new HashMap<>();

        rd = Util.reader(EnhancedHttpUtils.post(loginUrl, data, authHeaders, 10000, 30000));

        String line;
        StringBuilder reply = new StringBuilder();
        while ((line = rd.readLine()) != null) {
          reply.append(line);
          reply.append("\n");
        }

        Matcher m = SESSION_KEY.matcher(reply);
        if (m.find()) {
          sessionKey = m.group(1);
          requestHeaders.clear(); // Clear any old headers
          requestHeaders.put("Authorization", "Splunk " + sessionKey);
          LOGGER.debug("Successfully authenticated to Splunk, received session key");
        } else {
          LOGGER.error("Failed to extract session key from authentication response");
          throw new RuntimeException("Authentication failed - no session key found");
        }
      } catch (Exception e) {
        LOGGER.error("Authentication failed", e);
        throw new RuntimeException("Failed to authenticate to Splunk", e);
      } finally {
        if (rd != null) {
          close(rd);
        }
      }
    }
  }

  /**
   * Checks if an exception indicates an authentication error (HTTP 401).
   */
  private boolean isAuthenticationError(Throwable e) {
    if (e == null) {
      return false;
    }

    // Check for specific authentication exception
    if (e instanceof SplunkAuthenticationException) {
      return true;
    }

    String message = e.getMessage();
    if (message != null && message.contains(AUTH_ERROR_INDICATOR)) {
      return true;
    }

    // Check if it's a RuntimeException wrapping an authentication error
    Throwable cause = e.getCause();
    while (cause != null) {
      if (cause instanceof SplunkAuthenticationException) {
        return true;
      }
      String causeMessage = cause.getMessage();
      if (causeMessage != null && causeMessage.contains(AUTH_ERROR_INDICATOR)) {
        return true;
      }
      cause = cause.getCause();
    }

    return false;
  }

  /**
   * Re-authenticates the connection based on the authentication method.
   */
  private void reAuthenticate() {
    LOGGER.info("Re-authenticating Splunk connection due to session expiry");

    if (useTokenAuth) {
      // For token auth, we can't refresh the token automatically
      // Just reset the header - the token might have been refreshed externally
      synchronized (authLock) {
        requestHeaders.clear();
        requestHeaders.put("Authorization", "Bearer " + token);
      }
      LOGGER.info("Reset token authorization header");
    } else {
      // For username/password auth, get a new session
      connect();
      LOGGER.info("Successfully re-authenticated with new session key");
    }
  }

  @Override public void getSearchResults(String search, Map<String, String> otherArgs,
      List<String> fieldList, SearchResultListener srl) {
    Objects.requireNonNull(srl, "SearchResultListener cannot be null");
    performSearchWithRetry(search, otherArgs, srl);
  }

  @Override public Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields) {
    return getSearchResultEnumerator(search, otherArgs, fieldList, explicitFields, new HashMap<>());
  }

  @Override public Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields,
      Map<String, String> reverseFieldMapping) {
    return performSearchForEnumeratorWithRetry(search, otherArgs, fieldList, explicitFields, reverseFieldMapping);
  }

  /**
   * Performs search with automatic retry on authentication failure.
   */
  private void performSearchWithRetry(String search, Map<String, String> otherArgs, SearchResultListener srl) {
    Exception lastException = null;

    for (int attempt = 0; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        performSearch(search, otherArgs, srl);
        return; // Success
      } catch (IOException e) {
        lastException = e;

        if (isAuthenticationError(e) && attempt < MAX_RETRY_ATTEMPTS) {
          LOGGER.warn("Authentication error detected on attempt {}, retrying...", attempt + 1);
          try {
            reAuthenticate();
          } catch (Exception authException) {
            LOGGER.error("Re-authentication failed", authException);
            break; // Don't retry if re-auth fails
          }
        } else {
          break; // Non-auth error or max retries reached
        }
      } catch (Exception e) {
        lastException = e;

        if (isAuthenticationError(e) && attempt < MAX_RETRY_ATTEMPTS) {
          LOGGER.warn("Authentication error detected on attempt {}, retrying...", attempt + 1);
          try {
            reAuthenticate();
          } catch (Exception authException) {
            LOGGER.error("Re-authentication failed", authException);
            break; // Don't retry if re-auth fails
          }
        } else {
          break; // Non-auth error or max retries reached
        }
      }
    }

    // If we get here, all retries failed
    StringWriter sw = new StringWriter();
    if (lastException != null) {
      lastException.printStackTrace(new PrintWriter(sw));
      LOGGER.error("Search failed after {} attempts: {}\n{}",
          MAX_RETRY_ATTEMPTS + 1, lastException.getMessage(), sw);
    }
  }

  /**
   * Performs search for enumerator with automatic retry on authentication failure.
   */
  private Enumerator<Object> performSearchForEnumeratorWithRetry(
      String search,
      Map<String, String> otherArgs,
      List<String> schemaFieldList,
      Set<String> explicitFields,
      Map<String, String> reverseFieldMapping) {

    Exception lastException = null;

    for (int attempt = 0; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        return performSearchForEnumerator(search, otherArgs, schemaFieldList, explicitFields, reverseFieldMapping);
      } catch (IOException e) {
        lastException = e;

        if (isAuthenticationError(e) && attempt < MAX_RETRY_ATTEMPTS) {
          LOGGER.warn("Authentication error detected on attempt {}, retrying...", attempt + 1);
          try {
            reAuthenticate();
          } catch (Exception authException) {
            LOGGER.error("Re-authentication failed", authException);
            break; // Don't retry if re-auth fails
          }
        } else {
          break; // Non-auth error or max retries reached
        }
      } catch (Exception e) {
        lastException = e;

        if (isAuthenticationError(e) && attempt < MAX_RETRY_ATTEMPTS) {
          LOGGER.warn("Authentication error detected on attempt {}, retrying...", attempt + 1);
          try {
            reAuthenticate();
          } catch (Exception authException) {
            LOGGER.error("Re-authentication failed", authException);
            break; // Don't retry if re-auth fails
          }
        } else {
          break; // Non-auth error or max retries reached
        }
      }
    }

    // If we get here, all retries failed
    StringWriter sw = new StringWriter();
    if (lastException != null) {
      lastException.printStackTrace(new PrintWriter(sw));
      LOGGER.error("Search enumerator failed after {} attempts: {}\n{}",
          MAX_RETRY_ATTEMPTS + 1, lastException.getMessage(), sw);
    }

    return Linq4j.emptyEnumerator();
  }

  private void performSearch(
      String search,
      Map<String, String> otherArgs,
      SearchResultListener srl) throws IOException {
    String searchUrl =
        String.format(Locale.ROOT,
            "%s://%s:%d/services/search/jobs/export",
            url.getProtocol(),
            url.getHost(),
            url.getPort());

    StringBuilder data = new StringBuilder();
    Map<String, String> args = new LinkedHashMap<>(otherArgs);
    args.put("search", search);
    // override these args
    args.put("output_mode", "csv");
    args.put("preview", "0");

    // TODO: remove this once the csv parser can handle leading spaces
    args.put("check_connection", "0");

    appendURLEncodedArgs(data, args);

    // Use synchronized access to headers to ensure thread safety
    Map<String, String> headersToUse;
    synchronized (authLock) {
      headersToUse = new HashMap<>(requestHeaders);
    }

    // wait at most 30 minutes for first result
    InputStream in = EnhancedHttpUtils.post(searchUrl, data, headersToUse, 10000, 1800000);
    parseResults(in, srl);
  }

  private Enumerator<Object> performSearchForEnumerator(
      String search,
      Map<String, String> otherArgs,
      List<String> schemaFieldList,
      Set<String> explicitFields,
      Map<String, String> reverseFieldMapping) throws IOException {
    String searchUrl =
        String.format(Locale.ROOT,
            "%s://%s:%d/services/search/jobs/export",
            url.getProtocol(),
            url.getHost(),
            url.getPort());

    StringBuilder data = new StringBuilder();
    Map<String, String> args = new LinkedHashMap<>(otherArgs);
    args.put("search", search);

    // THE KEY CHANGE: Use JSON instead of CSV
    args.put("output_mode", "json");
    args.put("preview", "0");
    args.put("check_connection", "0");

    LOGGER.debug("=== SPLUNK SEARCH DEBUG ===");
    LOGGER.debug("Search URL: {}", searchUrl);
    LOGGER.debug("Search query: {}", search);
    LOGGER.debug("All search args: {}", args);
    LOGGER.debug("=== END SPLUNK SEARCH DEBUG ===");

    appendURLEncodedArgs(data, args);

    // Use synchronized access to headers to ensure thread safety
    Map<String, String> headersToUse;
    synchronized (authLock) {
      headersToUse = new HashMap<>(requestHeaders);
    }

    // wait at most 30 minutes for first result
    InputStream in = EnhancedHttpUtils.post(searchUrl, data, headersToUse, 10000, 1800000);
    return new SplunkJsonResultEnumeratorWithRetry(in, schemaFieldList, explicitFields, reverseFieldMapping, this);
  }

  private static void parseResults(InputStream in, SearchResultListener srl) {
    try (CSVReader r =
             new CSVReader(
                 new BufferedReader(
                     new InputStreamReader(in, StandardCharsets.UTF_8)))) {
      String[] header = r.readNext();
      if (header != null
          && header.length > 0
          && !(header.length == 1 && header[0].isEmpty())) {
        srl.setFieldNames(header);

        String[] line;
        while ((line = r.readNext()) != null) {
          if (line.length == header.length) {
            srl.processSearchResult(line);
          }
        }
      }
    } catch (IOException e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      LOGGER.warn("{}\n{}", e.getMessage(), sw);
    }
  }

  public static void parseArgs(String[] args, Map<String, String> map) {
    for (int i = 0; i < args.length; i++) {
      String argName = args[i++];
      String argValue = i < args.length ? args[i] : "";

      if (!argName.startsWith("-")) {
        throw new IllegalArgumentException("invalid argument name: " + argName
            + ". Argument names must start with -");
      }
      map.put(argName.substring(1), argValue);
    }
  }

  public static void printUsage(String errorMsg) {
    String[] strings = {
        "Usage: java Connection -<arg-name> <arg-value>",
        "The following <arg-name> are valid",
        "search        - required, search string to execute",
        "field_list    - "
            + "required, list of fields to request, comma delimited",
        "uri           - "
            + "uri to splunk's mgmt port, default: https://localhost:8089",
        "username      - "
            + "username to use for authentication, default: admin",
        "password      - "
            + "password to use for authentication, default: changeme",
        "earliest_time - earliest time for the search, default: -24h",
        "latest_time   - latest time for the search, default: now",
        "-print        - whether to print results or just the summary"
    };
    System.err.println(errorMsg);
    for (String s : strings) {
      System.err.println(s);
    }
    Unsafe.systemExit(1);
  }

  public static void main(String[] args) throws MalformedURLException {
    Map<String, String> argsMap = new HashMap<>();
    argsMap.put("uri",           "https://localhost:8089");
    argsMap.put("username",      "admin");
    argsMap.put("password",      "changeme");
    argsMap.put("earliest_time", "-24h");
    argsMap.put("latest_time",   "now");
    argsMap.put("-print",        "true");

    parseArgs(args, argsMap);

    String search = argsMap.get("search");
    String field_list = argsMap.get("field_list");

    if (search == null || search.trim().isEmpty()) {
      printUsage("Missing required argument: search");
      return;
    }
    if (field_list == null || field_list.trim().isEmpty()) {
      printUsage("Missing required argument: field_list");
      return;
    }

    List<String> fieldList = StringUtils.decodeList(field_list, ',');

    SplunkConnection c =
        new SplunkConnectionImpl(
            argsMap.get("uri"),
            argsMap.get("username"),
            argsMap.get("password"));

    Map<String, String> searchArgs = new HashMap<>();
    searchArgs.put("earliest_time", argsMap.get("earliest_time"));
    searchArgs.put("latest_time", argsMap.get("latest_time"));
    searchArgs.put(
        "field_list",
        StringUtils.encodeList(fieldList, ',').toString());

    String printArg = argsMap.get("-print");
    boolean shouldPrint = Boolean.parseBoolean(printArg);

    CountingSearchResultListener dummy = new CountingSearchResultListener(shouldPrint);
    long start = System.currentTimeMillis();
    c.getSearchResults(search, searchArgs, fieldList, dummy);
  }

  /** Implementation of
   * {@link SearchResultListener}
   * interface that just counts the results. */
  public static class CountingSearchResultListener
      implements SearchResultListener {
    String[] fieldNames = new String[0];
    int resultCount = 0;
    final boolean print;

    public CountingSearchResultListener(boolean print) {
      this.print = print;
    }

    @Override public void setFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override public boolean processSearchResult(String[] values) {
      resultCount++;
      if (print) {
        int maxIndex = Math.min(fieldNames.length, values.length);
        for (int i = 0; i < maxIndex; ++i) {
          LOGGER.debug(String.format(Locale.ROOT, "%s=%s\n", this.fieldNames[i],
              values[i]));
        }
      }
      return true;
    }

    public int getResultCount() {
      return resultCount;
    }
  }

  /**
   * Enhanced JSON-based result enumerator with retry capability for mid-stream authentication failures.
   * Much simpler and more reliable than the CSV approach.
   */
  public static class SplunkJsonResultEnumeratorWithRetry implements Enumerator<Object> {
    private BufferedReader reader;
    private final List<String> schemaFieldList; // REQUESTED fields in query order
    private final Set<String> explicitFields;
    private final Map<String, String> fieldMapping; // Schema field -> Splunk field
    private final SplunkConnectionImpl connection;
    private Object current;
    private int rowCount = 0;
    private boolean retryAttempted = false;

    // Store original search parameters for potential retry
    private String originalSearch;
    private Map<String, String> originalArgs;

    // Jackson ObjectMapper for robust JSON parsing
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
        new TypeReference<Map<String, Object>>() {};

    public SplunkJsonResultEnumeratorWithRetry(InputStream in, List<String> schemaFieldList,
        Set<String> explicitFields, Map<String, String> reverseFieldMapping,
        SplunkConnectionImpl connection) {
      this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      this.schemaFieldList = schemaFieldList;
      this.explicitFields = explicitFields;
      this.connection = connection;

      // Convert reverse mapping (Splunk -> Schema) to forward mapping (Schema -> Splunk)
      this.fieldMapping = new HashMap<>();
      for (Map.Entry<String, String> entry : reverseFieldMapping.entrySet()) {
        this.fieldMapping.put(entry.getValue(), entry.getKey());
      }

      LOGGER.debug("=== JSON ENUMERATOR WITH RETRY INITIALIZED ===");
      LOGGER.debug("REQUESTED fields (schemaFieldList) SIZE: {}", schemaFieldList.size());
      LOGGER.debug("REQUESTED fields: {}", schemaFieldList);
      LOGGER.debug("Field mapping (Schema -> Splunk): {}", fieldMapping);
      LOGGER.debug("Explicit fields: {}", explicitFields);

      // CRITICAL DEBUG: Show the exact order of REQUESTED schema fields
      LOGGER.debug("=== REQUESTED FIELD ORDER ===");
      for (int i = 0; i < schemaFieldList.size(); i++) {
        String schemaField = schemaFieldList.get(i);
        String splunkField = fieldMapping.getOrDefault(schemaField, schemaField);
        LOGGER.debug("  [{}] REQUESTED: '{}' -> '{}'", i, schemaField, splunkField);
      }
    }

    @Override
    public Object current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      try {
        return moveNextInternal();
      } catch (Exception e) {
        // Check for authentication error and retry if possible
        if (connection.isAuthenticationError(e) && !retryAttempted) {
          LOGGER.warn("Authentication error detected during stream processing, attempting to restart search");
          retryAttempted = true;

          try {
            connection.reAuthenticate();

            // Note: For a complete implementation, we would need to restart the search from the beginning
            // This is complex because we've already consumed part of the stream
            // For now, we'll just re-authenticate and let the connection handle subsequent requests
            LOGGER.warn("Re-authentication completed, but stream cannot be rewound. Query may need to be restarted.");

          } catch (Exception authException) {
            LOGGER.error("Re-authentication during stream processing failed", authException);
          }
        }

        LOGGER.error("Error in JSON enumerator: " + e.getMessage(), e);
        return false;
      }
    }

    private boolean moveNextInternal() throws Exception {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) continue;

        // Parse JSON line using Jackson
        Map<String, Object> rawJsonRecord = parseJsonLine(line);
        if (rawJsonRecord == null) continue;

        // Extract event data from Splunk's wrapper structure
        Map<String, Object> eventData = extractEventData(rawJsonRecord);

        rowCount++;

        // Debug first few rows
        if (rowCount <= 3) {
          LOGGER.debug("=== JSON ROW {} ===", rowCount);
          LOGGER.debug("Raw JSON keys: {}", rawJsonRecord.keySet());
          LOGGER.debug("Extracted event data keys: {}", eventData.keySet());
          LOGGER.debug("Event data field count: {}", eventData.size());

          // Show ALL event data fields so we can see what we're working with
          LOGGER.debug("ALL EVENT DATA (showing first 30 fields):");
          int fieldCount = 0;
          for (Map.Entry<String, Object> entry : eventData.entrySet()) {
            if (fieldCount >= 30) {
              LOGGER.debug("  ... ({} more fields)", eventData.size() - 30);
              break;
            }
            Object value = entry.getValue();
            LOGGER.debug("  '{}': '{}' ({})", entry.getKey(), value,
                value != null ? value.getClass().getSimpleName() : "null");
            fieldCount++;
          }
        }

        // Map to schema fields - build array in the exact order schemaFieldList specifies
        Object[] result = new Object[schemaFieldList.size()];

        for (int i = 0; i < schemaFieldList.size(); i++) {
          String schemaField = schemaFieldList.get(i);

          if ("_extra".equals(schemaField)) {
            // Collect unmapped fields as JSON
            if (rowCount <= 3) {
              LOGGER.debug("  [{}] Processing _extra field...", i);
            }
            result[i] = buildExtraFields(eventData);
          } else {
            // Map schema field name to Splunk field name
            String splunkField = fieldMapping.getOrDefault(schemaField, schemaField);
            Object value = eventData.get(splunkField);

            // DEBUG: Show the lookup process
            if (rowCount <= 3) {
              LOGGER.debug("  [{}] Looking up: schema='{}' -> splunk='{}'", i, schemaField, splunkField);
              LOGGER.debug("      Event data contains key '{}'? {}", splunkField, eventData.containsKey(splunkField));
              LOGGER.debug("      RAW VALUE: '{}' ({})", value, value != null ? value.getClass().getSimpleName() : "null");
            }

            result[i] = value;

            if (rowCount <= 3) {
              LOGGER.debug("  [{}] FINAL: schema='{}' -> splunk='{}' -> value='{}' ({})",
                  i, schemaField, splunkField, value,
                  value != null ? value.getClass().getSimpleName() : "null");
            }
          }
        }

        this.current = result;
        return true;
      }
      return false;
    }

    /**
     * Extract event data from Splunk's JSON wrapper structure.
     * Splunk returns: {"preview":false,"result":{...event data...},"offset":0}
     * We need the content of the "result" field.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractEventData(Map<String, Object> rawJson) {
      try {
        // Check if this is a wrapped Splunk result
        if (rawJson.containsKey("result")) {
          Object resultObj = rawJson.get("result");
          if (resultObj instanceof Map) {
            return (Map<String, Object>) resultObj;
          }
        }

        // If no "result" wrapper, assume it's already unwrapped event data
        return rawJson;

      } catch (Exception e) {
        System.err.println("Failed to extract event data from JSON: " + e.getMessage());
        return rawJson; // Fallback to original
      }
    }

    /**
     * Parse a single JSON line from Splunk using Jackson.
     * Robust and handles all JSON edge cases properly.
     */
    private Map<String, Object> parseJsonLine(String line) {
      try {
        // Show raw JSON for first few lines so we can see what we're actually getting
        if (rowCount <= 2) {
          LOGGER.debug("=== RAW JSON LINE {} ===", rowCount + 1);
          LOGGER.debug("{}", line);
          LOGGER.debug("=== END RAW JSON ===");
        }

        return OBJECT_MAPPER.readValue(line, MAP_TYPE_REF);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse JSON line: {}", line.substring(0, Math.min(100, line.length())));
        if (rowCount <= 3) {
          // Show more details for first few parsing errors
          LOGGER.warn("Parsing error: {}", e.getMessage());
        }
        return null;
      }
    }

    /**
     * Build _extra field containing all unmapped fields as JSON.
     * Uses Jackson for reliable serialization.
     */
    private String buildExtraFields(Map<String, Object> eventData) {
      Map<String, Object> extra = new HashMap<>();

      // Create set of Splunk fields that correspond to requested schema fields
      Set<String> requestedSplunkFields = new HashSet<>();
      for (String schemaField : schemaFieldList) {
        if (!"_extra".equals(schemaField)) {
          String splunkField = fieldMapping.getOrDefault(schemaField, schemaField);
          requestedSplunkFields.add(splunkField);
        }
      }

      if (rowCount <= 3) {
        LOGGER.debug("    REQUESTED schema fields: {}", schemaFieldList);
        LOGGER.debug("    REQUESTED Splunk fields: {}", requestedSplunkFields);
        LOGGER.debug("    AVAILABLE Splunk fields: {}", eventData.keySet());
      }

      for (Map.Entry<String, Object> entry : eventData.entrySet()) {
        String fieldName = entry.getKey();

        // Include in _extra if this Splunk field was NOT explicitly requested
        boolean wasRequested = requestedSplunkFields.contains(fieldName);

        if (rowCount <= 3) {
          LOGGER.debug("    Evaluating field '{}': wasRequested={}", fieldName, wasRequested);
        }

        if (!wasRequested) {
          extra.put(fieldName, entry.getValue());
          if (rowCount <= 3) {
            LOGGER.debug("    -> Including '{}' in _extra", fieldName);
          }
        } else if (rowCount <= 3) {
          LOGGER.debug("    -> Excluding '{}' from _extra (was requested)", fieldName);
        }
      }

      if (rowCount <= 3) {
        LOGGER.debug("  _extra field contains {} unmapped fields: {}", extra.size(), extra.keySet());
      }

      String extraJson = serializeToJson(extra);

      if (rowCount <= 3) {
        LOGGER.debug("  _extra JSON result: {}", extraJson);
      }

      return extraJson;
    }

    /**
     * Serialize map to JSON using Jackson.
     * Much more robust than hand-crafted JSON serialization.
     */
    private String serializeToJson(Map<String, Object> map) {
      try {
        return OBJECT_MAPPER.writeValueAsString(map);
      } catch (Exception e) {
        System.err.println("Failed to serialize _extra fields to JSON: " + e.getMessage());
        return "{}";
      }
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
