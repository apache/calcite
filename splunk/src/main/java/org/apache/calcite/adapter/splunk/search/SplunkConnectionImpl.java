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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
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

import static org.apache.calcite.runtime.HttpUtils.appendURLEncodedArgs;
import static org.apache.calcite.runtime.HttpUtils.post;

/**
 * Implementation of {@link SplunkConnection} based on Splunk's REST API.
 * Enhanced to support "_extra" field collection for CIM models and configurable SSL validation.
 * Uses JSON output for simpler and more reliable data processing.
 */
public class SplunkConnectionImpl implements SplunkConnection {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SplunkConnectionImpl.class);

  private static final Pattern SESSION_KEY =
      Pattern.compile(
          "<sessionKey>([0-9a-zA-Z^_]+)</sessionKey>");

  final URL url;
  final String username;
  final String password;
  final String token;
  final boolean disableSslValidation;
  String sessionKey = "";
  final Map<String, String> requestHeaders = new HashMap<>();

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
    this.disableSslValidation = disableSslValidation;

    if (disableSslValidation) {
      configureSSL();
    }

    // For token auth, set authorization header directly and skip connect()
    requestHeaders.put("Authorization", "Bearer " + token);
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

  @SuppressWarnings("CatchAndPrintStackTrace")
  private void connect() {
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

      rd = Util.reader(post(loginUrl, data, requestHeaders));

      String line;
      StringBuilder reply = new StringBuilder();
      while ((line = rd.readLine()) != null) {
        reply.append(line);
        reply.append("\n");
      }

      Matcher m = SESSION_KEY.matcher(reply);
      if (m.find()) {
        sessionKey = m.group(1);
        requestHeaders.put("Authorization", "Splunk " + sessionKey);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (rd != null) {
        close(rd);
      }
    }
  }

  @Override public void getSearchResults(String search, Map<String, String> otherArgs,
      List<String> fieldList, SearchResultListener srl) {
    Objects.requireNonNull(srl, "SearchResultListener cannot be null");
    performSearch(search, otherArgs, srl);
  }

  @Override public Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields) {
    return getSearchResultEnumerator(search, otherArgs, fieldList, explicitFields, new HashMap<>());
  }

  @Override public Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList, Set<String> explicitFields,
      Map<String, String> reverseFieldMapping) {
    return performSearchForEnumerator(search, otherArgs, fieldList, explicitFields, reverseFieldMapping);
  }

  private void performSearch(
      String search,
      Map<String, String> otherArgs,
      SearchResultListener srl) {
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
    try {
      // wait at most 30 minutes for first result
      InputStream in =
          post(searchUrl, data, requestHeaders, 10000, 1800000);
      parseResults(in, srl);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      LOGGER.warn("{}\n{}", e.getMessage(), sw);
    }
  }

  private Enumerator<Object> performSearchForEnumerator(
      String search,
      Map<String, String> otherArgs,
      List<String> schemaFieldList,
      Set<String> explicitFields,
      Map<String, String> reverseFieldMapping) {
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

    appendURLEncodedArgs(data, args);
    try {
      // wait at most 30 minutes for first result
      InputStream in =
          post(searchUrl, data, requestHeaders, 10000, 1800000);
      return new SplunkJsonResultEnumerator(in, schemaFieldList, explicitFields, reverseFieldMapping);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      LOGGER.warn("{}\n{}", e.getMessage(), sw);
      return Linq4j.emptyEnumerator();
    }
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
          System.out.printf(Locale.ROOT, "%s=%s\n", this.fieldNames[i],
              values[i]);
        }
        System.out.println();
      }
      return true;
    }

    public int getResultCount() {
      return resultCount;
    }
  }

  /**
   * Production JSON-based result enumerator using Jackson for robust JSON parsing.
   * Much simpler and more reliable than the CSV approach.
   */
  public static class SplunkJsonResultEnumerator implements Enumerator<Object> {
    private final BufferedReader reader;
    private final List<String> schemaFieldList;
    private final Set<String> explicitFields;
    private final Map<String, String> fieldMapping; // Schema field -> Splunk field
    private Object current;
    private int rowCount = 0;

    // Jackson ObjectMapper for robust JSON parsing
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
        new TypeReference<Map<String, Object>>() {};

    public SplunkJsonResultEnumerator(InputStream in, List<String> schemaFieldList,
        Set<String> explicitFields, Map<String, String> reverseFieldMapping) {
      this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      this.schemaFieldList = schemaFieldList;
      this.explicitFields = explicitFields;

      // Convert reverse mapping (Splunk -> Schema) to forward mapping (Schema -> Splunk)
      this.fieldMapping = new HashMap<>();
      for (Map.Entry<String, String> entry : reverseFieldMapping.entrySet()) {
        this.fieldMapping.put(entry.getValue(), entry.getKey());
      }

      System.out.println("=== JSON ENUMERATOR INITIALIZED ===");
      System.out.println("Schema field list: " + schemaFieldList);
      System.out.println("Reverse field mapping received: " + reverseFieldMapping);
      System.out.println("Field mapping (Schema -> Splunk): " + fieldMapping);
      System.out.println("Explicit fields: " + explicitFields);

      // DEBUG: Show what each schema field maps to
      for (String schemaField : schemaFieldList) {
        String splunkField = fieldMapping.getOrDefault(schemaField, schemaField);
        System.out.println("  '" + schemaField + "' -> '" + splunkField + "'");
      }

      System.out.println("DEBUG: Explicit fields that will be excluded from _extra: " + explicitFields);
      System.out.println("DEBUG: Mapped fields that will be excluded from _extra: " + fieldMapping.values());
    }

    @Override
    public Object current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) continue;

          // Parse JSON line using Jackson
          Map<String, Object> rawJsonRecord = parseJsonLine(line);
          if (rawJsonRecord == null) continue;

          // CRITICAL FIX: Extract event data from Splunk's wrapper structure
          Map<String, Object> jsonRecord = extractEventData(rawJsonRecord);

          rowCount++;

          // Debug first few rows
          if (rowCount <= 3) {
            System.out.println("=== JSON ROW " + rowCount + " ===");
            System.out.println("Raw JSON keys: " + rawJsonRecord.keySet());
            System.out.println("Extracted event data keys: " + jsonRecord.keySet());

            // Show ALL event data fields so we can see what we're working with
            System.out.println("ALL EVENT DATA:");
            for (Map.Entry<String, Object> entry : jsonRecord.entrySet()) {
              Object value = entry.getValue();
              System.out.printf("  '%s': '%s' (%s)\n", entry.getKey(), value,
                  value != null ? value.getClass().getSimpleName() : "null");
            }
            System.out.println();
          }

          // Map to schema fields
          Object[] result = new Object[schemaFieldList.size()];
          for (int i = 0; i < schemaFieldList.size(); i++) {
            String schemaField = schemaFieldList.get(i);

            if ("_extra".equals(schemaField)) {
              // Collect unmapped fields as JSON
              if (rowCount <= 3) {
                System.out.printf("  Processing _extra field...\n");
              }
              result[i] = buildExtraFields(jsonRecord);
            } else {
              // Map schema field name to Splunk field name
              String splunkField = fieldMapping.getOrDefault(schemaField, schemaField);
              Object value = jsonRecord.get(splunkField);

              // DEBUG: Show the lookup process
              if (rowCount <= 3) {
                System.out.printf("  Looking up: schema='%s' -> splunk='%s'\n", schemaField, splunkField);
                System.out.printf("    Event data contains key '%s'? %s\n", splunkField, jsonRecord.containsKey(splunkField));
                System.out.printf("    RAW VALUE: '%s' (%s)\n", value, value != null ? value.getClass().getSimpleName() : "null");
              }

              // Jackson preserves types well: Integer, Double, Boolean, null, String
              result[i] = value;

              if (rowCount <= 3) {
                System.out.printf("  result[%d]: schema='%s' -> splunk='%s' -> value='%s' (%s)\n",
                    i, schemaField, splunkField, value,
                    value != null ? value.getClass().getSimpleName() : "null");
              }
            }
          }

          this.current = result;
          return true;
        }
      } catch (Exception e) {
        System.err.println("Error in JSON enumerator: " + e.getMessage());
        e.printStackTrace();
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
          System.out.println("=== RAW JSON LINE " + (rowCount + 1) + " ===");
          System.out.println(line);
          System.out.println("=== END RAW JSON ===");
        }

        return OBJECT_MAPPER.readValue(line, MAP_TYPE_REF);
      } catch (Exception e) {
        System.err.println("Failed to parse JSON line: " + line.substring(0, Math.min(100, line.length())));
        if (rowCount <= 3) {
          // Show more details for first few parsing errors
          System.err.println("Parsing error: " + e.getMessage());
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

      for (Map.Entry<String, Object> entry : eventData.entrySet()) {
        String fieldName = entry.getKey();

        // Only include fields that aren't explicitly mapped to schema fields
        boolean isExplicitField = explicitFields.contains(fieldName);
        boolean isMappedField = fieldMapping.values().contains(fieldName);

        if (rowCount <= 3) {
          System.out.printf("    Evaluating field '%s': explicit=%s, mapped=%s\n",
              fieldName, isExplicitField, isMappedField);
        }

        // Include field in _extra if it's not mapped to any schema field
        if (!isMappedField && !"_extra".equals(fieldName)) {
          extra.put(fieldName, entry.getValue());
          if (rowCount <= 3) {
            System.out.printf("    -> Including '%s' in _extra\n", fieldName);
          }
        } else if (rowCount <= 3) {
          System.out.printf("    -> Excluding '%s' from _extra\n", fieldName);
        }
      }

      if (rowCount <= 3) {
        System.out.printf("  _extra field contains %d unmapped fields: %s\n",
            extra.size(), extra.keySet());
      }

      String extraJson = serializeToJson(extra);

      if (rowCount <= 3) {
        System.out.printf("  _extra JSON result: %s\n", extraJson);
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
