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
    return performSearchForEnumerator(search, otherArgs, fieldList, explicitFields);
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
      List<String> wantedFields,
      Set<String> explicitFields) {
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
      return new SplunkResultEnumerator(in, wantedFields, explicitFields);
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

    System.out.printf(Locale.ROOT, "received %d results in %dms\n",
        dummy.getResultCount(),
        System.currentTimeMillis() - start);
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

  /** Implementation of {@link Enumerator} that parses
   * results from a Splunk REST call.
   *
   * <p>The element type is either {@code String} or {@code String[]}, depending
   * on the value of {@code source}.
   *
   * <p>Enhanced to support "_extra" field collection for CIM models.
   */
  public static class SplunkResultEnumerator implements Enumerator<Object> {
    private final CSVReader csvReader;
    private String[] fieldNames = new String[0];
    private int[] sources = new int[0];
    private Object current = "";
    private boolean hasExtraField = false;
    private int extraFieldIndex = -1;
    private final Set<String> explicitFields;

    /**
     * Where to find the singleton field, or whether to map. Values:
     *
     * <ul>
     * <li>Non-negative The index of the sole field</li>
     * <li>-1 Generate a singleton null field for every record</li>
     * <li>-2 Return line intact</li>
     * <li>-3 Use sources to re-map</li>
     * </ul>
     */
    private int source = -1;

    public SplunkResultEnumerator(InputStream in, List<String> wantedFields, Set<String> explicitFields) {
      this.explicitFields = explicitFields;
      csvReader =
          new CSVReader(
              new BufferedReader(
                  new InputStreamReader(in, StandardCharsets.UTF_8)));
      try {
        String[] headerRow = csvReader.readNext();
        if (headerRow != null && headerRow.length > 0
            && !(headerRow.length == 1 && headerRow[0].isEmpty())) {
          this.fieldNames = headerRow;
          final List<String> headerList = Arrays.asList(fieldNames);

          // Check if "_extra" field is requested
          hasExtraField = wantedFields.contains("_extra");
          extraFieldIndex = hasExtraField ? wantedFields.indexOf("_extra") : -1;

          if (wantedFields.size() == 1) {
            // Yields 0 or higher if wanted field exists.
            // Yields -1 if wanted field does not exist.
            source = headerList.indexOf(wantedFields.get(0));
          } else if (wantedFields.equals(headerList)) {
            source = -2;
          } else {
            source = -3;
            sources = new int[wantedFields.size()];
            int i = 0;
            for (String wantedField : wantedFields) {
              sources[i++] = headerList.indexOf(wantedField);
            }
          }
        }
      } catch (IOException e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        LOGGER.warn("{}\n{}", e.getMessage(), sw);
      }
    }

    @Override public Object current() {
      return current;
    }

    @Override public boolean moveNext() {
      try {
        String[] line;
        while ((line = csvReader.readNext()) != null) {
          if (line.length == fieldNames.length) {
            switch (source) {
            case -3:
              // Re-map using sources
              String[] mapped = new String[sources.length];
              for (int i = 0; i < sources.length; i++) {
                int source1 = sources[i];
                mapped[i] = source1 < 0 ? "" : line[source1];
              }

              // Handle "_extra" field if present
              if (hasExtraField && extraFieldIndex >= 0) {
                mapped[extraFieldIndex] = collectExtraFields(line);
              }

              this.current = mapped;
              break;
            case -2:
              // Return line as is. No need to re-map.
              current = line;
              break;
            case -1:
              // Singleton empty string instead of null
              this.current = "";
              break;
            default:
              this.current = line[source];
              break;
            }
            return true;
          }
        }
      } catch (IOException e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        LOGGER.warn("{}\n{}", e.getMessage(), sw);
      }
      return false;
    }

    /**
     * Collect fields that are in Splunk results but not defined in the table schema.
     * Serialize them as JSON for the "_extra" field.
     */
    private String collectExtraFields(String[] line) {
      Map<String, String> extraFields = new HashMap<>();

      // Find fields that exist in Splunk but aren't defined in the table schema
      for (int i = 0; i < fieldNames.length && i < line.length; i++) {
        String fieldName = fieldNames[i];

        // If field is not explicitly defined in the table schema and isn't "_extra" itself, collect it
        if (!explicitFields.contains(fieldName) && !"_extra".equals(fieldName)) {
          extraFields.put(fieldName, line[i]);
        }
      }

      return serializeToJson(extraFields);
    }

    /**
     * Simple JSON serialization for extra fields.
     */
    private String serializeToJson(Map<String, String> extraFields) {
      if (extraFields.isEmpty()) {
        return "{}";
      }

      StringBuilder json = new StringBuilder("{");
      boolean first = true;

      for (Map.Entry<String, String> entry : extraFields.entrySet()) {
        if (!first) {
          json.append(",");
        }
        first = false;

        json.append("\"").append(escapeJson(entry.getKey())).append("\":");

        String value = entry.getValue();
        if (value == null || value.isEmpty()) {
          json.append("null");
        } else {
          json.append("\"").append(escapeJson(value)).append("\"");
        }
      }

      json.append("}");
      return json.toString();
    }

    /**
     * Simple JSON string escaping.
     */
    private String escapeJson(String str) {
      return str.replace("\\", "\\\\")
          .replace("\"", "\\\"")
          .replace("\n", "\\n")
          .replace("\r", "\\r")
          .replace("\t", "\\t");
    }

    @Override public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override public void close() {
      try {
        csvReader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
