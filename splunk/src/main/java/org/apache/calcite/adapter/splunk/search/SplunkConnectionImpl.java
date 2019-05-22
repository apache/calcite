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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.runtime.HttpUtils.appendURLEncodedArgs;
import static org.apache.calcite.runtime.HttpUtils.post;

/**
 * Implementation of {@link SplunkConnection} based on Splunk's REST API.
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
  String sessionKey;
  final Map<String, String> requestHeaders = new HashMap<>();

  public SplunkConnectionImpl(String url, String username, String password)
      throws MalformedURLException {
    this(new URL(url), username, password);
  }

  public SplunkConnectionImpl(URL url, String username, String password) {
    this.url      = url;
    this.username = username;
    this.password = password;
    connect();
  }

  private static void close(Closeable c) {
    try {
      c.close();
    } catch (Exception ignore) {
      // ignore
    }
  }

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
      close(rd);
    }
  }

  public void getSearchResults(String search, Map<String, String> otherArgs,
      List<String> fieldList, SearchResultListener srl) {
    assert srl != null;
    Enumerator<Object> x = getSearchResults_(search, otherArgs, fieldList, srl);
    assert x == null;
  }

  public Enumerator<Object> getSearchResultEnumerator(String search,
      Map<String, String> otherArgs, List<String> fieldList) {
    return getSearchResults_(search, otherArgs, fieldList, null);
  }

  private Enumerator<Object> getSearchResults_(
      String search,
      Map<String, String> otherArgs,
      List<String> wantedFields,
      SearchResultListener srl) {
    String searchUrl =
        String.format(Locale.ROOT,
            "%s://%s:%d/services/search/jobs/export",
            url.getProtocol(),
            url.getHost(),
            url.getPort());

    StringBuilder data = new StringBuilder();
    Map<String, String> args = new LinkedHashMap<>();
    if (otherArgs != null) {
      args.putAll(otherArgs);
    }
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
      if (srl == null) {
        return new SplunkResultEnumerator(in, wantedFields);
      } else {
        parseResults(
            in,
            srl);
        return null;
      }
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      LOGGER.warn("{}\n{}", e.getMessage(), sw);
      return srl == null ? Linq4j.emptyEnumerator() : null;
    }
  }

  private static void parseResults(InputStream in, SearchResultListener srl) {
    try (CSVReader r = new CSVReader(
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
    } catch (IOException ignore) {
      StringWriter sw = new StringWriter();
      ignore.printStackTrace(new PrintWriter(sw));
      LOGGER.warn("{}\n{}", ignore.getMessage(), sw);
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

    if (search == null) {
      printUsage("Missing required argument: search");
    }
    if (field_list == null) {
      printUsage("Missing required argument: field_list");
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


    CountingSearchResultListener dummy =
        new CountingSearchResultListener(
            Boolean.valueOf(argsMap.get("-print")));
    long start = System.currentTimeMillis();
    c.getSearchResults(search, searchArgs, null, dummy);

    System.out.printf(Locale.ROOT, "received %d results in %dms\n",
        dummy.getResultCount(),
        System.currentTimeMillis() - start);
  }

  /** Implementation of
   * {@link SearchResultListener}
   * interface that just counts the results. */
  public static class CountingSearchResultListener
      implements SearchResultListener {
    String[] fieldNames = null;
    int resultCount = 0;
    final boolean print;

    public CountingSearchResultListener(boolean print) {
      this.print = print;
    }

    public void setFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
    }

    public boolean processSearchResult(String[] values) {
      resultCount++;
      if (print) {
        for (int i = 0; i < this.fieldNames.length; ++i) {
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

  /** Implementation of {@link org.apache.calcite.linq4j.Enumerator} that parses
   * results from a Splunk REST call.
   *
   * <p>The element type is either {@code String} or {@code String[]}, depending
   * on the value of {@code source}.</p> */
  public static class SplunkResultEnumerator implements Enumerator<Object> {
    private final CSVReader csvReader;
    private String[] fieldNames;
    private int[] sources;
    private Object current;

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
    private int source;

    public SplunkResultEnumerator(InputStream in, List<String> wantedFields) {
      csvReader =
          new CSVReader(
              new BufferedReader(
                  new InputStreamReader(in, StandardCharsets.UTF_8)));
      try {
        fieldNames = csvReader.readNext();
        if (fieldNames == null
            || fieldNames.length == 0
            || fieldNames.length == 1 && fieldNames[0].isEmpty()) {
          // do nothing
        } else {
          final List<String> headerList = Arrays.asList(fieldNames);
          if (wantedFields.size() == 1) {
            // Yields 0 or higher if wanted field exists.
            // Yields -1 if wanted field does not exist.
            source = headerList.indexOf(wantedFields.get(0));
            assert source >= -1;
            sources = null;
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
      } catch (IOException ignore) {
        StringWriter sw = new StringWriter();
        ignore.printStackTrace(new PrintWriter(sw));
        LOGGER.warn("{}\n{}", ignore.getMessage(), sw);
      }
    }

    public Object current() {
      return current;
    }

    public boolean moveNext() {
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
                mapped[i] = source1 < 0 ? null : line[source1];
              }
              this.current = mapped;
              break;
            case -2:
              // Return line as is. No need to re-map.
              current = line;
              break;
            case -1:
              // Singleton null
              this.current = null;
              break;
            default:
              this.current = line[source];
              break;
            }
            return true;
          }
        }
      } catch (IOException ignore) {
        StringWriter sw = new StringWriter();
        ignore.printStackTrace(new PrintWriter(sw));
        LOGGER.warn("{}\n{}", ignore.getMessage(), sw);
      }
      return false;
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }

    public void close() {
      try {
        csvReader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

// End SplunkConnectionImpl.java
