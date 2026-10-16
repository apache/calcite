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
package org.apache.calcite.adapter.file;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import au.com.bytecode.opencsv.CSVParser;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 *
 * <p>By default, only local {@code file} sources are dereferenced. Table operands
 * naming remote URLs make the host fetch an arbitrary, model-supplied URL and return the
 * response as rows, so remote URLs must be explicitly allowed by the operator via the
 * {@link CalciteSystemProperty#FILE_URLS_ALLOWED} system property.
 */
class FileSchema extends AbstractSchema {
  private final ImmutableList<Map<String, Object>> tables;
  private final @Nullable File baseDirectory;

  /**
   * Creates an HTML tables schema.
   *
   * @param parentSchema  Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   * @param tables        List containing HTML table identifiers, or null
   */
  FileSchema(SchemaPlus parentSchema, String name, @Nullable File baseDirectory,
      @Nullable List<Map<String, Object>> tables) {
    this.tables =
        tables == null ? ImmutableList.of()
            : ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string.
   */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null.
   */
  private static @Nullable String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (Map<String, Object> tableDef : this.tables) {
      addTable(builder, tableDef);
    }

    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    if (baseDirectory != null) {
      final Source baseSource = Sources.of(baseDirectory);
      File[] files = baseDirectory.listFiles((dir, name) -> {
        final String nameSansGz = trim(name, ".gz");
        return nameSansGz.endsWith(".csv")
            || nameSansGz.endsWith(".json");
      });
      if (files == null) {
        System.out.println("directory " + baseDirectory + " not found");
        files = new File[0];
      }
      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        Source source = Sources.of(file);
        Source sourceSansGz = source.trim(".gz");
        final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
        if (sourceSansJson != null) {
          addTable(builder, source, sourceSansJson.relative(baseSource).path(),
              null);
        }
        final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
        if (sourceSansCsv != null) {
          addTable(builder, source, sourceSansCsv.relative(baseSource).path(),
              null);
        }
      }
    }

    return builder.build();
  }

  // Compile regex pattern for allowed remote urls just once
  private static final @Nullable Supplier<Pattern> FILE_URLS_ALLOWED_PATTERN =
      CalciteSystemProperty.FILE_URLS_ALLOWED.value().isEmpty()
          ? null
          : Suppliers.memoize(() -> {
            String allowedUrls = CalciteSystemProperty.FILE_URLS_ALLOWED.value();
            try {
              return Pattern.compile(allowedUrls);
            } catch (PatternSyntaxException e) {
              throw new IllegalArgumentException("Invalid regular expression '" + allowedUrls
                  + "' in system property \"calcite.file.urls.allowed\"", e);
            }
          });

  /** Checks that a model-supplied table URL may be dereferenced by this
   * adapter: a local {@code file} URL always, a remote one (any other
   * protocol, or a {@code file} URL naming a host, such as a Windows UNC path)
   * only when it matches the regular expression
   * {@link CalciteSystemProperty#FILE_URLS_ALLOWED}. */
  static void checkUrlAllowed(String url) {
    checkUrlAllowed(url, FILE_URLS_ALLOWED_PATTERN);
  }

  static void checkUrlAllowed(String url, @Nullable Supplier<Pattern> pattern) {
    final URI uri;
    try {
      uri = new URI(url);
      uri.toURL();
    } catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
      // Malformed; Sources.url parses the same way and will report the error
      return;
    }
    if (isLocalUri(uri) || (pattern != null && pattern.get().matcher(url).matches())) {
      return;
    }
    throw new IllegalArgumentException("Remote URL '" + url
        + "' is not allowed, it does not match the \"calcite.file.urls.allowed\" pattern");
  }

  /**
   * Returns whether a URI is local: a "file" URL is local unless it names a host,
   * as an authority ("file://host/share") or, as Windows resolves it to a UNC path, as a path
   * starting with two slashes or backslashes ("file:////host/share", "file:%5C%5Chost%5Cshare").
   */
  static boolean isLocalUri(URI uri) {
    final String path =
        Util.first(uri.isOpaque() ? uri.getSchemeSpecificPart() : uri.getPath(), "");
    return "file".equalsIgnoreCase(uri.getScheme())
        && Util.isNullOrEmpty(uri.getRawAuthority())
        && !path.replace('\\', '/').startsWith("//");
  }

  private boolean addTable(ImmutableMap.Builder<String, Table> builder,
      Map<String, Object> tableDef) {
    final String tableName = (String) tableDef.get("name");
    final String url = (String) tableDef.get("url");
    checkUrlAllowed(url);
    final Source source0 = Sources.url(url);
    final Source source;
    if (baseDirectory == null) {
      source = source0;
    } else {
      source = Sources.of(baseDirectory).append(source0);
    }
    return addTable(builder, source, tableName, tableDef);
  }

  private static boolean addTable(ImmutableMap.Builder<String, Table> builder,
      Source source, String tableName, @Nullable Map<String, Object> tableDef) {
    final Source sourceSansGz = source.trim(".gz");
    final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson != null) {
      final Table table = new JsonScannableTable(source);
      builder.put(Util.first(tableName, sourceSansJson.path()), table);
      return true;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      final Table table =
          new CsvTranslatableTable(source, null, CSVParser.DEFAULT_SEPARATOR);
      builder.put(Util.first(tableName, sourceSansCsv.path()), table);
      return true;
    }

    if (tableDef != null) {
      try {
        FileTable table = FileTable.create(source, tableDef);
        builder.put(Util.first(tableName, source.path()), table);
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate table for: "
            + tableName);
      }
    }

    return false;
  }
}
