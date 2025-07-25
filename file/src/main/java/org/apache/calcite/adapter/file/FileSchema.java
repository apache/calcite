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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import org.apache.poi.ss.usermodel.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
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


  private void convertExcelFilesToJson(File baseDirectory) {

    // Get the list of all files and directories
    File[] files = baseDirectory.listFiles();

    if (files != null) {
      for (File file : files) {
        // If it's a directory, recurse into it
        if (file.isDirectory()) {
          convertExcelFilesToJson(file);
        } else if (file.getName().endsWith(".xlsx") && !file.getName().startsWith("~")) {
          // If it's a file ending in .xlsx, convert it
          try {
            ExcelToJsonConverter.convertFileToJson(file);
          } catch (Exception e) {
            e.printStackTrace();
            System.out.println("!");
          }
        }
      }
    }
  }

  private File[] getFilesInDir(File dir) {
    List<File> files = new ArrayList<>();
    File[] fileArr = dir.listFiles();

    for (File file : fileArr) {
      if (!file.getName().startsWith("._")) {
        if (file.isDirectory()) {
          files.addAll(Arrays.asList(getFilesInDir(file)));
        } else {
          final String nameSansGz = trim(file.getName(), ".gz");
          if (nameSansGz.endsWith(".csv")
              || nameSansGz.endsWith(".json")
              || nameSansGz.endsWith(".hml")
              || nameSansGz.endsWith(".yaml")
              || nameSansGz.endsWith(".yml")) {
            files.add(file);
          }
        }
      }
    }

    return files.toArray(new File[0]);
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (Map<String, Object> tableDef : this.tables) {
      addTable(builder, tableDef);
    }

    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    if (baseDirectory != null) {

      convertExcelFilesToJson(baseDirectory);

      final Source baseSource = Sources.of(baseDirectory);
      File[] files = getFilesInDir(baseDirectory);
      if (files == null) {
        System.out.println("directory " + baseDirectory + " not found");
        files = new File[0];
      }
      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        Source source = Sources.of(file);
        Source sourceSansGz = source.trim(".gz");
        Source sourceSansJson = sourceSansGz.trimOrNull(".json");
        if (sourceSansJson == null) {
          sourceSansJson = sourceSansGz.trimOrNull(".yaml");
        }
        if (sourceSansJson == null) {
          sourceSansJson = sourceSansGz.trimOrNull(".yml");
        }
        if (sourceSansJson == null) {
          sourceSansJson = sourceSansGz.trimOrNull(".hml");
        }
        if (sourceSansJson != null) {
          String tableName = sourceSansJson.relative(baseSource).path()
              .replace(File.separator, ".")
              .replaceAll("\\s+", "_");
          addTable(builder, source, tableName, null);
        }
        final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
        if (sourceSansCsv != null) {
          String tableName = sourceSansCsv.relative(baseSource).path()
              .replace(File.separator, ".")
              .replaceAll("\\s+", "_");
          addTable(builder, source, tableName, null);
        }
      }
    }

    return builder.build();
  }

  private boolean addTable(ImmutableMap.Builder<String, Table> builder,
      Map<String, Object> tableDef) {
    final String tableName = (String) tableDef.get("name");
    final String url = (String) tableDef.get("url");
    Source source0;
    if (url.startsWith("s3://")) {
      source0 = Sources.of(url);
    } else {
      source0 = Sources.url(url);
    }
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
    Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yaml");
    }
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".yml");
    }
    if (sourceSansJson == null) {
      sourceSansJson = sourceSansGz.trimOrNull(".hml");
    }
    if (sourceSansJson != null) {
      final Table table = new JsonScannableTable(source);
      builder.put(Util.first(tableName, sourceSansJson.path()), table);
      return true;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      final Table table = new CsvTranslatableTable(source, null);
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
