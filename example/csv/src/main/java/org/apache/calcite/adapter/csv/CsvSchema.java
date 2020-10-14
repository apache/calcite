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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.adapter.file.JsonScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files.
 * Each table in the schema is a CSV file in that directory.
 *
 * 与某个目录下csv文件相对应的schema：一个csv文件代表一个表。
 */
public class CsvSchema extends AbstractSchema {

  // 文件目录
  private final File directoryFile;

  // 特点：可扫描的、可过滤的、可传递的
  private final CsvTable.Flavor flavor;

  private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *
   * @param directoryFile Directory that holds {@code .csv} files
   *                      保存scv文件的目录。
   *
   * @param flavor     Whether to instantiate flavor tables that undergo(经过) query optimization
   *                   是否实例化经过查询优化的 flavor tables
   */
  public CsvSchema(File directoryFile, CsvTable.Flavor flavor) {
    super();
    this.directoryFile = directoryFile;
    this.flavor = flavor;
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
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  // 返回 表名称 到 表对象的映射。
  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    // Look for files in the directory ending in ".csv", ".csv.gz", ".json", ".json.gz".
    // 查找指定目录下以 ".csv", ".csv.gz", ".json", ".json.gz" 结尾的文件。
    final Source baseSource = Sources.of(directoryFile);
    File[] files = directoryFile.listFiles((dir, name) -> {
      final String nameSansGz = trim(name, ".gz");
      return nameSansGz.endsWith(".csv")
          || nameSansGz.endsWith(".json");
    });

    // 如果没有文件，则赋值为空数组。
    if (files == null) {
      System.out.println("directory " + directoryFile + " not found");
      files = new File[0];
    }

    // Build a map from table name to table; each file becomes a table.
    // 将每个文件看作一个表：构造表名称到表的映射。
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
      if (sourceSansJson != null) {
        final Table table = new JsonScannableTable(source);
        builder.put(sourceSansJson.relative(baseSource).path(), table);
      }
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if (sourceSansCsv != null) {
        final Table table = createTable(source);
        builder.put(sourceSansCsv.relative(baseSource).path(), table);
      }
    }
    return builder.build();
  }

  /**
   * Creates different sub-type of table based on the "flavor" attribute.
   *
   * fixme 基于flavor属性，创建 Table 具体的实现类。
   */
  private Table createTable(Source source) {
    // 可传递的
    switch (flavor) {
    case TRANSLATABLE:
      return new CsvTranslatableTable(source, null);
    case SCANNABLE:
      return new CsvScannableTable(source, null);
    case FILTERABLE:
      return new CsvFilterableTable(source, null);
    default:
      throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }
}
