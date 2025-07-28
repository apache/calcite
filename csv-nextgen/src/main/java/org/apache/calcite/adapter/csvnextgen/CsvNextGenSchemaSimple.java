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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Simplified schema for CsvNextGen adapter.
 * Scans directory for CSV/TSV files and creates tables.
 */
public class CsvNextGenSchemaSimple extends AbstractSchema {
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  private final File baseDirectory;
  private final String engineType;
  private final int batchSize;
  private final boolean header;

  public CsvNextGenSchemaSimple(File baseDirectory, String engineType,
      int batchSize, boolean header) {
    this.baseDirectory = baseDirectory;
    this.engineType = engineType;
    this.batchSize = batchSize;
    this.header = header;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (baseDirectory == null || !baseDirectory.exists()) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    File[] files = baseDirectory.listFiles((dir, name) ->
        name.endsWith(".csv") || name.endsWith(".tsv"));

    if (files != null) {
      Source baseSource = Sources.of(baseDirectory);

      for (File file : files) {
        String fileName = file.getName();
        String tableName = createTableName(fileName, baseSource, file);
        Source source = Sources.of(file);

        CsvNextGenTableSimple table =
            new CsvNextGenTableSimple(source, null, engineType, batchSize, header);
        builder.put(tableName, table);
      }
    }

    return builder.build();
  }

  /**
   * Creates a table name from the file name and path.
   */
  private String createTableName(String fileName, Source baseSource, File file) {
    // Remove extension
    String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));

    // Create table name from relative path
    Source fileSource = Sources.of(file);
    String relativePath = fileSource.relative(baseSource).path()
        .replace(File.separator, ".");

    // Remove extension from relative path and replace whitespace with underscores
    String tableName = relativePath.substring(0, relativePath.lastIndexOf('.'));
    // Convert to uppercase for SQL compatibility
    return WHITESPACE_PATTERN.matcher(tableName).replaceAll("_").toUpperCase(java.util.Locale.ROOT);
  }
}
