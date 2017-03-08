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

import org.apache.calcite.adapter.csv.CsvFilterableTable;
import org.apache.calcite.adapter.csv.JsonTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 */
class FileSchema extends AbstractSchema {
  private final ImmutableList<Map<String, Object>> tables;
  private final File baseDirectory;

  /**
   * Creates an HTML tables schema.
   *
   * @param parentSchema Parent schema
   * @param name Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   * @param tables List containing HTML table identifiers
   */
  FileSchema(SchemaPlus parentSchema, String name, File baseDirectory,
      List<Map<String, Object>> tables) {
    this.tables = ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (Map<String, Object> tableDef : this.tables) {
      String tableName = (String) tableDef.get("name");
      try {
        addTable(builder, tableDef);
      } catch (MalformedURLException e) {
        throw new RuntimeException("Unable to instantiate table for: "
            + tableName);
      }
    }

    return builder.build();
  }

  private void addTable(ImmutableMap.Builder<String, Table> builder,
      Map<String, Object> tableDef) throws MalformedURLException {
    final String tableName = (String) tableDef.get("name");
    final String url = (String) tableDef.get("url");
    final Source source0 = Sources.url(url);
    final Source source;
    if (baseDirectory == null) {
      source = source0;
    } else {
      source = Sources.of(baseDirectory).append(source0);
    }

    final Source sourceSansGz = source.trim(".gz");
    final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson != null) {
      JsonTable table = new JsonTable(source);
      builder.put(Util.first(tableName, sourceSansJson.path()), table);
      return;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      final Table table = new CsvFilterableTable(source, null);
      builder.put(Util.first(tableName, sourceSansCsv.path()), table);
      return;
    }

    try {
      FileTable table = FileTable.create(source, tableDef);
      builder.put(Util.first(tableName, source.path()), table);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate table for: "
          + tableName);
    }
  }
}

// End FileSchema.java
