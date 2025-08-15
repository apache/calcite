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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator.JsonDataConverter;
import org.apache.calcite.adapter.file.format.json.SharedJsonData;
import org.apache.calcite.adapter.file.format.json.JsonSearchConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable {
  private final Source source;
  private final SharedJsonData sharedData;
  private final String jsonPath;
  private final JsonSearchConfig config;
  private @Nullable RelDataType rowType;
  protected @Nullable List<Object> dataList;
  protected final Map<String, Object> options;
  protected final String columnNameCasing;
  private @Nullable JsonDataConverter cachedConverter;
  private long lastModifiedTime = -1;

  public JsonTable(Source source) {
    this(source, null, "UNCHANGED");
  }

  public JsonTable(Source source, Map<String, Object> options) {
    this(source, options, "UNCHANGED");
  }
  
  public JsonTable(Source source, Map<String, Object> options, String columnNameCasing) {
    this.source = source;
    this.sharedData = null;
    this.jsonPath = null;
    this.config = null;
    this.options = options;
    this.columnNameCasing = columnNameCasing;
  }
  
  /**
   * Constructor for path-specific table using shared JSON data.
   *
   * @param sharedData The shared parsed JSON data
   * @param jsonPath The JSONPath to extract data from
   * @param config The JSON search configuration
   */
  public JsonTable(SharedJsonData sharedData, String jsonPath, JsonSearchConfig config) {
    this(sharedData, jsonPath, config, "UNCHANGED");
  }
  
  /**
   * Constructor for path-specific table using shared JSON data with column casing.
   *
   * @param sharedData The shared parsed JSON data
   * @param jsonPath The JSONPath to extract data from
   * @param config The JSON search configuration
   * @param columnNameCasing The column name casing strategy
   */
  public JsonTable(SharedJsonData sharedData, String jsonPath, JsonSearchConfig config, String columnNameCasing) {
    this.source = null;
    this.sharedData = sharedData;
    this.jsonPath = jsonPath;
    this.config = config;
    this.options = config.getOptions();
    this.columnNameCasing = columnNameCasing;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      if (cachedConverter == null) {
        cachedConverter = JsonEnumerator.deduceRowType(typeFactory, source, options, columnNameCasing);
      }
      rowType = cachedConverter.getRelDataType();
    }
    return rowType;
  }

  /** Returns the data list of the table. */
  public List<Object> getDataList(RelDataTypeFactory typeFactory) {
    // Check if we need to invalidate cache due to file changes
    boolean needsRefresh = false;
    if (source != null) {
      File file = source.file();
      if (file != null && file.exists()) {
        long currentModified = file.lastModified();
        if (lastModifiedTime == -1) {
          lastModifiedTime = currentModified;
        } else if (currentModified > lastModifiedTime) {
          needsRefresh = true;
          lastModifiedTime = currentModified;
        }
      }
    }
    
    if (dataList == null || needsRefresh) {
      if (needsRefresh) {
        // Clear cached data
        dataList = null;
        cachedConverter = null;
        rowType = null;
      }
      
      if (cachedConverter == null) {
        cachedConverter = JsonEnumerator.deduceRowType(typeFactory, source, options, columnNameCasing);
      }
      dataList = cachedConverter.getDataList();
    }
    return dataList;
  }


  @Override public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }
}
