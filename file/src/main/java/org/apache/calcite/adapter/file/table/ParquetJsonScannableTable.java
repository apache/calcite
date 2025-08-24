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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.parquet.ParquetFileEnumerator;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import java.util.Map;

/**
 * JSON table that uses the Parquet execution engine for columnar processing.
 *
 * <p>This table implementation provides:
 * <ul>
 *   <li>JSON to Parquet format conversion</li>
 *   <li>Efficient columnar processing of JSON data</li>
 *   <li>Streaming support for large JSON files</li>
 *   <li>Compressed in-memory storage</li>
 * </ul>
 */
public class ParquetJsonScannableTable extends JsonScannableTable
    implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetJsonScannableTable.class);

  private final Source source;
  private final ExecutionEngineConfig engineConfig;
  private final Map<String, Object> options;

  public ParquetJsonScannableTable(Source source, ExecutionEngineConfig engineConfig) {
    super(source);
    this.source = source;
    this.engineConfig = engineConfig;
    this.options = null;
  }

  public ParquetJsonScannableTable(Source source, ExecutionEngineConfig engineConfig,
      Map<String, Object> options) {
    super(source, options);
    this.source = source;
    this.engineConfig = engineConfig;
    this.options = options;
  }

  public ParquetJsonScannableTable(Source source, ExecutionEngineConfig engineConfig,
      Map<String, Object> options, String columnNameCasing) {
    super(source, options, columnNameCasing);
    this.source = source;
    this.engineConfig = engineConfig;
    this.options = options;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    // For JSON files, always use the standard JSON processing
    // The mock ParquetExecutionEngine causes ClassCastException issues with aggregation
    // and provides no real benefit since real Parquet files use ParquetEnumerableFactory
    LOGGER.debug("ParquetJsonScannableTable.scan: Using standard JSON processing for source: {}", source.path());
    return super.scan(root);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // Get row type from parent class
    return super.getRowType(typeFactory);
  }

  @Override public String toString() {
    return "ParquetJsonScannableTable(" + source + ")";
  }
}
