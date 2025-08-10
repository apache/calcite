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
    // Check if flattening is enabled
    if (options != null && Boolean.TRUE.equals(options.get("flatten"))) {
      // Use parent class's scan method which handles flattening
      return super.scan(root);
    }

    // Check if this is a StorageProviderSource (HTTP, etc.) - use standard JSON processing
    if (source instanceof org.apache.calcite.adapter.file.storage.StorageProviderSource) {
      System.out.println("ParquetJsonScannableTable.scan: Detected StorageProviderSource, using standard JSON processing");
      return super.scan(root);
    }

    // Otherwise use Parquet file enumerator for file-based sources
    System.out.println("ParquetJsonScannableTable.scan: Using ParquetFileEnumerator for source: " + source.path());
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        final RelDataType rowType = getRowType(root.getTypeFactory());
        return new ParquetFileEnumerator<>(
            source, rowType, engineConfig.getBatchSize());
      }
    };
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // Get row type from parent class
    return super.getRowType(typeFactory);
  }

  @Override public String toString() {
    return "ParquetJsonScannableTable(" + source + ")";
  }
}
