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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

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

  ParquetJsonScannableTable(Source source, ExecutionEngineConfig engineConfig) {
    super(source);
    this.source = source;
    this.engineConfig = engineConfig;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
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
