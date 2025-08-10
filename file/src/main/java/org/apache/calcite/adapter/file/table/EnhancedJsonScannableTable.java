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
import org.apache.calcite.adapter.file.execution.vectorized.VectorizedFileEnumerator;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Enhanced JSON table with vectorized execution engine support.
 *
 * <p>Extends the standard JsonScannableTable to support multiple execution engines:
 * <ul>
 *   <li>LINQ4J: Traditional row-by-row processing</li>
 *   <li>VECTORIZED: Optimized columnar processing using Arrow</li>
 * </ul>
 *
 * <p>Supports JSON, YAML, and YML file formats.
 */
public class EnhancedJsonScannableTable extends JsonScannableTable {

  private final ExecutionEngineConfig engineConfig;
  private final Map<String, Object> options;

  /**
   * Creates an EnhancedJsonScannableTable.
   */
  public EnhancedJsonScannableTable(Source source, ExecutionEngineConfig engineConfig) {
    super(source);
    this.engineConfig = engineConfig;
    this.options = null;
  }

  /**
   * Creates an EnhancedJsonScannableTable with options.
   */
  public EnhancedJsonScannableTable(Source source, ExecutionEngineConfig engineConfig,
      Map<String, Object> options) {
    super(source, options);
    this.engineConfig = engineConfig;
    this.options = options;
  }

  /**
   * Creates an EnhancedJsonScannableTable with options and column casing.
   */
  public EnhancedJsonScannableTable(Source source, ExecutionEngineConfig engineConfig,
      Map<String, Object> options, String columnNameCasing) {
    super(source, options, columnNameCasing);
    this.engineConfig = engineConfig;
    this.options = options;
  }

  @Override public String toString() {
    return "EnhancedJsonScannableTable[engine=" + engineConfig.getEngineType() + "]";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();

        // Use vectorized processing if enabled
        if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.VECTORIZED) {
          // Create row-based JSON enumerator
          JsonEnumerator jsonEnumerator = new JsonEnumerator(getDataList(typeFactory));

          // Convert to iterator adapter
          EnumeratorIteratorAdapter adapter = new EnumeratorIteratorAdapter(jsonEnumerator);

          // Wrap with vectorized processing
          return new VectorizedFileEnumerator(adapter,
              getRowType(typeFactory), engineConfig.getBatchSize());
        } else {
          // Use traditional LINQ4J processing
          return new JsonEnumerator(getDataList(typeFactory));
        }
      }
    };
  }

  /**
   * Adapter that converts Enumerator to Iterator.
   */
  private static class EnumeratorIteratorAdapter implements java.util.Iterator<@Nullable Object[]>,
      AutoCloseable {
    private final Enumerator<@Nullable Object[]> enumerator;
    private boolean hasNext;
    private boolean checkedNext = false;

    EnumeratorIteratorAdapter(Enumerator<@Nullable Object[]> enumerator) {
      this.enumerator = enumerator;
    }

    @Override public boolean hasNext() {
      if (!checkedNext) {
        hasNext = enumerator.moveNext();
        checkedNext = true;
      }
      return hasNext;
    }

    @Override public @Nullable Object[] next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      checkedNext = false;
      return enumerator.current();
    }

    @Override public void close() throws Exception {
      enumerator.close();
    }
  }
}
