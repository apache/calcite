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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;
import org.apache.calcite.adapter.file.execution.vectorized.VectorizedCsvEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enhanced CSV table with vectorized execution engine support.
 *
 * <p>Extends the standard CsvTranslatableTable to support multiple execution engines:
 * <ul>
 *   <li>LINQ4J: Traditional row-by-row processing</li>
 *   <li>VECTORIZED: Optimized columnar processing using Arrow</li>
 * </ul>
 */
public class EnhancedCsvTranslatableTable extends CsvTranslatableTable {

  private final ExecutionEngineConfig engineConfig;

  /** Creates an EnhancedCsvTranslatableTable. */
  public EnhancedCsvTranslatableTable(Source source, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig engineConfig) {
    super(source, protoRowType);
    this.engineConfig = engineConfig;
  }

  /** Creates an EnhancedCsvTranslatableTable with column casing. */
  public EnhancedCsvTranslatableTable(Source source, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig engineConfig, String columnCasing) {
    super(source, protoRowType, columnCasing);
    this.engineConfig = engineConfig;
  }

  @Override public String toString() {
    return "EnhancedCsvTranslatableTable[engine=" + engineConfig.getEngineType() + "]";
  }


  /** Returns an enumerable over a given projection of the fields. */
  @Override @SuppressWarnings("unused") // called from generated code
  public Enumerable<Object> project(final DataContext root,
      final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();

        // Use vectorized processing if enabled
        if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.VECTORIZED) {
          // Create a vectorized enumerator that processes data in batches
          // while maintaining compatibility with Calcite's type system
          //noinspection unchecked
          return (Enumerator<Object>) new VectorizedCsvEnumerator<>(source, cancelFlag,
              getFieldTypes(typeFactory), ImmutableIntList.of(fields),
              engineConfig.getBatchSize());
        } else {
          // Use traditional LINQ4J processing
          //noinspection unchecked
          return (Enumerator<Object>) new CsvEnumerator<>(source, cancelFlag,
              getFieldTypes(typeFactory), ImmutableIntList.of(fields));
        }
      }
    };
  }

  /**
   * Adapter that converts Enumerator of Object arrays to Enumerator of Object.
   */
  private static class ObjectArrayToObjectEnumerator implements Enumerator<Object> {
    private final Enumerator<@Nullable Object[]> delegate;

    ObjectArrayToObjectEnumerator(Enumerator<@Nullable Object[]> delegate) {
      this.delegate = delegate;
    }

    @Override public Object current() {
      return delegate.current();
    }

    @Override public boolean moveNext() {
      return delegate.moveNext();
    }

    @Override public void reset() {
      delegate.reset();
    }

    @Override public void close() {
      delegate.close();
    }
  }

  /**
   * Enumerator that applies projection to rows.
   */
  private static class ProjectingObjectEnumerator implements Enumerator<Object> {
    private final Enumerator<@Nullable Object[]> delegate;
    private final int[] fields;

    ProjectingObjectEnumerator(Enumerator<@Nullable Object[]> delegate, int[] fields) {
      this.delegate = delegate;
      this.fields = fields;
    }

    @Override public Object current() {
      Object[] row = delegate.current();
      if (row == null) {
        return null;
      }
      // For aggregations, we need to return the full row
      // The aggregation functions will extract the needed columns
      return row;
    }

    @Override public boolean moveNext() {
      return delegate.moveNext();
    }

    @Override public void reset() {
      delegate.reset();
    }

    @Override public void close() {
      delegate.close();
    }
  }

  /**
   * Adapter that converts Enumerator to Iterator.
   */
  private static class EnumeratorIteratorAdapter
      implements java.util.Iterator<Object[]>, AutoCloseable {
    private final Enumerator<Object[]> enumerator;
    private boolean hasNext;
    private boolean checkedNext = false;

    EnumeratorIteratorAdapter(Enumerator<Object[]> enumerator) {
      this.enumerator = enumerator;
    }

    @Override public boolean hasNext() {
      if (!checkedNext) {
        hasNext = enumerator.moveNext();
        checkedNext = true;
      }
      return hasNext;
    }

    @Override public Object[] next() {
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
