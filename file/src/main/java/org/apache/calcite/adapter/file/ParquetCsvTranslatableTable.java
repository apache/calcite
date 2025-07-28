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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CSV table that uses the Parquet execution engine for columnar processing.
 *
 * <p>This table implementation provides:
 * <ul>
 *   <li>In-memory Parquet format storage</li>
 *   <li>Row group-based streaming</li>
 *   <li>Compressed columnar storage</li>
 *   <li>Efficient predicate pushdown</li>
 * </ul>
 */
public class ParquetCsvTranslatableTable extends CsvTranslatableTable
    implements TranslatableTable {

  private final ExecutionEngineConfig engineConfig;

  public ParquetCsvTranslatableTable(Source source,
                                     RelProtoDataType protoRowType,
                                     ExecutionEngineConfig engineConfig) {
    super(source, protoRowType);
    this.engineConfig = engineConfig;
  }

  @Override public Enumerable<Object> project(final DataContext root, final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();

        // Use Parquet-style columnar processing
        return new ParquetEnumerator<>(source, cancelFlag,
            getFieldTypes(typeFactory), fields, engineConfig.getBatchSize(),
            engineConfig.getMemoryThreshold());
      }
    };
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    // Request all fields
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = CsvEnumerator.identityList(fieldCount);

    // Create a custom table scan that uses Parquet processing
    return new ParquetCsvTableScan(context.getCluster(), relOptTable, this, fields);
  }

  /**
   * Custom table scan for Parquet-based CSV processing.
   */
  private static class ParquetCsvTableScan extends CsvTableScan {

    ParquetCsvTableScan(RelOptCluster cluster, RelOptTable table,
                        ParquetCsvTranslatableTable csvTable, int[] fields) {
      super(cluster, table, csvTable, fields);
    }

    // Remove the override - let parent handle the copy
  }

  @Override public String toString() {
    return "ParquetCsvTranslatableTable(" + source + ")";
  }
}
