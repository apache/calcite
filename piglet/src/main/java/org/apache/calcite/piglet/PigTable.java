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
package org.apache.calcite.piglet;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A non-queriable table that contains only row type to represent a Pig Table. This table is used
 * for constructing Calcite logical plan from Pig DAG.
 */
public class PigTable extends AbstractTable implements ScannableTable {
  // Dummy statistics with 10 rows for any table
  private static final Statistic DUMMY_STATISTICS = Statistics.of(10.0, ImmutableList.of());
  private final RelDataType rowType;

  private PigTable(RelDataType rowType) {
    this.rowType = rowType;
  }

  /**
   * Creates a {@link RelOptTable} for a schema only table.
   *
   * @param schema Catalog object
   * @param rowType Relational schema for the table
   * @param names Names of Pig table
   */
  public static RelOptTable createRelOptTable(RelOptSchema schema,
      RelDataType rowType, List<String> names) {
    final PigTable pigTable = new PigTable(rowType);
    return RelOptTableImpl.create(schema, rowType, names, pigTable,
        c -> Expressions.constant(Boolean.TRUE));
  }

  @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return rowType;
  }

  @Override public Statistic getStatistic() {
    return DUMMY_STATISTICS;
  }

  @Override public Enumerable<@Nullable Object[]> scan(final DataContext root) {
    return null;
  }
}
