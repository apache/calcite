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
package org.apache.calcite.plan;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link RelOptTable} for temporary spool tables.
 *
 * <p>This table represents temporary storage used by spool operators
 * during query execution. It's used for planning purposes only and
 * will be converted to appropriate physical operators later.
 */
public class SpoolRelOptTable implements RelOptTable {
  private final @Nullable RelOptSchema schema;
  private final RelDataType rowType;
  private final String name;
  private final double rowCount;
  private final Table table;

  /**
   * Creates a SpoolRelOptTable with explicit row count.
   *
   * @param schema the schema this table belongs to (can be null for temporary tables)
   * @param rowType the row type of the data that will be stored in this spool
   * @param name optional name for the spool table
   * @param rowCount the estimated number of rows that will be materialized in this spool
   */
  public SpoolRelOptTable(
      @Nullable RelOptSchema schema,
      RelDataType rowType,
      String name,
      double rowCount) {
    this.schema = schema;
    this.rowType = rowType;
    this.name = name;
    this.rowCount = rowCount;
    // Use standard ListTransientTable with custom statistics for accurate cost estimation
    this.table = new ListTransientTable(name, rowType) {
      @Override public Statistic getStatistic() {
        return Statistics.of(rowCount, ImmutableList.of());
      }
    };
  }

  @Override public RelNode toRel(ToRelContext context) {
    // This shouldn't be called during planning - spools are created differently
    throw new UnsupportedOperationException("SpoolRelOptTable.toRel should not be called");
  }

  @Override public List<String> getQualifiedName() {
    return ImmutableList.of("TEMP", name);
  }

  @Override public double getRowCount() {
    // Return the actual row count of the materialized data in this spool
    return rowCount;
  }

  @Override public RelDataType getRowType() {
    return rowType;
  }

  @Override public @Nullable RelOptSchema getRelOptSchema() {
    return schema;
  }

  @Override public @Nullable RelDistribution getDistribution() {
    return RelDistributions.ANY;
  }

  @Override public @Nullable List<ImmutableBitSet> getKeys() {
    // Spools typically don't have keys
    return ImmutableList.of();
  }

  @Override public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
    // Temporary tables don't have referential constraints
    return ImmutableList.of();
  }

  @Override public @Nullable List<RelCollation> getCollationList() {
    // Could be extended to preserve collations from the input
    return ImmutableList.of();
  }

  @Override public boolean isKey(ImmutableBitSet columns) {
    return false;
  }

  @Override public @Nullable Expression getExpression(Class clazz) {
    // Return null so EnumerableTableScanRule won't try to convert spool table scans
    // Spool table scans are handled within the spool operator itself
    return null;
  }

  @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    throw new UnsupportedOperationException("SpoolRelOptTable.extend should not be called");
  }

  @Override public List<ColumnStrategy> getColumnStrategies() {
    return Collections.emptyList();
  }

  @Override public <C> @Nullable C unwrap(Class<C> aClass) {
    if (aClass.isInstance(table)) {
      return aClass.cast(table);
    }
    return null;
  }
}
