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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;


/**
 * To represent a table provided by Pig script.
 */
public class PigDummyTable implements ScannableTable {
  private RelDataType rowType;

  private PigDummyTable(RelDataType rowType) {
    this.rowType = rowType;
  }

  /**
   * Creates a dummy @{@link RelOptTable} for a table provided from Pig script.
   * For that type of tables, we only care about its schema for our Pig-To-Rel
   * translation process.
   *
   * @param rowType Relational schema of Pig table
   * @param names Names of Pig table
   */
  public static RelOptTable createDummyRelOptTable(RelOptSchema optSchema, RelDataType rowType,
      List<String> names) {
    return RelOptTableImpl.create(
        optSchema,
        rowType,
        names,
        new PigDummyTable(rowType), // Dummy Table, required for PigRelPlanner to work
        Expressions.constant(Boolean.TRUE));
  }

  public static final Statistic DUMMY_STATISTICS =
      new Statistic() {
        public Double getRowCount() {
          // Default rowcount of 10, required for PigRelPlanner to work.
          return 10.0;
        }

        public boolean isKey(ImmutableBitSet columns) {
          return false;
        }

        public List<RelReferentialConstraint> getReferentialConstraints() {
          return ImmutableList.of();
        }

        public List<RelCollation> getCollations() {
          return ImmutableList.of();
        }

        public RelDistribution getDistribution() {
          return RelDistributionTraitDef.INSTANCE.getDefault();
        }
      };

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  public Statistic getStatistic() {
    return DUMMY_STATISTICS;
  }

  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override public boolean isRolledUp(String column) {
    return false;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
      SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return null;
  }
}

// End PigDummyTable.java
