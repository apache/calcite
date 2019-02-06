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
package org.apache.calcite.schema.impl;

import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Virtual table that is composed of two or more tables joined together.
 *
 * <p>Star tables do not occur in end-user queries. They are introduced by the
 * optimizer to help matching queries to materializations, and used only
 * during the planning process.</p>
 *
 * <p>When a materialization is defined, if it involves a join, it is converted
 * to a query on top of a star table. Queries that are candidates to map onto
 * the materialization are mapped onto the same star table.</p>
 */
public class StarTable extends AbstractTable implements TranslatableTable {
  public final Lattice lattice;

  // TODO: we'll also need a list of join conditions between tables. For now
  //  we assume that join conditions match
  public final ImmutableList<Table> tables;

  /** Number of fields in each table's row type. */
  public ImmutableIntList fieldCounts;

  /** Creates a StarTable. */
  private StarTable(Lattice lattice, ImmutableList<Table> tables) {
    this.lattice = Objects.requireNonNull(lattice);
    this.tables = tables;
  }

  /** Creates a StarTable and registers it in a schema. */
  public static StarTable of(Lattice lattice, List<Table> tables) {
    return new StarTable(lattice, ImmutableList.copyOf(tables));
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.STAR;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final List<RelDataType> typeList = new ArrayList<>();
    final List<Integer> fieldCounts = new ArrayList<>();
    for (Table table : tables) {
      final RelDataType rowType = table.getRowType(typeFactory);
      typeList.addAll(RelOptUtil.getFieldTypeList(rowType));
      fieldCounts.add(rowType.getFieldCount());
    }
    // Compute fieldCounts the first time this method is called. Safe to assume
    // that the field counts will be the same whichever type factory is used.
    if (this.fieldCounts == null) {
      this.fieldCounts = ImmutableIntList.copyOf(fieldCounts);
    }
    return typeFactory.createStructType(typeList, lattice.uniqueColumnNames());
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    // Create a table scan of infinite cost.
    return new StarTableScan(context.getCluster(), table);
  }

  public StarTable add(Table table) {
    return of(lattice,
        ImmutableList.<Table>builder().addAll(tables).add(table).build());
  }

  /** Returns the column offset of the first column of {@code table} in this
   * star table's output row type.
   *
   * @param table Table
   * @return Column offset
   * @throws IllegalArgumentException if table is not in this star
   */
  public int columnOffset(Table table) {
    int n = 0;
    for (Pair<Table, Integer> pair : Pair.zip(tables, fieldCounts)) {
      if (pair.left == table) {
        return n;
      }
      n += pair.right;
    }
    throw new IllegalArgumentException("star table " + this
        + " does not contain table " + table);
  }

  /** Relational expression that scans a {@link StarTable}.
   *
   * <p>It has infinite cost.
   */
  public static class StarTableScan extends TableScan {
    public StarTableScan(RelOptCluster cluster, RelOptTable relOptTable) {
      super(cluster, cluster.traitSetOf(Convention.NONE), relOptTable);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }
  }
}

// End StarTable.java
