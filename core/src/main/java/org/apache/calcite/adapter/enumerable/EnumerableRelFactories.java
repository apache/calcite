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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.base.Preconditions;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
public class EnumerableRelFactories {

  public static final org.apache.calcite.rel.core.RelFactories.TableScanFactory
      ENUMERABLE_TABLE_SCAN_FACTORY = new TableScanFactoryImpl();

  public static final org.apache.calcite.rel.core.RelFactories.ProjectFactory
      ENUMERABLE_PROJECT_FACTORY = new ProjectFactoryImpl();

  public static final org.apache.calcite.rel.core.RelFactories.FilterFactory
      ENUMERABLE_FILTER_FACTORY = new FilterFactoryImpl();

  public static final org.apache.calcite.rel.core.RelFactories.SortFactory
      ENUMERABLE_SORT_FACTORY = new SortFactoryImpl();

  /**
   * Implementation of {@link org.apache.calcite.rel.core.RelFactories.TableScanFactory} that
   * returns a vanilla {@link EnumerableTableScan}.
   */
  private static class TableScanFactoryImpl
      implements org.apache.calcite.rel.core.RelFactories.TableScanFactory {
    @Override public RelNode createScan(RelOptTable.ToRelContext toRelContext, RelOptTable table) {
      return EnumerableTableScan.create(toRelContext.getCluster(), table);
    }
  }

  /**
   * Implementation of {@link org.apache.calcite.rel.core.RelFactories.ProjectFactory} that
   * returns a vanilla {@link EnumerableProject}.
   */
  private static class ProjectFactoryImpl
      implements org.apache.calcite.rel.core.RelFactories.ProjectFactory {
    @Override public RelNode createProject(RelNode input, List<RelHint> hints,
        List<? extends RexNode> childExprs,
        @Nullable List<? extends @Nullable String> fieldNames,
        Set<CorrelationId> variablesSet) {
      Preconditions.checkArgument(variablesSet.isEmpty(),
          "EnumerableProject does not allow variables");
      final RelDataType rowType =
          RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs,
              fieldNames, SqlValidatorUtil.F_SUGGESTER);
      return EnumerableProject.create(input, childExprs, rowType);
    }
  }

  /**
   * Implementation of {@link org.apache.calcite.rel.core.RelFactories.FilterFactory} that
   * returns a vanilla {@link EnumerableFilter}.
   */
  private static class FilterFactoryImpl
      implements org.apache.calcite.rel.core.RelFactories.FilterFactory {
    @Override public RelNode createFilter(RelNode input, RexNode condition,
                         Set<CorrelationId> variablesSet) {
      return EnumerableFilter.create(input, condition);
    }
  }

  /**
   * Implementation of {@link org.apache.calcite.rel.core.RelFactories.SortFactory} that
   * returns a vanilla {@link EnumerableSort}.
   */
  private static class SortFactoryImpl
      implements org.apache.calcite.rel.core.RelFactories.SortFactory {
    @Override public RelNode createSort(RelNode input, RelCollation collation,
        @Nullable RexNode offset, @Nullable RexNode fetch) {
      return EnumerableSort.create(input, collation, offset, fetch);
    }
  }

  private EnumerableRelFactories() {
  }
}
