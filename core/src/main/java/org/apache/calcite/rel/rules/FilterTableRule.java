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
package org.apache.calcite.rel.rules;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Planner rule that pushes a filter into a scan of a {@link FilterableTable}
 * or {@link org.apache.calcite.schema.ProjectableFilterableTable}.
 */
public class FilterTableRule extends RelOptRule {
  private static final Predicate<TableScan> PREDICATE =
      new Predicate<TableScan>() {
        public boolean apply(TableScan scan) {
          // We can only push filters into a FilterableTable or
          // ProjectableFilterableTable.
          final RelOptTable table = scan.getTable();
          return table.unwrap(FilterableTable.class) != null
              || table.unwrap(ProjectableFilterableTable.class) != null;
        }
      };

  public static final FilterTableRule INSTANCE = new FilterTableRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterTableRule. */
  private FilterTableRule() {
    super(
        operand(Filter.class,
            operand(EnumerableInterpreter.class,
                operand(TableScan.class, null, PREDICATE, none()))));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final EnumerableInterpreter interpreter = call.rel(1);
    final TableScan scan = call.rel(2);
    final FilterableTable filterableTable =
        scan.getTable().unwrap(FilterableTable.class);
    final ProjectableFilterableTable projectableFilterableTable =
        scan.getTable().unwrap(ProjectableFilterableTable.class);

    final FilterSplit filterSplit;
    if (filterableTable != null) {
      filterSplit = FilterSplit.of(filterableTable, filter.getCondition(),
          null);
    } else if (projectableFilterableTable != null) {
      filterSplit = FilterSplit.of(projectableFilterableTable,
          filter.getCondition(), null);
    } else {
      throw new AssertionError(scan.getTable());
    }

    // It's worth using the ProjectableFilterableTable interface even if it
    // refused all filters.
    final RelNode newFilter =
        RelOptUtil.createFilter(interpreter.getInput(),
            filterSplit.acceptedFilters, EnumerableRel.FILTER_FACTORY);
    final RelNode newInterpreter =
        new EnumerableInterpreter(interpreter.getCluster(),
            interpreter.getTraitSet(), newFilter, 0.15d);
    final RelNode residue =
        RelOptUtil.createFilter(newInterpreter, filterSplit.rejectedFilters);
    call.transformTo(residue);
  }

  /** Splits a filter condition into parts that can and cannot be
   * handled by a {@link FilterableTable} or
   * {@link ProjectableFilterableTable}. */
  public static class FilterSplit {
    public final ImmutableList<RexNode> acceptedFilters;
    public final ImmutableList<RexNode> rejectedFilters;

    public FilterSplit(ImmutableList<RexNode> acceptedFilters,
        ImmutableList<RexNode> rejectedFilters) {
      this.acceptedFilters = acceptedFilters;
      this.rejectedFilters = rejectedFilters;
    }

    public static FilterSplit of(FilterableTable table,
        RexNode condition, DataContext dataContext) {
      final List<RexNode> filters = Lists.newArrayList();
      RelOptUtil.decomposeConjunction(condition, filters);
      final List<RexNode> originalFilters = ImmutableList.copyOf(filters);

      final Enumerable<Object[]> enumerable =
          table.scan(dataContext, filters);
      return rest(originalFilters, filters, enumerable);
    }

    public static FilterSplit of(ProjectableFilterableTable table,
        RexNode condition, DataContext dataContext) {
      final List<RexNode> filters = Lists.newArrayList();
      RelOptUtil.decomposeConjunction(condition, filters);
      final List<RexNode> originalFilters = ImmutableList.copyOf(filters);

      final Enumerable<Object[]> enumerable =
          table.scan(dataContext, filters, null);
      return rest(originalFilters, filters, enumerable);
    }

    private static FilterSplit rest(List<RexNode> originalFilters,
        List<RexNode> filters,
        Enumerable<Object[]> enumerable) {
      if (enumerable == null) {
        throw RESOURCE.filterableTableScanReturnedNull().ex();
      }
      final ImmutableList.Builder<RexNode> accepted = ImmutableList.builder();
      final ImmutableList.Builder<RexNode> rejected = ImmutableList.builder();
      for (RexNode originalFilter : originalFilters) {
        if (filters.contains(originalFilter)) {
          rejected.add(originalFilter);
        } else {
          accepted.add(originalFilter);
        }
      }
      for (RexNode node : filters) {
        if (!originalFilters.contains(node)) {
          throw RESOURCE.filterableTableInventedFilter(node.toString()).ex();
        }
      }
      return new FilterSplit(accepted.build(), rejected.build());
    }
  }
}

// End FilterTableRule.java
