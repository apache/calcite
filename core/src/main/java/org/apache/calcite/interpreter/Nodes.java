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
package org.apache.calcite.interpreter;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.rules.FilterTableRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

/**
 * Helper methods for {@link Node} and implementations for core relational
 * expressions.
 */
public class Nodes {
  /** Extension to
   * {@link org.apache.calcite.interpreter.Interpreter.Compiler}
   * that knows how to handle the core logical
   * {@link org.apache.calcite.rel.RelNode}s. */
  public static class CoreCompiler extends Interpreter.Compiler {
    CoreCompiler(Interpreter interpreter) {
      super(interpreter);
    }

    public void rewrite(Project project) {
      RelNode input = project.getInput();
      final Mappings.TargetMapping mapping = project.getMapping();
      if (mapping == null) {
        return;
      }
      RexNode condition;
      if (input instanceof Filter) {
        final Filter filter = (Filter) input;
        condition = filter.getCondition();
        input = filter.getInput();
      } else {
        condition = project.getCluster().getRexBuilder().makeLiteral(true);
      }
      if (input instanceof TableScan) {
        final TableScan scan = (TableScan) input;
        final RelOptTable table = scan.getTable();
        final ProjectableFilterableTable projectableFilterableTable =
            table.unwrap(ProjectableFilterableTable.class);
        if (projectableFilterableTable != null) {
          final FilterTableRule.FilterSplit filterSplit =
              FilterTableRule.FilterSplit.of(projectableFilterableTable,
                  condition, interpreter.getDataContext());
          rel = new FilterScan(project.getCluster(), project.getTraitSet(),
              table, filterSplit.acceptedFilters,
              ImmutableIntList.copyOf(Mappings.asList(mapping.inverse())));
          rel = RelOptUtil.createFilter(rel, filterSplit.rejectedFilters);
        }
      }
    }

    public void rewrite(Filter filter) {
      if (filter.getInput() instanceof TableScan) {
        final TableScan scan = (TableScan) filter.getInput();
        final RelOptTable table = scan.getTable();
        final ProjectableFilterableTable projectableFilterableTable =
            table.unwrap(ProjectableFilterableTable.class);
        if (projectableFilterableTable != null) {
          final FilterTableRule.FilterSplit filterSplit =
              FilterTableRule.FilterSplit.of(projectableFilterableTable,
                  filter.getCondition(),
                  interpreter.getDataContext());
          if (!filterSplit.acceptedFilters.isEmpty()) {
            rel = new FilterScan(scan.getCluster(), scan.getTraitSet(),
                table, filterSplit.acceptedFilters, null);
            rel = RelOptUtil.createFilter(rel, filterSplit.rejectedFilters);
            return;
          }
        }
        final FilterableTable filterableTable =
            table.unwrap(FilterableTable.class);
        if (filterableTable != null) {
          final FilterTableRule.FilterSplit filterSplit =
              FilterTableRule.FilterSplit.of(filterableTable,
                  filter.getCondition(),
                  interpreter.getDataContext());
          if (!filterSplit.acceptedFilters.isEmpty()) {
            rel = new FilterScan(scan.getCluster(), scan.getTraitSet(),
                table, filterSplit.acceptedFilters, null);
            rel = RelOptUtil.createFilter(rel, filterSplit.rejectedFilters);
          }
        }
      }
    }

    public void visit(Aggregate agg) {
      node = new AggregateNode(interpreter, agg);
    }

    public void visit(Filter filter) {
      node = new FilterNode(interpreter, filter);
    }

    public void visit(Project project) {
      node = new ProjectNode(interpreter, project);
    }

    /** Per {@link #rewrite(RelNode)}, writes to {@link #rel}.
     *
     * <p>We don't handle {@link org.apache.calcite.rel.core.Calc} directly.
     * Expand to a {@link org.apache.calcite.rel.core.Project}
     * on {@link org.apache.calcite.rel.core.Filter} (or just a
     * {@link org.apache.calcite.rel.core.Project}). */
    public void rewrite(Calc calc) {
      final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
          calc.getProgram().split();
      rel = calc.getInput();
      rel = RelOptUtil.createFilter(rel, projectFilter.right);
      rel = RelOptUtil.createProject(rel, projectFilter.left,
          calc.getRowType().getFieldNames());
    }

    public void visit(Values value) {
      node = new ValuesNode(interpreter, value);
    }

    public void visit(TableScan scan) {
      node = new TableScanNode(interpreter, scan, ImmutableList.<RexNode>of(),
          null);
    }

    public void visit(FilterScan scan) {
      node = new TableScanNode(interpreter, scan, scan.filters, scan.projects);
    }

    public void visit(Sort sort) {
      node = new SortNode(interpreter, sort);
    }

    public void visit(Union union) {
      node = new UnionNode(interpreter, union);
    }

    public void visit(Join join) {
      node = new JoinNode(interpreter, join);
    }

    public void visit(Window window) {
      node = new WindowNode(interpreter, window);
    }
  }

  /** Table scan that applies filters and optionally projects. Only used in an
   * interpreter. */
  public static class FilterScan extends TableScan {
    private final ImmutableList<RexNode> filters;
    private final ImmutableIntList projects;

    protected FilterScan(RelOptCluster cluster, RelTraitSet traits,
        RelOptTable table, ImmutableList<RexNode> filters,
        ImmutableIntList projects) {
      super(cluster, traits, table);
      this.filters = filters;
      this.projects = projects;
    }
  }
}

// End Nodes.java
