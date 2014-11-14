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
package net.hydromatic.optiq.impl.interpreter;

import net.hydromatic.optiq.FilterableTable;
import net.hydromatic.optiq.ProjectableFilterableTable;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.FilterTableRule;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.Pair;
import org.eigenbase.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

/**
 * Helper methods for {@link Node} and implementations for core relational
 * expressions.
 */
public class Nodes {
  /** Extension to
   * {@link net.hydromatic.optiq.impl.interpreter.Interpreter.Compiler}
   * that knows how to handle the core logical
   * {@link org.eigenbase.rel.RelNode}s. */
  public static class CoreCompiler extends Interpreter.Compiler {
    CoreCompiler(Interpreter interpreter) {
      super(interpreter);
    }

    public void rewrite(ProjectRelBase project) {
      RelNode input = project.getChild();
      final Mappings.TargetMapping mapping = project.getMapping();
      if (mapping == null) {
        return;
      }
      RexNode condition;
      if (input instanceof FilterRelBase) {
        final FilterRelBase filter = (FilterRelBase) input;
        condition = filter.getCondition();
        input = filter.getChild();
      } else {
        condition = project.getCluster().getRexBuilder().makeLiteral(true);
      }
      if (input instanceof TableAccessRelBase) {
        final TableAccessRelBase scan = (TableAccessRelBase) input;
        final RelOptTable table = scan.getTable();
        final ProjectableFilterableTable projectableFilterableTable =
            table.unwrap(ProjectableFilterableTable.class);
        if (projectableFilterableTable != null) {
          final FilterTableRule.FilterSplit filterSplit =
              FilterTableRule.FilterSplit.of(projectableFilterableTable,
                  condition, interpreter.getDataContext());
          rel = new FilterScanRel(project.getCluster(), project.getTraitSet(),
              table, filterSplit.acceptedFilters,
              ImmutableIntList.copyOf(Mappings.asList(mapping.inverse())));
          rel = RelOptUtil.createFilter(rel, filterSplit.rejectedFilters);
        }
      }
    }

    public void rewrite(FilterRelBase filter) {
      if (filter.getChild() instanceof TableAccessRelBase) {
        final TableAccessRelBase scan = (TableAccessRelBase) filter.getChild();
        final RelOptTable table = scan.getTable();
        final ProjectableFilterableTable projectableFilterableTable =
            table.unwrap(ProjectableFilterableTable.class);
        if (projectableFilterableTable != null) {
          final FilterTableRule.FilterSplit filterSplit =
              FilterTableRule.FilterSplit.of(projectableFilterableTable,
                  filter.getCondition(),
                  interpreter.getDataContext());
          if (!filterSplit.acceptedFilters.isEmpty()) {
            rel = new FilterScanRel(scan.getCluster(), scan.getTraitSet(),
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
            rel = new FilterScanRel(scan.getCluster(), scan.getTraitSet(),
                table, filterSplit.acceptedFilters, null);
            rel = RelOptUtil.createFilter(rel, filterSplit.rejectedFilters);
          }
        }
      }
    }

    public void visit(FilterRelBase filter) {
      node = new FilterNode(interpreter, filter);
    }

    public void visit(ProjectRelBase project) {
      node = new ProjectNode(interpreter, project);
    }

    /** Per {@link #rewrite(RelNode)}, writes to {@link #rel}.
     *
     * <p>We don't handle {@link CalcRelBase} directly. Expand to a
     * {@link ProjectRelBase} on {@link FilterRelBase} (or just a
     * {@link ProjectRelBase}). */
    public void rewrite(CalcRelBase calc) {
      final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
          calc.getProgram().split();
      rel = calc.getChild();
      rel = RelOptUtil.createFilter(rel, projectFilter.right);
      rel = RelOptUtil.createProject(rel, projectFilter.left,
          calc.getRowType().getFieldNames());
    }

    public void visit(ValuesRelBase value) {
      node = new ValuesNode(interpreter, value);
    }

    public void visit(TableAccessRelBase scan) {
      node = new ScanNode(interpreter, scan, ImmutableList.<RexNode>of(), null);
    }

    public void visit(FilterScanRel scan) {
      node = new ScanNode(interpreter, scan, scan.filters, scan.projects);
    }

    public void visit(SortRel sort) {
      node = new SortNode(interpreter, sort);
    }
  }

  /** Table scan that applies filters and optionally projects. Only used in an
   * interpreter. */
  public static class FilterScanRel extends TableAccessRelBase {
    private final ImmutableList<RexNode> filters;
    private final ImmutableIntList projects;

    protected FilterScanRel(RelOptCluster cluster, RelTraitSet traits,
        RelOptTable table, ImmutableList<RexNode> filters,
        ImmutableIntList projects) {
      super(cluster, traits, table);
      this.filters = filters;
      this.projects = projects;
    }
  }
}

// End Nodes.java
