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

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Planner rule that converts a {@link Project}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * of a {@link org.apache.calcite.schema.ProjectableFilterableTable}
 * to a {@link org.apache.calcite.interpreter.Bindables.BindableTableScan}.
 *
 * <p>The {@link #INTERPRETER} variant allows an intervening
 * {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see FilterTableScanRule
 */
public abstract class ProjectTableScanRule extends RelOptRule {
  public static final Predicate<TableScan> PREDICATE =
      new PredicateImpl<TableScan>() {
        public boolean test(TableScan scan) {
          // We can only push projects into a ProjectableFilterableTable.
          final RelOptTable table = scan.getTable();
          return table.unwrap(ProjectableFilterableTable.class) != null;
        }
      };

  /** Rule that matches Project on TableScan. */
  public static final ProjectTableScanRule INSTANCE =
      new ProjectTableScanRule(
          operand(Project.class,
              operand(TableScan.class, null, PREDICATE, none())),
          RelFactories.LOGICAL_BUILDER,
          "ProjectScanRule") {
        @Override public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          final TableScan scan = call.rel(1);
          apply(call, project, scan);
        }
      };

  /** Rule that matches Project on EnumerableInterpreter on TableScan. */
  public static final ProjectTableScanRule INTERPRETER =
      new ProjectTableScanRule(
          operand(Project.class,
              operand(EnumerableInterpreter.class,
                  operand(TableScan.class, null, PREDICATE, none()))),
          RelFactories.LOGICAL_BUILDER,
          "ProjectScanRule:interpreter") {
        @Override public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          final TableScan scan = call.rel(2);
          apply(call, project, scan);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /** Creates a ProjectTableScanRule. */
  public ProjectTableScanRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    super(operand, relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  protected void apply(RelOptRuleCall call, Project project, TableScan scan) {
    final RelOptTable table = scan.getTable();
    assert table.unwrap(ProjectableFilterableTable.class) != null;

    final Mappings.TargetMapping mapping = project.getMapping();
    if (mapping == null
        || Mappings.isIdentity(mapping)) {
      return;
    }

    final ImmutableIntList projects;
    final ImmutableList<RexNode> filters;
    if (scan instanceof Bindables.BindableTableScan) {
      final Bindables.BindableTableScan bindableScan =
          (Bindables.BindableTableScan) scan;
      filters = bindableScan.filters;
      projects = bindableScan.projects;
    } else {
      filters = ImmutableList.of();
      projects = scan.identity();
    }

    final List<Integer> projects2 =
        Mappings.apply((Mapping) mapping, projects);
    call.transformTo(
        Bindables.BindableTableScan.create(scan.getCluster(), scan.getTable(),
            filters, projects2));
  }
}

// End ProjectTableScanRule.java
