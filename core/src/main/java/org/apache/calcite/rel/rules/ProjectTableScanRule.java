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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that converts a {@link Project}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * of a {@link org.apache.calcite.schema.ProjectableFilterableTable}
 * to a {@link org.apache.calcite.interpreter.Bindables.BindableTableScan}.
 *
 * <p>The {@link CoreRules#PROJECT_INTERPRETER_TABLE_SCAN} variant allows an
 * intervening
 * {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see FilterTableScanRule
 */
@Value.Enclosing
public class ProjectTableScanRule
    extends RelRule<ProjectTableScanRule.Config> {

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static final com.google.common.base.Predicate<TableScan> PREDICATE =
      ProjectTableScanRule::test;

  /** Creates a ProjectTableScanRule. */
  protected ProjectTableScanRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectTableScanRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  protected static boolean test(TableScan scan) {
    // We can only push projects into a ProjectableFilterableTable.
    final RelOptTable table = scan.getTable();
    return table.unwrap(ProjectableFilterableTable.class) != null;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final Project project = call.rel(0);
      final TableScan scan = call.rel(1);
      apply(call, project, scan);
    } else if (call.rels.length == 3) {
      // the variant with intervening EnumerableInterpreter
      final Project project = call.rel(0);
      final TableScan scan = call.rel(2);
      apply(call, project, scan);
    } else {
      throw new AssertionError();
    }
  }

  protected void apply(RelOptRuleCall call, Project project, TableScan scan) {
    final RelOptTable table = scan.getTable();
    requireNonNull(table.unwrap(ProjectableFilterableTable.class));

    final List<Integer> selectedColumns = new ArrayList<>();
    final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
      @Override public Void visitInputRef(RexInputRef inputRef) {
        if (!selectedColumns.contains(inputRef.getIndex())) {
          selectedColumns.add(inputRef.getIndex());
        }
        return null;
      }
    };
    visitor.visitEach(project.getProjects());

    final List<RexNode> filtersPushDown;
    final List<Integer> projectsPushDown;
    if (scan instanceof Bindables.BindableTableScan) {
      final Bindables.BindableTableScan bindableScan =
          (Bindables.BindableTableScan) scan;
      filtersPushDown = bindableScan.filters;
      projectsPushDown = selectedColumns.stream()
          .map(bindableScan.projects::get)
          .collect(Collectors.toList());
    } else {
      filtersPushDown = ImmutableList.of();
      projectsPushDown = selectedColumns;
    }
    Bindables.BindableTableScan newScan =
        Bindables.BindableTableScan.create(scan.getCluster(), scan.getTable(),
            filtersPushDown, projectsPushDown);
    Mapping mapping =
        Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
    final List<RexNode> newProjectRexNodes =
        RexUtil.apply(mapping, project.getProjects());

    if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
      call.transformTo(newScan);
    } else {
      call.transformTo(
          call.builder()
              .push(newScan)
              .project(newProjectRexNodes)
              .build());
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Project on TableScan. */
    Config DEFAULT = ImmutableProjectTableScanRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Project.class).oneInput(b1 ->
                b1.operand(TableScan.class)
                    .predicate(ProjectTableScanRule::test)
                    .noInputs()));

    /** Config that matches Project on EnumerableInterpreter on TableScan. */
    Config INTERPRETER = DEFAULT
        .withOperandSupplier(b0 ->
            b0.operand(Project.class).oneInput(b1 ->
                b1.operand(EnumerableInterpreter.class).oneInput(b2 ->
                    b2.operand(TableScan.class)
                        .predicate(ProjectTableScanRule::test)
                        .noInputs())))
        .withDescription("ProjectTableScanRule:interpreter")
        .as(Config.class);

    @Override default ProjectTableScanRule toRule() {
      return new ProjectTableScanRule(this);
    }
  }
}
