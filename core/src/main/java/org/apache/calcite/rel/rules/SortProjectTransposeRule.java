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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.immutables.value.Value;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Sort}
 * past a {@link org.apache.calcite.rel.core.Project}.
 *
 * @see CoreRules#SORT_PROJECT_TRANSPOSE
 */
@Value.Enclosing
public class SortProjectTransposeRule
    extends RelRule<SortProjectTransposeRule.Config>
    implements TransformationRule {

  /** Creates a SortProjectTransposeRule. */
  protected SortProjectTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass) {
    this(Config.DEFAULT.withOperandFor(sortClass, projectClass));
  }

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass,
      String description) {
    this(Config.DEFAULT.withDescription(description)
        .as(Config.class)
        .withOperandFor(sortClass, projectClass));
  }

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .as(Config.class)
        .withOperandFor(sortClass, projectClass));
  }

  @Deprecated // to be removed before 2.0
  protected SortProjectTransposeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  protected SortProjectTransposeRule(RelOptRuleOperand operand) {
    this(Config.DEFAULT
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Project project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutationIgnoreCast(
            project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
      final RexNode node = project.getProjects().get(fc.getFieldIndex());
      if (node.isA(SqlKind.CAST)) {
        // Check whether it is a monotonic preserving cast, otherwise we cannot push
        final RexCall cast = (RexCall) node;
        RelFieldCollation newFc = requireNonNull(RexUtil.apply(map, fc));
        final RexCallBinding binding =
            RexCallBinding.create(cluster.getTypeFactory(), cast,
                ImmutableList.of(RelCollations.of(newFc)));
        if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
          return;
        }
      }
    }
    final RelCollation newCollation =
        cluster.traitSet().canonize(
            RexUtil.apply(map, sort.getCollation()));
    final Sort newSort =
        sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getInput(),
            newCollation,
            sort.offset,
            sort.fetch);
    RelNode newProject =
        project.copy(
            sort.getTraitSet(),
            ImmutableList.of(newSort));
    // Not only is newProject equivalent to sort;
    // newSort is equivalent to project's input
    // (but only if the sort is not also applying an offset/limit).
    Map<RelNode, RelNode> equiv;
    if (sort.offset == null
        && sort.fetch == null
        && cluster.getPlanner().getRelTraitDefs()
            .contains(RelCollationTraitDef.INSTANCE)) {
      equiv = ImmutableMap.of(newSort, project.getInput());
    } else {
      equiv = ImmutableMap.of();
    }
    call.transformTo(newProject, equiv);
  }
  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortProjectTransposeRule.Config.of()
        .withOperandFor(Sort.class, LogicalProject.class);

    @Override default SortProjectTransposeRule toRule() {
      return new SortProjectTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Sort> sortClass,
        Class<? extends Project> projectClass) {
      return withOperandSupplier(b0 ->
          b0.operand(sortClass).oneInput(b1 ->
              b1.operand(projectClass)
                  .predicate(p -> !p.containsOver()).anyInputs()))
          .as(Config.class);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Sort> sortClass,
        Class<? extends Project> projectClass,
        Class<? extends RelNode> inputClass) {
      return withOperandSupplier(b0 ->
          b0.operand(sortClass).oneInput(b1 ->
              b1.operand(projectClass)
                  .predicate(p -> !p.containsOver())
                  .oneInput(b2 ->
                      b2.operand(inputClass).anyInputs())))
          .as(Config.class);
    }
  }
}
