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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;

/** Planner rules relating to the Arrow adapter. */
public class ArrowRules {
  private ArrowRules() {}

  /** Rule that matches a {@link org.apache.calcite.rel.core.Project} on
   * an {@link ArrowTableScan} and pushes down projects if possible. */
  public static final ArrowProjectRule PROJECT_SCAN =
      ArrowProjectRule.DEFAULT_CONFIG.toRule(ArrowProjectRule.class);

  public static final ArrowFilterRule FILTER_SCAN =
      ArrowFilterRule.Config.DEFAULT.toRule();

  public static final ConverterRule TO_ENUMERABLE =
      ArrowToEnumerableConverterRule.DEFAULT_CONFIG
          .toRule(ArrowToEnumerableConverterRule.class);

  public static final List<RelOptRule> RULES = ImmutableList.of(PROJECT_SCAN, FILTER_SCAN);

  static List<String> arrowFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Base class for planner rules that convert a relational expression to
   * the Arrow calling convention. */
  abstract static class ArrowConverterRule extends ConverterRule {
    ArrowConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Filter} to an
   * {@link ArrowFilter}.
   */
  public static class ArrowFilterRule extends RelRule<ArrowFilterRule.Config> {

    /** Creates an ArrowFilterRule. */
    protected ArrowFilterRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);

      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter);
        call.transformTo(converted);
      }
    }

    RelNode convert(Filter filter) {
      final RelTraitSet traitSet =
          filter.getTraitSet().replace(ArrowRel.CONVENTION);
      return new ArrowFilter(filter.getCluster(), traitSet,
          convert(filter.getInput(), ArrowRel.CONVENTION),
          filter.getCondition());
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
      Config DEFAULT = ImmutableConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class).oneInput(b1 ->
                  b1.operand(ArrowTableScan.class).noInputs()))
          .build();

      @Override default ArrowFilterRule toRule() {
        return new ArrowFilterRule(this);
      }
    }
  }

  /**
   * Planner rule that projects from an {@link ArrowTableScan} just the columns
   * needed to satisfy a projection. If the projection's expressions are
   * trivial, the projection is removed.
   *
   * @see ArrowRules#PROJECT_SCAN
   */
  public static class ArrowProjectRule extends ArrowConverterRule {

    /** Default configuration. */
    protected static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            ArrowRel.CONVENTION, "ArrowProjectRule")
        .withRuleFactory(ArrowProjectRule::new);

    /** Creates an ArrowProjectRule. */
    protected ArrowProjectRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Project project = (Project) rel;
      @Nullable List<Integer> fields =
          ArrowProject.getProjectFields(project.getProjects());
      if (fields == null) {
        // Project contains expressions more complex than just field references.
        return null;
      }
      final RelTraitSet traitSet =
          project.getTraitSet().replace(ArrowRel.CONVENTION);
      return new ArrowProject(project.getCluster(), traitSet,
          convert(project.getInput(), ArrowRel.CONVENTION),
          project.getProjects(), project.getRowType());
    }
  }

  /**
   * Rule to convert a relational expression from
   * {@link ArrowRel#CONVENTION} to {@link EnumerableConvention}.
   */
  static class ArrowToEnumerableConverterRule extends ConverterRule {

    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(RelNode.class, ArrowRel.CONVENTION,
            EnumerableConvention.INSTANCE, "ArrowToEnumerableConverterRule")
        .withRuleFactory(ArrowToEnumerableConverterRule::new);

    /** Creates an ArrowToEnumerableConverterRule. */
    protected ArrowToEnumerableConverterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
      return new ArrowToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
  }
}
