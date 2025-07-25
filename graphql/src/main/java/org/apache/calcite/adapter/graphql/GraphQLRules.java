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
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import java.util.*;

/**
 * Utility class containing rules for converting relational algebra expressions to GraphQL operators.
 */
public class GraphQLRules {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLRules.class);

  private GraphQLRules() {}

  // Helper method to check if a RexNode is a constant or dynamic parameter
  private static boolean isConstantOrDynamicParam(RexNode node) {
    return node instanceof RexLiteral || node instanceof RexDynamicParam;
  }

  public static final ConverterRule TO_ENUMERABLE =
      ConverterRule.Config.INSTANCE
          .withConversion(RelNode.class, GraphQLRel.CONVENTION,
              EnumerableConvention.INSTANCE, "GraphQLToEnumerableConverterRule")
          .withRuleFactory(GraphQLToEnumerableConverterRule::new)
          .toRule(GraphQLToEnumerableConverterRule.class);

  public static final GraphQLProjectRule PROJECT_RULE =
      GraphQLProjectRule.DEFAULT_CONFIG.toRule(GraphQLProjectRule.class);

  public static final GraphQLSortRule SORT_RULE =
      GraphQLSortRule.DEFAULT_CONFIG.toRule(GraphQLSortRule.class);

  public static final GraphQLFilterRule FILTER_RULE =
      GraphQLFilterRule.DEFAULT_CONFIG.toRule(GraphQLFilterRule.class);

  // Add a rule for splitting projects containing calculated fields
  @SuppressWarnings("deprecation")
  public static final RelOptRule PROJECT_SPLIT_RULE =
      new GraphQLProjectSplitRule(
          RelOptRule.operand(LogicalProject.class, RelOptRule.any()));

  // Update RULES list to include filter rule
  public static final List<RelOptRule> RULES =
      ImmutableList.of(PROJECT_SPLIT_RULE,
      PROJECT_RULE,
      SORT_RULE,
      FILTER_RULE);

  static List<String> graphQLFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  abstract static class GraphQLConverterRule extends ConverterRule {
    GraphQLConverterRule(Config config) {
      super(config);
    }
  }

  /** Rule to split projects with calculations into a GraphQL fetch followed by calculation */
  public static class GraphQLProjectSplitRule extends RelOptRule {

    protected GraphQLProjectSplitRule(RelOptRuleOperand operand) {
      super(operand, "GraphQLProjectSplitRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      LOGGER.debug("GraphQLProjectSplitRule.matches checking project: {}", project);

      LOGGER.debug("Project row type: {}", project.getRowType());
      LOGGER.debug("Project expressions: {}", project.getProjects());

      for (RexNode expr : project.getProjects()) {
        if (!isSimpleFieldReference(expr) && !isConstantOrDynamicParam(expr)) {
          LOGGER.debug("Found computed expression: {}", expr);
          return true;
        }
      }
      return false;
    }

    private boolean isSimpleFieldReference(RexNode expr) {
      boolean isRef = expr instanceof RexInputRef;
      LOGGER.debug("Checking if expression is simple reference: {} = {}", expr, isRef);
      return isRef;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      RelNode input = project.getInput();

      LOGGER.debug("Processing project split for: {}", project);
      LOGGER.debug("Input relation: {}", input);
      LOGGER.debug("Input row type: {}", input.getRowType());

      List<RexNode> baseProjects = new ArrayList<>();
      List<RexNode> calcProjects = new ArrayList<>();
      List<String> baseFieldNames = new ArrayList<>();
      List<String> calcFieldNames = new ArrayList<>();

      for (int i = 0; i < project.getProjects().size(); i++) {
        RexNode expr = project.getProjects().get(i);
        String fieldName = project.getRowType().getFieldNames().get(i);

        LOGGER.debug("Processing expression {} with field name {}", expr, fieldName);
        LOGGER.debug("Expression type: {}", expr.getType());

        if (isSimpleFieldReference(expr) || isConstantOrDynamicParam(expr)) {
          baseProjects.add(expr);
          baseFieldNames.add(fieldName);
          calcProjects.add(new RexInputRef(baseProjects.size() - 1, expr.getType()));
          calcFieldNames.add(fieldName);
          LOGGER.debug("Added to base projects and calc projects as reference: {} as {}", expr, fieldName);
        } else {
          baseProjects.add(expr);
          baseFieldNames.add(fieldName);
          calcProjects.add(expr);
          calcFieldNames.add(fieldName);
          LOGGER.debug("Added to calc projects: {} as {}", expr, fieldName);
        }
      }

      @SuppressWarnings("deprecation")
      RelNode baseProject =
          LogicalProject.create(input,
          Collections.emptyList(),
          baseProjects,
          baseFieldNames);

      LOGGER.debug("Created base project: {}", baseProject);
      LOGGER.debug("Base project row type: {}", baseProject.getRowType());

      @SuppressWarnings("deprecation")
      RelNode calcProject =
          LogicalProject.create(baseProject,
          Collections.emptyList(),
          calcProjects,
          calcFieldNames);

      LOGGER.debug("Created calc project: {}", calcProject);
      LOGGER.debug("Calc project row type: {}", calcProject.getRowType());

      LOGGER.debug("Final row types comparison:");
      LOGGER.debug("Original project: {}", project.getRowType());
      LOGGER.debug("Split result: {}", calcProject.getRowType());

      call.transformTo(calcProject);
    }
  }

  /** Rule to convert a {@link LogicalProject} to a {@link GraphQLProject}. */
  public static class GraphQLProjectRule extends GraphQLConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            GraphQLRel.CONVENTION, "GraphQLProjectRule")
        .withRuleFactory(GraphQLProjectRule::new);

    protected GraphQLProjectRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Project project = call.rel(0);
      return project.getConvention() == Convention.NONE
          && !(project instanceof GraphQLProject)
          && project.getProjects().stream().allMatch(this::isSimpleFieldReference);
    }

    private boolean isSimpleFieldReference(RexNode expr) {
      return expr.isA(org.apache.calcite.sql.SqlKind.INPUT_REF);
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new GraphQLProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }

  /** Rule to convert a {@link LogicalSort} to a {@link GraphQLSort}. */
  public static class GraphQLSortRule extends GraphQLConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalSort.class, Convention.NONE,
            GraphQLRel.CONVENTION, "GraphQLSortRule")
        .withRuleFactory(GraphQLSortRule::new);

    protected GraphQLSortRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Sort sort = call.rel(0);
      LOGGER.debug("GraphQLSortRule matches called - Sort: {}, Traits: {}", sort, sort.getTraitSet());

      if (sort.getConvention() != Convention.NONE || sort instanceof GraphQLSort) {
        return false;
      }

      // Updated to accept both literals and dynamic parameters
      if (sort.offset != null && !isConstantOrDynamicParam(sort.offset)) {
        LOGGER.debug("Skipping sort with non-constant/non-param offset: {}", sort.offset);
        return false;
      }

      if (sort.fetch != null && !isConstantOrDynamicParam(sort.fetch)) {
        LOGGER.debug("Skipping sort with non-constant/non-param fetch: {}", sort.fetch);
        return false;
      }

      return true;
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalSort sort = (LogicalSort) rel;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);

      return new GraphQLSort(
          sort.getCluster(),
          traitSet,
          convert(sort.getInput(), out),
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    }
  }

  /** Rule to convert a {@link LogicalFilter} to a {@link GraphQLFilter}. */
  public static class GraphQLFilterRule extends GraphQLConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalFilter.class, Convention.NONE,
            GraphQLRel.CONVENTION, "GraphQLFilterRule")
        .withRuleFactory(GraphQLFilterRule::new);

    protected GraphQLFilterRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      LOGGER.debug("GraphQLFilterRule matches called - Filter: {}, Traits: {}", filter, filter.getTraitSet());

      // Only match if it's not already a GraphQL filter and has the right convention
      if (filter.getConvention() != Convention.NONE || filter instanceof GraphQLFilter) {
        return false;
      }

      // Check if the filter condition is simple enough to push down
      return isSimpleFilter(filter.getCondition());
    }

    private boolean isSimpleFilter(RexNode condition) {
      LOGGER.debug("Checking if filter is simple: {}", condition);

      // Handle *AND* and *OR* conditions recursively
      if (condition.isA(SqlKind.AND) || condition.isA(SqlKind.OR)) {
        List<RexNode> operands = ((RexCall) condition).getOperands();
        return operands.stream().allMatch(this::isSimpleFilter);
      }

      // Handle NOT conditions
      if (condition.isA(SqlKind.NOT)) {
        RexCall call = (RexCall) condition;
        RexNode operand = call.getOperands().get(0);
        // Only allow NOT on simple conditions
        return isSimpleFilter(operand);
      }

      // Handle IS NULL and IS NOT NULL
      if (condition.isA(SqlKind.IS_NULL) || condition.isA(SqlKind.IS_NOT_NULL)) {
        RexCall call = (RexCall) condition;
        RexNode operand = call.getOperands().get(0);
        // Only push down if operating on a column reference
        boolean isSimple = operand instanceof RexInputRef;
        LOGGER.debug("Checking IS NULL/IS NOT NULL - Operand: {}, IsSimple: {}",
            operand, isSimple);
        return isSimple;
      }

      // Handle BETWEEN
      if (condition.isA(SqlKind.BETWEEN)) {
        RexCall call = (RexCall) condition;
        RexNode value = call.getOperands().get(0);  // The value being tested
        RexNode lower = call.getOperands().get(1);  // Lower bound
        RexNode upper = call.getOperands().get(2);  // Upper bound

        // Allow both literals and dynamic parameters for bounds
        boolean isSimple = value instanceof RexInputRef
            && (isConstantOrDynamicParam(lower))
            && (isConstantOrDynamicParam(upper));

        LOGGER.debug("Checking BETWEEN - Value: {}, Lower: {}, Upper: {}, IsSimple: {}",
            value, lower, upper, isSimple);

        return isSimple;
      }

      // For basic comparison operations
      if (condition.isA(SqlKind.EQUALS) ||
          condition.isA(SqlKind.LESS_THAN) ||
          condition.isA(SqlKind.LESS_THAN_OR_EQUAL) ||
          condition.isA(SqlKind.GREATER_THAN) ||
          condition.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {

        RexCall call = (RexCall) condition;
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        // Allow both column reference on left and constant/param on right
        // OR constant/param on left and column reference on right
        boolean isSimple = (left instanceof RexInputRef && isConstantOrDynamicParam(right)) ||
            (isConstantOrDynamicParam(left) && right instanceof RexInputRef);

        LOGGER.debug("Checking comparison - Left: {}, Right: {}, IsSimple: {}",
            left, right, isSimple);

        return isSimple;
      }

      LOGGER.debug("Filter is not a supported operation type: {}", condition.getKind());
      return false;
    }

    private boolean isConstantOrDynamicParam(RexNode node) {
      return node instanceof RexLiteral || node instanceof RexDynamicParam;
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);

      // Handle both *AND* and *OR* conditions
      if (filter.getCondition().isA(SqlKind.AND) || filter.getCondition().isA(SqlKind.OR)) {
        RexCall condition = (RexCall) filter.getCondition();
        List<RexNode> allConditions = condition.getOperands();
        List<RexNode> pushDownConditions = new ArrayList<>();
        List<RexNode> remainingConditions = new ArrayList<>();

        for (RexNode subCondition : allConditions) {
          if (isSimpleFilter(subCondition)) {
            pushDownConditions.add(subCondition);
          } else {
            remainingConditions.add(subCondition);
          }
        }

        // Create pushed down filter
        RelNode converted = convert(filter.getInput(), out);
        if (!pushDownConditions.isEmpty()) {
          RexNode pushDownCondition;
          if (condition.isA(SqlKind.AND)) {
            pushDownCondition =
                RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pushDownConditions);
          } else {
            pushDownCondition =
                RexUtil.composeDisjunction(filter.getCluster().getRexBuilder(), pushDownConditions);
          }
          converted =
              new GraphQLFilter(filter.getCluster(),
              traitSet,
              converted,
              pushDownCondition);
        }

        // Add remaining filters as a regular filter
        if (!remainingConditions.isEmpty()) {
          RexNode remainingCondition;
          if (condition.isA(SqlKind.AND)) {
            remainingCondition =
                RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), remainingConditions);
          } else {
            remainingCondition =
                RexUtil.composeDisjunction(filter.getCluster().getRexBuilder(), remainingConditions);
          }
          converted = LogicalFilter.create(converted, remainingCondition);
        }

        return converted;
      }

      // For simple single conditions
      return new GraphQLFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), out),
          filter.getCondition());
    }
  }

  private static class GraphQLToEnumerableConverterRule extends ConverterRule {

    protected GraphQLToEnumerableConverterRule(Config config) {
      super(config);
      LOGGER.debug("Created GraphQLToEnumerableConverterRule with config: {}", config);
    }

    @Override public RelNode convert(RelNode rel) {
      LOGGER.debug("Converting RelNode to Enumerable: {}", rel);
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
      LOGGER.debug("Created new trait set: {}", newTraitSet);
      return new GraphQLToEnumerableConverter(
          rel.getCluster(),
          newTraitSet,
          rel);
    }
  }
}
