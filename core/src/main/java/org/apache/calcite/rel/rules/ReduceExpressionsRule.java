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
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 *
 * @param <C> Configuration type
 */
public abstract class ReduceExpressionsRule<C extends ReduceExpressionsRule.Config>
    extends RelRule<C>
    implements SubstitutionRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Regular expression that matches the description of all instances of this
   * rule and {@link ValuesReduceRule} also. Use
   * it to prevent the planner from invoking these rules.
   */
  public static final Pattern EXCLUSION_PATTERN =
      Pattern.compile("Reduce(Expressions|Values)Rule.*");

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Filter}.
   * If the condition is a constant, the filter is removed (if TRUE) or replaced with
   * an empty {@link org.apache.calcite.rel.core.Values} (if FALSE or NULL).
   *
   * @see CoreRules#FILTER_REDUCE_EXPRESSIONS
   */
  public static class FilterReduceExpressionsRule
      extends ReduceExpressionsRule<FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig> {
    /** Creates a FilterReduceExpressionsRule. */
    protected FilterReduceExpressionsRule(FilterReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public FilterReduceExpressionsRule(Class<? extends Filter> filterClass,
        RelBuilderFactory relBuilderFactory) {
      this(FilterReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(FilterReduceExpressionsRuleConfig.class)
          .withOperandFor(filterClass)
          .withMatchNullability(true)
          .as(FilterReduceExpressionsRuleConfig.class));
    }

    @Deprecated // to be removed before 2.0
    public FilterReduceExpressionsRule(Class<? extends Filter> filterClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(FilterReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(FilterReduceExpressionsRuleConfig.class)
          .withOperandFor(filterClass)
          .withMatchNullability(matchNullability)
          .as(FilterReduceExpressionsRuleConfig.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> expList =
          Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;
      boolean reduced;
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(filter.getInput());
      if (reduceExpressions(filter, expList, predicates, true,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {
        assert expList.size() == 1;
        newConditionExp = expList.get(0);
        reduced = true;
      } else {
        // No reduction, but let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        newConditionExp = filter.getCondition();
        reduced = false;
      }

      // Even if no reduction, let's still test the original
      // predicate to see if it was already a constant,
      // in which case we don't need any runtime decision
      // about filtering.
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
            filter.getInput());
      } else if (newConditionExp instanceof RexLiteral
          || RexUtil.isNullLiteral(newConditionExp, true)) {
        call.transformTo(createEmptyRelOrEquivalent(call, filter));
      } else if (reduced) {
        call.transformTo(call.builder()
            .push(filter.getInput())
            .filter(newConditionExp).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          boolean reverse = newConditionExp.getKind() == SqlKind.NOT;
          if (reverse) {
            newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, newConditionExp, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().prune(filter);
    }

    /**
     * For static schema systems, a filter that is always false or null can be
     * replaced by a values operator that produces no rows, as the schema
     * information can just be taken from the input Rel. In dynamic schema
     * environments, the filter might have an unknown input type, in these cases
     * they must define a system specific alternative to a Values operator, such
     * as inserting a limit 0 instead of a filter on top of the original input.
     *
     * <p>The default implementation of this method is to call
     * {@link RelBuilder#empty}, which for the static schema will be optimized
     * to an empty
     * {@link org.apache.calcite.rel.core.Values}.
     *
     * @param input rel to replace, assumes caller has already determined
     *              equivalence to Values operation for 0 records or a
     *              false filter.
     * @return equivalent but less expensive replacement rel
     */
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Filter input) {
      return call.builder().push(input).empty().build();
    }

    private void reduceNotNullableFilter(
        RelOptRuleCall call,
        Filter filter,
        RexNode rexNode,
        boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexNode.getKind()) {
      case IS_NULL:
      case IS_UNKNOWN:
        alwaysTrue = false;
        break;
      case IS_NOT_NULL:
        alwaysTrue = true;
        break;
      default:
        return;
      }
      if (reverse) {
        alwaysTrue = !alwaysTrue;
      }
      RexNode operand = ((RexCall) rexNode).getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
          // New plan is absolutely better than old plan.
          call.getPlanner().prune(filter);
        }
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface FilterReduceExpressionsRuleConfig extends ReduceExpressionsRule.Config {
      FilterReduceExpressionsRuleConfig DEFAULT = ImmutableFilterReduceExpressionsRuleConfig.of()
          .withMatchNullability(true)
          .withOperandFor(LogicalFilter.class)
          .withDescription("ReduceExpressionsRule(Filter)")
          .as(FilterReduceExpressionsRuleConfig.class);

      @Override default FilterReduceExpressionsRule toRule() {
        return new FilterReduceExpressionsRule(this);
      }
    }
  }

  /** Rule that reduces constants inside a
   * {@link org.apache.calcite.rel.core.Project}.
   *
   * @see CoreRules#PROJECT_REDUCE_EXPRESSIONS */
  public static class ProjectReduceExpressionsRule
      extends ReduceExpressionsRule<
      ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig> {
    /** Creates a ProjectReduceExpressionsRule. */
    protected ProjectReduceExpressionsRule(ProjectReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public ProjectReduceExpressionsRule(Class<? extends Project> projectClass,
        RelBuilderFactory relBuilderFactory) {
      this(ProjectReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(ProjectReduceExpressionsRuleConfig.class)
          .withOperandFor(projectClass)
          .as(ProjectReduceExpressionsRuleConfig.class));
    }

    @Deprecated // to be removed before 2.0
    public ProjectReduceExpressionsRule(Class<? extends Project> projectClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(ProjectReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(ProjectReduceExpressionsRuleConfig.class)
          .withOperandFor(projectClass)
          .withMatchNullability(matchNullability)
          .as(ProjectReduceExpressionsRuleConfig.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList =
          Lists.newArrayList(project.getProjects());
      if (reduceExpressions(project, expList, predicates, false,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {
        assert !project.getProjects().equals(expList)
            : "Reduced expressions should be different from original expressions";
        call.transformTo(
            call.builder()
                .push(project.getInput())
                .project(expList, project.getRowType().getFieldNames())
                .build());

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(project);
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface ProjectReduceExpressionsRuleConfig extends ReduceExpressionsRule.Config {
      ProjectReduceExpressionsRuleConfig DEFAULT = ImmutableProjectReduceExpressionsRuleConfig.of()
          .withMatchNullability(true)
          .withOperandFor(LogicalProject.class)
          .withDescription("ReduceExpressionsRule(Project)")
          .as(ProjectReduceExpressionsRuleConfig.class);

      @Override default ProjectReduceExpressionsRule toRule() {
        return new ProjectReduceExpressionsRule(this);
      }
    }
  }

  /** Rule that reduces constants inside a {@link Join}.
   *
   * @see CoreRules#JOIN_REDUCE_EXPRESSIONS */
  public static class JoinReduceExpressionsRule
      extends ReduceExpressionsRule<JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig> {
    /** Creates a JoinReduceExpressionsRule. */
    protected JoinReduceExpressionsRule(JoinReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        RelBuilderFactory relBuilderFactory) {
      this(JoinReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(JoinReduceExpressionsRuleConfig.class)
          .withOperandFor(joinClass)
          .withMatchNullability(true)
          .as(JoinReduceExpressionsRuleConfig.class));
    }

    @Deprecated // to be removed before 2.0
    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(JoinReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(JoinReduceExpressionsRuleConfig.class)
          .withOperandFor(joinClass)
          .withMatchNullability(matchNullability)
          .as(JoinReduceExpressionsRuleConfig.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final List<RexNode> expList = Lists.newArrayList(join.getCondition());
      final int fieldCount = join.getLeft().getRowType().getFieldCount();
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList leftPredicates =
          mq.getPulledUpPredicates(join.getLeft());
      final RelOptPredicateList rightPredicates =
          mq.getPulledUpPredicates(join.getRight());
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RelOptPredicateList predicates =
          leftPredicates.union(rexBuilder,
              rightPredicates.shift(rexBuilder, fieldCount));
      if (!reduceExpressions(join, expList, predicates, true,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {
        return;
      }
      call.transformTo(
          join.copy(
              join.getTraitSet(),
              expList.get(0),
              join.getLeft(),
              join.getRight(),
              join.getJoinType(),
              join.isSemiJoinDone()));

      // New plan is absolutely better than old plan.
      call.getPlanner().prune(join);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface JoinReduceExpressionsRuleConfig extends ReduceExpressionsRule.Config {
      JoinReduceExpressionsRuleConfig DEFAULT = ImmutableJoinReduceExpressionsRuleConfig.of()
          .withMatchNullability(false)
          .withOperandFor(Join.class)
          .withDescription("ReduceExpressionsRule(Join)")
          .as(JoinReduceExpressionsRuleConfig.class);

      @Override default JoinReduceExpressionsRule toRule() {
        return new JoinReduceExpressionsRule(this);
      }
    }
  }

  /**
   * Rule that reduces constants inside a {@link Calc}.
   *
   * @see CoreRules#CALC_REDUCE_EXPRESSIONS
   */
  public static class CalcReduceExpressionsRule
      extends ReduceExpressionsRule<CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig> {
    /** Creates a CalcReduceExpressionsRule. */
    protected CalcReduceExpressionsRule(CalcReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public CalcReduceExpressionsRule(Class<? extends Calc> calcClass,
        RelBuilderFactory relBuilderFactory) {
      this(CalcReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(CalcReduceExpressionsRuleConfig.class)
          .withOperandFor(calcClass)
          .withMatchNullability(true)
          .as(CalcReduceExpressionsRuleConfig.class));
    }

    @Deprecated // to be removed before 2.0
    public CalcReduceExpressionsRule(Class<? extends Calc> calcClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(CalcReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(CalcReduceExpressionsRuleConfig.class)
          .withOperandFor(calcClass)
          .withMatchNullability(matchNullability)
          .as(CalcReduceExpressionsRuleConfig.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Calc calc = call.rel(0);
      RexProgram program = calc.getProgram();
      final List<RexNode> exprList = program.getExprList();

      // Form a list of expressions with sub-expressions fully expanded.
      final List<RexNode> expandedExprList = new ArrayList<>();
      final RexShuttle shuttle =
          new RexShuttle() {
            @Override public RexNode visitLocalRef(RexLocalRef localRef) {
              return expandedExprList.get(localRef.getIndex());
            }
          };
      for (RexNode expr : exprList) {
        expandedExprList.add(expr.accept(shuttle));
      }
      final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
      if (reduceExpressions(calc, expandedExprList, predicates, false,
          config.matchNullability(), config.treatDynamicCallsAsConstant())) {
        final RexProgramBuilder builder =
            new RexProgramBuilder(
                calc.getInput().getRowType(),
                calc.getCluster().getRexBuilder());
        final List<RexLocalRef> list = new ArrayList<>();
        for (RexNode expr : expandedExprList) {
          list.add(builder.registerInput(expr));
        }
        if (program.getCondition() != null) {
          final int conditionIndex =
              program.getCondition().getIndex();
          final RexNode newConditionExp =
              expandedExprList.get(conditionIndex);
          if (newConditionExp.isAlwaysTrue()) {
            // condition is always TRUE - drop it.
          } else if (newConditionExp instanceof RexLiteral
              || RexUtil.isNullLiteral(newConditionExp, true)) {
            // condition is always NULL or FALSE - replace calc
            // with empty.
            call.transformTo(createEmptyRelOrEquivalent(call, calc));
            return;
          } else {
            builder.addCondition(list.get(conditionIndex));
          }
        }
        int k = 0;
        for (RexLocalRef projectExpr : program.getProjectList()) {
          final int index = projectExpr.getIndex();
          builder.addProject(
              list.get(index).getIndex(),
              program.getOutputRowType().getFieldNames().get(k++));
        }
        call.transformTo(
            calc.copy(calc.getTraitSet(), calc.getInput(), builder.getProgram()));

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(calc);
      }
    }

    /**
     * For static schema systems, a filter that is always false or null can be
     * replaced by a values operator that produces no rows, as the schema
     * information can just be taken from the input Rel. In dynamic schema
     * environments, the filter might have an unknown input type, in these cases
     * they must define a system specific alternative to a Values operator, such
     * as inserting a limit 0 instead of a filter on top of the original input.
     *
     * <p>The default implementation of this method is to call
     * {@link RelBuilder#empty}, which for the static schema will be optimized
     * to an Immutable.Config.of()
     * {@link org.apache.calcite.rel.core.Values}.
     *
     * @param input rel to replace, assumes caller has already determined
     *              equivalence to Values operation for 0 records or a
     *              false filter.
     * @return equivalent but less expensive replacement rel
     */
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Calc input) {
      return call.builder().push(input).empty().build();
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface CalcReduceExpressionsRuleConfig extends ReduceExpressionsRule.Config {
      CalcReduceExpressionsRuleConfig DEFAULT = ImmutableCalcReduceExpressionsRuleConfig.of()
          .withMatchNullability(true)
          .withOperandFor(LogicalCalc.class)
          .withDescription("ReduceExpressionsRule(Calc)")
          .as(CalcReduceExpressionsRuleConfig.class);

      @Override default CalcReduceExpressionsRule toRule() {
        return new CalcReduceExpressionsRule(this);
      }
    }
  }

  /** Rule that reduces constants inside a {@link Window}.
   *
   * @see CoreRules#WINDOW_REDUCE_EXPRESSIONS */
  public static class WindowReduceExpressionsRule
      extends ReduceExpressionsRule<WindowReduceExpressionsRule.WindowReduceExpressionsRuleConfig> {
    /** Creates a WindowReduceExpressionsRule. */
    protected WindowReduceExpressionsRule(WindowReduceExpressionsRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public WindowReduceExpressionsRule(Class<? extends Window> windowClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(WindowReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(WindowReduceExpressionsRuleConfig.class)
          .withOperandFor(windowClass)
          .withMatchNullability(matchNullability)
          .as(WindowReduceExpressionsRuleConfig.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalWindow window = call.rel(0);
      RexBuilder rexBuilder = window.getCluster().getRexBuilder();
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates = mq
          .getPulledUpPredicates(window.getInput());

      boolean reduced = false;
      final List<Window.Group> groups = new ArrayList<>();
      for (Window.Group group : window.groups) {
        List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
        for (Window.RexWinAggCall aggCall : group.aggCalls) {
          final List<RexNode> expList = new ArrayList<>(aggCall.getOperands());
          if (reduceExpressions(window, expList, predicates)) {
            aggCall = new Window.RexWinAggCall(
                (SqlAggFunction) aggCall.getOperator(), aggCall.type, expList,
                aggCall.ordinal, aggCall.distinct, aggCall.ignoreNulls);
            reduced = true;
          }
          aggCalls.add(aggCall);
        }

        final ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
        for (Integer key : group.keys) {
          if (!predicates.constantMap.containsKey(
              rexBuilder.makeInputRef(window.getInput(), key))) {
            keyBuilder.set(key);
          }
        }
        final ImmutableBitSet keys = keyBuilder.build();
        reduced |= keys.cardinality() != group.keys.cardinality();

        final List<RelFieldCollation> collationsList = group.orderKeys
            .getFieldCollations().stream()
            .filter(fc ->
                !predicates.constantMap.containsKey(
                    rexBuilder.makeInputRef(window.getInput(),
                        fc.getFieldIndex())))
            .collect(Collectors.toList());

        boolean collationReduced =
            group.orderKeys.getFieldCollations().size() != collationsList.size();
        reduced |= collationReduced;
        RelCollation relCollation = collationReduced
            ? RelCollations.of(collationsList)
            : group.orderKeys;
        groups.add(
            new Window.Group(keys, group.isRows, group.lowerBound,
                group.upperBound, relCollation, aggCalls));
      }
      if (reduced) {
        call.transformTo(LogicalWindow
            .create(window.getTraitSet(), window.getInput(),
                window.getConstants(), window.getRowType(), groups));
        call.getPlanner().prune(window);
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface WindowReduceExpressionsRuleConfig extends ReduceExpressionsRule.Config {
      WindowReduceExpressionsRuleConfig DEFAULT = ImmutableWindowReduceExpressionsRuleConfig.of()
          .withMatchNullability(true)
          .withOperandFor(LogicalWindow.class)
          .withDescription("ReduceExpressionsRule(Window)")
          .as(WindowReduceExpressionsRuleConfig.class);

      @Override default WindowReduceExpressionsRule toRule() {
        return new WindowReduceExpressionsRule(this);
      }
    }
  }

  //~ Constructors -----------------------------------------------------------

  /** Creates a ReduceExpressionsRule. */
  protected ReduceExpressionsRule(C config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Reduces a list of expressions.
   *
   * @param rel     Relational expression
   * @param expList List of expressions, modified in place
   * @param predicates Constraints known to hold on input expressions
   * @return whether reduction found something to change, and succeeded
   */
  protected static boolean reduceExpressions(RelNode rel, List<RexNode> expList,
      RelOptPredicateList predicates) {
    return reduceExpressions(rel, expList, predicates, false, true, false);
  }

  @Deprecated // to be removed before 2.0
  protected static boolean reduceExpressions(RelNode rel, List<RexNode> expList,
      RelOptPredicateList predicates, boolean unknownAsFalse) {
    return reduceExpressions(rel, expList, predicates, unknownAsFalse, true, false);
  }

  /**
   * Reduces a list of expressions.
   *
   * <p>The {@code matchNullability} flag comes into play when reducing a
   * expression whose type is nullable. Suppose we are reducing an expression
   * {@code CASE WHEN 'a' = 'a' THEN 1 ELSE NULL END}. Before reduction the
   * type is {@code INTEGER} (nullable), but after reduction the literal 1 has
   * type {@code INTEGER NOT NULL}.
   *
   * <p>In some situations it is more important to preserve types; in this
   * case you should use {@code matchNullability = true} (which used to be
   * the default behavior of this method), and it will cast the literal to
   * {@code INTEGER} (nullable).
   *
   * <p>In other situations, you would rather propagate the new stronger type,
   * because it may allow further optimizations later; pass
   * {@code matchNullability = false} and no cast will be added, but you may
   * need to adjust types elsewhere in the expression tree.
   *
   * @param rel     Relational expression
   * @param expList List of expressions, modified in place
   * @param predicates Constraints known to hold on input expressions
   * @param unknownAsFalse Whether UNKNOWN will be treated as FALSE
   * @param matchNullability Whether Calcite should add a CAST to a literal
   *                         resulting from simplification and expression if the
   *                         expression had nullable type and the literal is
   *                         NOT NULL
   * @param treatDynamicCallsAsConstant Whether to treat dynamic functions as
   *                                    constants
   *
   * @return whether reduction found something to change, and succeeded
   */
  protected static boolean reduceExpressions(RelNode rel, List<RexNode> expList,
      RelOptPredicateList predicates, boolean unknownAsFalse,
      boolean matchNullability, boolean treatDynamicCallsAsConstant) {
    final RelOptCluster cluster = rel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final List<RexNode> originExpList = Lists.newArrayList(expList);
    final RexExecutor executor =
        Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, predicates, executor);

    // Simplify predicates in place
    final RexUnknownAs unknownAs = RexUnknownAs.falseIf(unknownAsFalse);
    final boolean reduced = reduceExpressionsInternal(rel, simplify, unknownAs,
        expList, predicates, treatDynamicCallsAsConstant);

    boolean simplified = false;
    for (int i = 0; i < expList.size(); i++) {
      final RexNode expr2 =
          simplify.simplifyPreservingType(expList.get(i), unknownAs,
              matchNullability);
      if (!expr2.equals(expList.get(i))) {
        expList.set(i, expr2);
        simplified = true;
      }
    }

    if (reduced && simplified) {
      return !originExpList.equals(expList);
    }

    return reduced || simplified;
  }

  protected static boolean reduceExpressionsInternal(RelNode rel,
      RexSimplify simplify, RexUnknownAs unknownAs, List<RexNode> expList,
      RelOptPredicateList predicates, boolean treatDynamicCallsAsConstant) {
    // Replace predicates on CASE to CASE on predicates.
    boolean changed = new CaseShuttle().mutate(expList);

    // Find reducible expressions.
    final List<RexNode> constExps = new ArrayList<>();
    List<Boolean> addCasts = new ArrayList<>();
    findReducibleExps(rel.getCluster().getTypeFactory(), expList,
        predicates.constantMap, constExps, addCasts, treatDynamicCallsAsConstant);
    if (constExps.isEmpty()) {
      return changed;
    }

    final List<RexNode> constExps2 = Lists.newArrayList(constExps);
    if (!predicates.constantMap.isEmpty()) {
      final List<Map.Entry<RexNode, RexNode>> pairs =
          Lists.newArrayList(predicates.constantMap.entrySet());
      RexReplacer replacer =
          new RexReplacer(simplify, unknownAs, Pair.left(pairs),
              Pair.right(pairs), Collections.nCopies(pairs.size(), false));
      replacer.mutate(constExps2);
    }

    // Compute the values they reduce to.
    RexExecutor executor = rel.getCluster().getPlanner().getExecutor();
    if (executor == null) {
      // Cannot reduce expressions: caller has not set an executor in their
      // environment. Caller should execute something like the following before
      // invoking the planner:
      //
      // final RexExecutorImpl executor =
      //   new RexExecutorImpl(Schemas.createDataContext(null));
      // rootRel.getCluster().getPlanner().setExecutor(executor);
      return changed;
    }

    final List<RexNode> reducedValues = new ArrayList<>();
    executor.reduce(simplify.rexBuilder, constExps2, reducedValues);

    // Use RexNode.digest to judge whether each newly generated RexNode
    // is equivalent to the original one.
    if (RexUtil.strings(constExps).equals(RexUtil.strings(reducedValues))) {
      return changed;
    }

    // For Project, we have to be sure to preserve the result
    // types, so always cast regardless of the expression type.
    // For other RelNodes like Filter, in general, this isn't necessary,
    // and the presence of casts could hinder other rules such as sarg
    // analysis, which require bare literals.  But there are special cases,
    // like when the expression is a UDR argument, that need to be
    // handled as special cases.
    if (rel instanceof Project) {
      addCasts = Collections.nCopies(reducedValues.size(), true);
    }

    new RexReplacer(simplify, unknownAs, constExps, reducedValues, addCasts)
        .mutate(expList);
    return true;
  }

  /**
   * Locates expressions that can be reduced to literals or converted to
   * expressions with redundant casts removed.
   *
   * @param typeFactory    Type factory
   * @param exps           list of candidate expressions to be examined for
   *                       reduction
   * @param constants      List of expressions known to be constant
   * @param constExps      returns the list of expressions that can be constant
   *                       reduced
   * @param addCasts       indicator for each expression that can be constant
   *                       reduced, whether a cast of the resulting reduced
   *                       expression is potentially necessary
   * @param treatDynamicCallsAsConstant Whether to treat dynamic functions as
   *                                    constants
   */
  protected static void findReducibleExps(RelDataTypeFactory typeFactory,
      List<RexNode> exps, ImmutableMap<RexNode, RexNode> constants,
      List<RexNode> constExps, List<Boolean> addCasts, boolean treatDynamicCallsAsConstant) {
    ReducibleExprLocator gardener =
        new ReducibleExprLocator(typeFactory, constants, constExps,
            addCasts, treatDynamicCallsAsConstant);
    for (RexNode exp : exps) {
      gardener.analyze(exp);
    }
    assert constExps.size() == addCasts.size();
  }

  /** Creates a map containing each (e, constant) pair that occurs within
   * a predicate list.
   *
   * @param clazz Class of expression that is considered constant
   * @param rexBuilder Rex builder
   * @param predicates Predicate list
   * @param <C> what to consider a constant: {@link RexLiteral} to use a narrow
   *           definition of constant, or {@link RexNode} to use
   *           {@link RexUtil#isConstant(RexNode)}
   * @return Map from values to constants
   *
   * @deprecated Use {@link RelOptPredicateList#constantMap}
   */
  @Deprecated // to be removed before 2.0
  public static <C extends RexNode> ImmutableMap<RexNode, C> predicateConstants(
      Class<C> clazz, RexBuilder rexBuilder, RelOptPredicateList predicates) {
    return RexUtil.predicateConstants(clazz, rexBuilder,
        predicates.pulledUpPredicates);
  }

  /** Pushes predicates into a CASE.
   *
   * <p>We have a loose definition of 'predicate': any boolean expression will
   * do, except CASE. For example '(CASE ...) = 5' or '(CASE ...) IS NULL'.
   */
  public static RexCall pushPredicateIntoCase(RexCall call) {
    if (call.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
      return call;
    }
    switch (call.getKind()) {
    case CASE:
    case AND:
    case OR:
      return call; // don't push CASE into CASE!
    case EQUALS: {
      // checks that the EQUALS operands may be split and
      // doesn't push EQUALS into CASE
      List<RexNode> equalsOperands = call.getOperands();
      ImmutableBitSet left = RelOptUtil.InputFinder.bits(equalsOperands.get(0));
      ImmutableBitSet right = RelOptUtil.InputFinder.bits(equalsOperands.get(1));
      if (!left.isEmpty() && !right.isEmpty() && left.intersect(right).isEmpty()) {
        return call;
      }
      break;
    }
    default:
      break;
    }
    int caseOrdinal = -1;
    final List<RexNode> operands = call.getOperands();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (operand.getKind() == SqlKind.CASE) {
        caseOrdinal = i;
      }
    }
    if (caseOrdinal < 0) {
      return call;
    }
    // Convert
    //   f(CASE WHEN p1 THEN v1 ... END, arg)
    // to
    //   CASE WHEN p1 THEN f(v1, arg) ... END
    final RexCall case_ = (RexCall) operands.get(caseOrdinal);
    final List<RexNode> nodes = new ArrayList<>();
    for (int i = 0; i < case_.getOperands().size(); i++) {
      RexNode node = case_.getOperands().get(i);
      if (!RexUtil.isCasePredicate(case_, i)) {
        node = substitute(call, caseOrdinal, node);
      }
      nodes.add(node);
    }
    return case_.clone(call.getType(), nodes);
  }

  /** Converts op(arg0, ..., argOrdinal, ..., argN) to op(arg0,..., node, ..., argN). */
  protected static RexNode substitute(RexCall call, int ordinal, RexNode node) {
    final List<RexNode> newOperands = Lists.newArrayList(call.getOperands());
    newOperands.set(ordinal, node);
    return call.clone(call.getType(), newOperands);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Replaces expressions with their reductions. Note that we only have to
   * look for RexCall, since nothing else is reducible in the first place.
   */
  protected static class RexReplacer extends RexShuttle {
    private final RexSimplify simplify;
    private final List<RexNode> reducibleExps;
    private final List<RexNode> reducedValues;
    private final List<Boolean> addCasts;

    RexReplacer(
        RexSimplify simplify,
        RexUnknownAs unknownAs,
        List<RexNode> reducibleExps,
        List<RexNode> reducedValues,
        List<Boolean> addCasts) {
      this.simplify = simplify;
      this.reducibleExps = reducibleExps;
      this.reducedValues = reducedValues;
      this.addCasts = addCasts;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      RexNode node = visit(inputRef);
      if (node == null) {
        return super.visitInputRef(inputRef);
      }
      return node;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexNode node = visit(call);
      if (node != null) {
        return node;
      }
      node = super.visitCall(call);
      return node;
    }

    private @Nullable RexNode visit(final RexNode call) {
      int i = reducibleExps.indexOf(call);
      if (i == -1) {
        return null;
      }
      RexNode replacement = reducedValues.get(i);
      if (addCasts.get(i)
          && (replacement.getType() != call.getType())) {
        // Handle change from nullable to NOT NULL by claiming
        // that the result is still nullable, even though
        // we know it isn't.
        //
        // Also, we cannot reduce CAST('abc' AS VARCHAR(4)) to 'abc'.
        // If we make 'abc' of type VARCHAR(4), we may later encounter
        // the same expression in a Project's digest where it has
        // type VARCHAR(3), and that's wrong.
        replacement =
            simplify.rexBuilder.makeAbstractCast(call.getType(), replacement);
      }
      return replacement;
    }
  }

  /**
   * Helper class used to locate expressions that either can be reduced to
   * literals or contain redundant casts.
   */
  protected static class ReducibleExprLocator extends RexVisitorImpl<Void> {
    /** Whether an expression is constant, and if so, whether it can be
     * reduced to a simpler constant. */
    enum Constancy {
      NON_CONSTANT, REDUCIBLE_CONSTANT, IRREDUCIBLE_CONSTANT
    }

    private final boolean treatDynamicCallsAsConstant;

    private final List<Constancy> stack = new ArrayList<>();

    private final ImmutableMap<RexNode, RexNode> constants;

    private final List<RexNode> constExprs;

    private final List<Boolean> addCasts;

    private final Deque<SqlOperator> parentCallTypeStack = new ArrayDeque<>();

    ReducibleExprLocator(RelDataTypeFactory typeFactory,
        ImmutableMap<RexNode, RexNode> constants, List<RexNode> constExprs,
        List<Boolean> addCasts, boolean treatDynamicCallsAsConstant) {
      // go deep
      super(true);
      this.constants = constants;
      this.constExprs = constExprs;
      this.addCasts = addCasts;
      this.treatDynamicCallsAsConstant = treatDynamicCallsAsConstant;
    }

    public void analyze(RexNode exp) {
      assert stack.isEmpty();

      exp.accept(this);

      // Deal with top of stack
      assert stack.size() == 1;
      assert parentCallTypeStack.isEmpty();
      Constancy rootConstancy = stack.get(0);
      if (rootConstancy == Constancy.REDUCIBLE_CONSTANT) {
        // The entire subtree was constant, so add it to the result.
        addResult(exp);
      }
      stack.clear();
    }

    private Void pushVariable() {
      stack.add(Constancy.NON_CONSTANT);
      return null;
    }

    private void addResult(RexNode exp) {
      // Cast of literal can't be reduced, so skip those (otherwise we'd
      // go into an infinite loop as we add them back).
      if (exp.getKind() == SqlKind.CAST) {
        RexCall cast = (RexCall) exp;
        RexNode operand = cast.getOperands().get(0);
        if (operand instanceof RexLiteral) {
          return;
        }
      }
      constExprs.add(exp);

      // In the case where the expression corresponds to a UDR argument,
      // we need to preserve casts.  Note that this only applies to
      // the topmost argument, not expressions nested within the UDR
      // call.
      //
      // REVIEW zfong 6/13/08 - Are there other expressions where we
      // also need to preserve casts?
      SqlOperator op = parentCallTypeStack.peek();
      if (op == null) {
        addCasts.add(false);
      } else {
        addCasts.add(isUdf(op));
      }
    }

    private static Boolean isUdf(@SuppressWarnings("unused") SqlOperator operator) {
      // return operator instanceof UserDefinedRoutine
      return false;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      final RexNode constant = constants.get(inputRef);
      if (constant != null) {
        if (constant instanceof RexCall || constant instanceof RexDynamicParam) {
          constant.accept(this);
        } else {
          stack.add(Constancy.REDUCIBLE_CONSTANT);
        }
        return null;
      }
      return pushVariable();
    }

    @Override public Void visitLiteral(RexLiteral literal) {
      stack.add(Constancy.IRREDUCIBLE_CONSTANT);
      return null;
    }

    @Override public Void visitOver(RexOver over) {
      // assume non-constant (running SUM(1) looks constant but isn't)
      analyzeCall(over, Constancy.NON_CONSTANT);
      return null;
    }

    @Override public Void visitCorrelVariable(RexCorrelVariable variable) {
      return pushVariable();
    }

    @Override public Void visitCall(RexCall call) {
      // assume REDUCIBLE_CONSTANT until proven otherwise
      analyzeCall(call, Constancy.REDUCIBLE_CONSTANT);
      return null;
    }

    @Override public Void visitSubQuery(RexSubQuery subQuery) {
      analyzeCall(subQuery, Constancy.REDUCIBLE_CONSTANT);
      return null;
    }

    private void analyzeCall(RexCall call, Constancy callConstancy) {
      parentCallTypeStack.push(call.getOperator());

      // visit operands, pushing their states onto stack
      super.visitCall(call);

      // look for NON_CONSTANT operands
      int operandCount = call.getOperands().size();
      List<Constancy> operandStack = Util.last(stack, operandCount);
      for (Constancy operandConstancy : operandStack) {
        if (operandConstancy == Constancy.NON_CONSTANT) {
          callConstancy = Constancy.NON_CONSTANT;
          break;
        }
      }

      // Even if all operands are constant, the call itself may
      // be non-deterministic.
      if (!call.getOperator().isDeterministic()) {
        callConstancy = Constancy.NON_CONSTANT;
      } else if (!treatDynamicCallsAsConstant
          && call.getOperator().isDynamicFunction()) {
        // In some circumstances, we should avoid caching the plan if we have dynamic functions.
        // If desired, treat this situation the same as a non-deterministic function.
        callConstancy = Constancy.NON_CONSTANT;
      }

      // Row operator itself can't be reduced to a literal, but if
      // the operands are constants, we still want to reduce those
      if ((callConstancy == Constancy.REDUCIBLE_CONSTANT)
          && (call.getOperator() instanceof SqlRowOperator)) {
        callConstancy = Constancy.NON_CONSTANT;
      }

      if (callConstancy == Constancy.NON_CONSTANT) {
        // any REDUCIBLE_CONSTANT children are now known to be maximal
        // reducible subtrees, so they can be added to the result
        // list
        for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
          Constancy constancy = operandStack.get(iOperand);
          if (constancy == Constancy.REDUCIBLE_CONSTANT) {
            addResult(call.getOperands().get(iOperand));
          }
        }
      }

      // pop operands off of the stack
      operandStack.clear();

      // pop this parent call operator off the stack
      parentCallTypeStack.pop();

      // push constancy result for this call onto stack
      stack.add(callConstancy);
    }

    @Override public Void visitDynamicParam(RexDynamicParam dynamicParam) {
      return pushVariable();
    }

    @Override public Void visitRangeRef(RexRangeRef rangeRef) {
      return pushVariable();
    }

    @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      return pushVariable();
    }
  }

  /** Shuttle that pushes predicates into a CASE. */
  protected static class CaseShuttle extends RexShuttle {
    @Override public RexNode visitCall(RexCall call) {
      for (;;) {
        call = (RexCall) super.visitCall(call);
        final RexCall old = call;
        call = pushPredicateIntoCase(call);
        if (call == old) {
          return call;
        }
      }
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    @Override ReduceExpressionsRule<?> toRule();

    /** Whether to add a CAST when a nullable expression
     * reduces to a NOT NULL literal. */
    @Value.Default default boolean matchNullability() {
      return false;
    }

    /** Sets {@link #matchNullability()}. */
    Config withMatchNullability(boolean matchNullability);

    /** Whether to treat
     * {@link SqlOperator#isDynamicFunction() dynamic functions} as constants.
     *
     * <p>When false (the default), calls to dynamic functions (e.g.
     * {@code USER}) are not reduced. When true, calls to dynamic functions
     * are treated as a constant, and reduced. */
    @Value.Default default boolean treatDynamicCallsAsConstant() {
      return false;
    }

    /** Sets {@link #treatDynamicCallsAsConstant()}. */
    Config withTreatDynamicCallsAsConstant(boolean treatDynamicCallsAsConstant);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends RelNode> relClass) {
      return withOperandSupplier(b -> b.operand(relClass).anyInputs())
          .as(Config.class);
    }
  }
}
