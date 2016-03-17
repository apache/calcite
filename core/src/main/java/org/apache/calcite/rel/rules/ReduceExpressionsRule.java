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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.EquiJoin;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
 */
public abstract class ReduceExpressionsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Regular expression that matches the description of all instances of this
   * rule and {@link ValuesReduceRule} also. Use
   * it to prevent the planner from invoking these rules.
   */
  public static final Pattern EXCLUSION_PATTERN =
      Pattern.compile("Reduce(Expressions|Values)Rule.*");

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.calcite.rel.logical.LogicalFilter}.
   */
  public static final ReduceExpressionsRule FILTER_INSTANCE =
      new FilterReduceExpressionsRule(LogicalFilter.class, RelFactories.LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public static final ReduceExpressionsRule PROJECT_INSTANCE =
      new ProjectReduceExpressionsRule(LogicalProject.class, RelFactories.LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.calcite.rel.core.Join}.
   */
  public static final ReduceExpressionsRule JOIN_INSTANCE =
      new JoinReduceExpressionsRule(Join.class, RelFactories.LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.calcite.rel.logical.LogicalCalc}.
   */
  public static final ReduceExpressionsRule CALC_INSTANCE =
      new CalcReduceExpressionsRule(LogicalCalc.class, RelFactories.LOGICAL_BUILDER);

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Filter}.
   * If the condition is a constant, the filter is removed (if TRUE) or replaced with
   * an empty {@link org.apache.calcite.rel.core.Values} (if FALSE or NULL).
   */
  public static class FilterReduceExpressionsRule extends ReduceExpressionsRule {

    public FilterReduceExpressionsRule(Class<? extends Filter> filterClass,
        RelBuilderFactory relBuilderFactory) {
      super(filterClass, relBuilderFactory, "ReduceExpressionsRule(Filter)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> expList =
          Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;
      boolean reduced;
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(filter.getInput());
      if (reduceExpressions(filter, expList, predicates, true)) {
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
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
            filter.getInput());
      } else if (newConditionExp instanceof RexLiteral
          || RexUtil.isNullLiteral(newConditionExp, true)) {
        call.transformTo(createEmptyRelOrEquivalent(call, filter));
      } else if (reduced) {
        call.transformTo(call.builder().
            push(filter.getInput()).filter(expList.get(0)).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          RexCall rexCall = (RexCall) newConditionExp;
          boolean reverse =
              rexCall.getOperator()
                  == SqlStdOperatorTable.NOT;
          if (reverse) {
            rexCall = (RexCall) rexCall.getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, rexCall, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().setImportance(filter, 0.0);
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
        RexCall rexCall,
        boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexCall.getKind()) {
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
      RexNode operand = rexCall.getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
        }
      }
    }
  }

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Project}.
   */
  public static class ProjectReduceExpressionsRule extends ReduceExpressionsRule {

    public ProjectReduceExpressionsRule(Class<? extends Project> projectClass,
        RelBuilderFactory relBuilderFactory) {
      super(projectClass, relBuilderFactory, "ReduceExpressionsRule(Project)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList =
          Lists.newArrayList(project.getProjects());
      if (reduceExpressions(project, expList, predicates)) {
        call.transformTo(
            call.builder()
                .push(project.getInput())
                .project(expList, project.getRowType().getFieldNames())
                .build());

        // New plan is absolutely better than old plan.
        call.getPlanner().setImportance(project, 0.0);
      }
    }
  }

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Join}.
   */
  public static class JoinReduceExpressionsRule extends ReduceExpressionsRule {

    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        RelBuilderFactory relBuilderFactory) {
      super(joinClass, relBuilderFactory, "ReduceExpressionsRule(Join)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final List<RexNode> expList = Lists.newArrayList(join.getCondition());
      final int fieldCount = join.getLeft().getRowType().getFieldCount();
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final RelOptPredicateList leftPredicates =
          mq.getPulledUpPredicates(join.getLeft());
      final RelOptPredicateList rightPredicates =
          mq.getPulledUpPredicates(join.getRight());
      final RelOptPredicateList predicates =
          leftPredicates.union(rightPredicates.shift(fieldCount));
      if (!reduceExpressions(join, expList, predicates)) {
        return;
      }
      if (join instanceof EquiJoin) {
        final JoinInfo joinInfo =
            JoinInfo.of(join.getLeft(), join.getRight(), expList.get(0));
        if (!joinInfo.isEqui()) {
          // This kind of join must be an equi-join, and the condition is
          // no longer an equi-join. SemiJoin is an example of this.
          return;
        }
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
      call.getPlanner().setImportance(join, 0.0);
    }
  }

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Calc}.
   */
  public static class CalcReduceExpressionsRule extends ReduceExpressionsRule {

    public CalcReduceExpressionsRule(Class<? extends Calc> calcClass,
        RelBuilderFactory relBuilderFactory) {
      super(calcClass, relBuilderFactory, "ReduceExpressionsRule(Calc)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Calc calc = call.rel(0);
      RexProgram program = calc.getProgram();
      final List<RexNode> exprList = program.getExprList();

      // Form a list of expressions with sub-expressions fully expanded.
      final List<RexNode> expandedExprList = Lists.newArrayList();
      final RexShuttle shuttle =
          new RexShuttle() {
            public RexNode visitLocalRef(RexLocalRef localRef) {
              return expandedExprList.get(localRef.getIndex());
            }
          };
      for (RexNode expr : exprList) {
        expandedExprList.add(expr.accept(shuttle));
      }
      final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
      if (reduceExpressions(calc, expandedExprList, predicates)) {
        final RexProgramBuilder builder =
            new RexProgramBuilder(
                calc.getInput().getRowType(),
                calc.getCluster().getRexBuilder());
        final List<RexLocalRef> list = Lists.newArrayList();
        for (RexNode expr : expandedExprList) {
          list.add(builder.registerInput(expr));
        }
        if (program.getCondition() != null) {
          final int conditionIndex =
              program.getCondition().getIndex();
          final RexNode newConditionExp =
              expandedExprList.get(conditionIndex);
          if (newConditionExp.isAlwaysTrue()) {
            // condition is always TRUE - drop it
          } else if (newConditionExp instanceof RexLiteral
              || RexUtil.isNullLiteral(newConditionExp, true)) {
            // condition is always NULL or FALSE - replace calc
            // with empty
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
        call.getPlanner().setImportance(calc, 0.0);
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
     * to an empty
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
  }

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReduceExpressionsRule.
   *
   * @param clazz class of rels to which this rule should apply
   */
  protected ReduceExpressionsRule(Class<? extends RelNode> clazz,
      RelBuilderFactory relBuilderFactory, String desc) {
    super(operand(clazz, any()), relBuilderFactory, desc);
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
    return reduceExpressions(rel, expList, predicates, false);
  }

  /**
   * Reduces a list of expressions.
   *
   * @param rel     Relational expression
   * @param expList List of expressions, modified in place
   * @param predicates Constraints known to hold on input expressions
   * @param unknownAsFalse Whether UNKNOWN will be treated as FALSE
   *
   * @return whether reduction found something to change, and succeeded
   */
  protected static boolean reduceExpressions(RelNode rel, List<RexNode> expList,
      RelOptPredicateList predicates, boolean unknownAsFalse) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    boolean reduced = reduceExpressionsInternal(rel, expList, predicates);

    // Simplify preds in place
    boolean simplified = false;
    for (int i = 0; i < expList.size(); i++) {
      RexNode newExp = RexUtil.simplify(rexBuilder, expList.get(i),
          unknownAsFalse);
      if (!newExp.toString().equals(expList.get(i).toString())) {
        expList.remove(i);
        expList.add(i, newExp);
        simplified = true;
      }
    }

    return reduced || simplified;
  }

  protected static boolean reduceExpressionsInternal(RelNode rel, List<RexNode> expList,
      RelOptPredicateList predicates) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Replace predicates on CASE to CASE on predicates.
    new CaseShuttle().mutate(expList);

    // Find reducible expressions.
    final List<RexNode> constExps = Lists.newArrayList();
    List<Boolean> addCasts = Lists.newArrayList();
    final List<RexNode> removableCasts = Lists.newArrayList();
    final ImmutableMap<RexNode, RexLiteral> constants =
        predicateConstants(RexLiteral.class, rexBuilder, predicates);
    findReducibleExps(rel.getCluster().getTypeFactory(), expList, constants,
        constExps, addCasts, removableCasts);
    if (constExps.isEmpty() && removableCasts.isEmpty()) {
      return false;
    }

    // Remove redundant casts before reducing constant expressions.
    // If the argument to the redundant cast is a reducible constant,
    // reducing that argument to a constant first will result in not being
    // able to locate the original cast expression.
    if (!removableCasts.isEmpty()) {
      final List<RexNode> reducedExprs = Lists.newArrayList();
      for (RexNode exp : removableCasts) {
        RexCall call = (RexCall) exp;
        reducedExprs.add(call.getOperands().get(0));
      }
      RexReplacer replacer =
          new RexReplacer(
              rexBuilder,
              removableCasts,
              reducedExprs,
              Collections.nCopies(removableCasts.size(), false));
      replacer.mutate(expList);
    }

    if (constExps.isEmpty()) {
      return true;
    }

    final List<RexNode> constExps2 = Lists.newArrayList(constExps);
    if (!constants.isEmpty()) {
      //noinspection unchecked
      final List<Map.Entry<RexNode, RexNode>> pairs =
          (List<Map.Entry<RexNode, RexNode>>) (List)
              Lists.newArrayList(constants.entrySet());
      RexReplacer replacer =
          new RexReplacer(
              rexBuilder,
              Pair.left(pairs),
              Pair.right(pairs),
              Collections.nCopies(pairs.size(), false));
      replacer.mutate(constExps2);
    }

    // Compute the values they reduce to.
    RelOptPlanner.Executor executor =
        rel.getCluster().getPlanner().getExecutor();
    if (executor == null) {
      // Cannot reduce expressions: caller has not set an executor in their
      // environment. Caller should execute something like the following before
      // invoking the planner:
      //
      // final RexExecutorImpl executor =
      //   new RexExecutorImpl(Schemas.createDataContext(null));
      // rootRel.getCluster().getPlanner().setExecutor(executor);
      return false;
    }

    final List<RexNode> reducedValues = Lists.newArrayList();
    executor.reduce(rexBuilder, constExps2, reducedValues);

    // Use RexNode.digest to judge whether each newly generated RexNode
    // is equivalent to the original one.
    if (RexUtil.strings(constExps).equals(RexUtil.strings(reducedValues))) {
      return false;
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

    RexReplacer replacer =
        new RexReplacer(
            rexBuilder,
            constExps,
            reducedValues,
            addCasts);
    replacer.mutate(expList);
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
   * @param removableCasts returns the list of cast expressions where the cast
   */
  protected static void findReducibleExps(RelDataTypeFactory typeFactory,
      List<RexNode> exps, ImmutableMap<RexNode, ? extends RexNode> constants,
      List<RexNode> constExps, List<Boolean> addCasts,
      List<RexNode> removableCasts) {
    ReducibleExprLocator gardener =
        new ReducibleExprLocator(typeFactory, constants, constExps,
            addCasts, removableCasts);
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
   */
  protected static <C extends RexNode> ImmutableMap<RexNode, C>
  predicateConstants(Class<C> clazz,
      RexBuilder rexBuilder, RelOptPredicateList predicates) {
    // We cannot use an ImmutableMap.Builder here. If there are multiple entries
    // with the same key (e.g. "WHERE deptno = 1 AND deptno = 2"), it doesn't
    // matter which we take, so the latter will replace the former.
    // The basic idea is to find all the pairs of RexNode = RexLiteral
    // (1) If 'predicates' contain a non-EQUALS, we bail out.
    // (2) It is OK if a RexNode is equal to the same RexLiteral several times,
    // (e.g. "WHERE deptno = 1 AND deptno = 1")
    // (3) It will return false if there are inconsistent constraints (e.g.
    // "WHERE deptno = 1 AND deptno = 2")
    final Map<RexNode, C> map = new HashMap<>();
    final Set<RexNode> excludeSet = new HashSet<>();
    for (RexNode predicate : predicates.pulledUpPredicates) {
      gatherConstraints(clazz, predicate, map, excludeSet, rexBuilder);
    }
    final ImmutableMap.Builder<RexNode, C> builder =
        ImmutableMap.builder();
    for (Map.Entry<RexNode, C> entry : map.entrySet()) {
      RexNode rexNode = entry.getKey();
      if (!overlap(rexNode, excludeSet)) {
        builder.put(rexNode, entry.getValue());
      }
    }
    return builder.build();
  }

  private static boolean overlap(RexNode rexNode, Set<RexNode> set) {
    if (rexNode instanceof RexCall) {
      for (RexNode r : ((RexCall) rexNode).getOperands()) {
        if (overlap(r, set)) {
          return true;
        }
      }
      return false;
    } else {
      return set.contains(rexNode);
    }
  }

  /** Tries to decompose the RexNode which is a RexCall into non-literal
   * RexNodes. */
  private static void decompose(Set<RexNode> set, RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      for (RexNode r : ((RexCall) rexNode).getOperands()) {
        decompose(set, r);
      }
    } else if (!(rexNode instanceof RexLiteral)) {
      set.add(rexNode);
    }
  }

  private static <C extends RexNode> void gatherConstraints(Class<C> clazz,
      RexNode predicate, Map<RexNode, C> map, Set<RexNode> excludeSet,
      RexBuilder rexBuilder) {
    if (predicate.getKind() != SqlKind.EQUALS) {
      decompose(excludeSet, predicate);
      return;
    }
    final List<RexNode> operands = ((RexCall) predicate).getOperands();
    if (operands.size() != 2) {
      decompose(excludeSet, predicate);
      return;
    }
    // if it reaches here, we have rexNode equals rexNode
    final RexNode left = operands.get(0);
    final RexNode right = operands.get(1);
    // note that literals are immutable too and they can only be compared through
    // values.
    gatherConstraint(clazz, left, right, map, excludeSet, rexBuilder);
    gatherConstraint(clazz, right, left, map, excludeSet, rexBuilder);
  }

  private static <C extends RexNode> void gatherConstraint(Class<C> clazz,
      RexNode left, RexNode right, Map<RexNode, C> map, Set<RexNode> excludeSet,
      RexBuilder rexBuilder) {
    if (!clazz.isInstance(right)) {
      return;
    }
    if (!RexUtil.isConstant(right)) {
      return;
    }
    C constant = clazz.cast(right);
    if (excludeSet.contains(left)) {
      return;
    }
    final C existedValue = map.get(left);
    if (existedValue == null) {
      switch (left.getKind()) {
      case CAST:
        // Convert "CAST(c) = literal" to "c = literal", as long as it is a
        // widening cast.
        final RexNode operand = ((RexCall) left).getOperands().get(0);
        if (canAssignFrom(left.getType(), operand.getType())) {
          final RexNode castRight =
              rexBuilder.makeCast(operand.getType(), constant);
          if (castRight instanceof RexLiteral) {
            left = operand;
            constant = clazz.cast(castRight);
          }
        }
      }
      map.put(left, constant);
    } else {
      if (existedValue instanceof RexLiteral
          && constant instanceof RexLiteral
          && !((RexLiteral) existedValue).getValue()
              .equals(((RexLiteral) constant).getValue())) {
        // we found conflicting values, e.g. left = 10 and left = 20
        map.remove(left);
        excludeSet.add(left);
      }
    }
  }

  /** Returns whether a value of {@code type2} can be assigned to a variable
   * of {@code type1}.
   *
   * <p>For example:
   * <ul>
   *   <li>{@code canAssignFrom(BIGINT, TINYINT)} returns {@code true}</li>
   *   <li>{@code canAssignFrom(TINYINT, BIGINT)} returns {@code false}</li>
   *   <li>{@code canAssignFrom(BIGINT, VARCHAR)} returns {@code false}</li>
   * </ul>
   */
  private static boolean canAssignFrom(RelDataType type1, RelDataType type2) {
    final SqlTypeName name1 = type1.getSqlTypeName();
    final SqlTypeName name2 = type2.getSqlTypeName();
    if (name1.getFamily() == name2.getFamily()) {
      switch (name1.getFamily()) {
      case NUMERIC:
        return name1.compareTo(name2) >= 0;
      default:
        return true;
      }
    }
    return false;
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
    }
    int caseOrdinal = -1;
    final List<RexNode> operands = call.getOperands();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      switch (operand.getKind()) {
      case CASE:
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
    private final RexBuilder rexBuilder;
    private final List<RexNode> reducibleExps;
    private final List<RexNode> reducedValues;
    private final List<Boolean> addCasts;

    RexReplacer(
        RexBuilder rexBuilder,
        List<RexNode> reducibleExps,
        List<RexNode> reducedValues,
        List<Boolean> addCasts) {
      this.rexBuilder = rexBuilder;
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
      if (node != call) {
        node = RexUtil.simplify(rexBuilder, node);
      }
      return node;
    }

    private RexNode visit(final RexNode call) {
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
        replacement = rexBuilder.makeCast(call.getType(), replacement, true);
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

    private final RelDataTypeFactory typeFactory;

    private final List<Constancy> stack;

    private final ImmutableMap<RexNode, ? extends RexNode> constants;

    private final List<RexNode> constExprs;

    private final List<Boolean> addCasts;

    private final List<RexNode> removableCasts;

    private final Deque<SqlOperator> parentCallTypeStack = new ArrayDeque<>();

    ReducibleExprLocator(RelDataTypeFactory typeFactory,
        ImmutableMap<RexNode, ? extends RexNode> constants,
        List<RexNode> constExprs,
        List<Boolean> addCasts, List<RexNode> removableCasts) {
      // go deep
      super(true);
      this.typeFactory = typeFactory;
      this.constants = constants;
      this.constExprs = constExprs;
      this.addCasts = addCasts;
      this.removableCasts = removableCasts;
      this.stack = Lists.newArrayList();
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
      if (parentCallTypeStack.isEmpty()) {
        addCasts.add(false);
      } else {
        addCasts.add(isUdf(parentCallTypeStack.peek()));
      }
    }

    private Boolean isUdf(SqlOperator operator) {
      // return operator instanceof UserDefinedRoutine
      return false;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      if (constants.containsKey(inputRef)) {
        stack.add(Constancy.REDUCIBLE_CONSTANT);
        return null;
      }
      return pushVariable();
    }

    public Void visitLiteral(RexLiteral literal) {
      stack.add(Constancy.IRREDUCIBLE_CONSTANT);
      return null;
    }

    public Void visitOver(RexOver over) {
      // assume non-constant (running SUM(1) looks constant but isn't)
      analyzeCall(over, Constancy.NON_CONSTANT);
      return null;
    }

    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
      return pushVariable();
    }

    public Void visitCall(RexCall call) {
      // assume REDUCIBLE_CONSTANT until proven otherwise
      analyzeCall(call, Constancy.REDUCIBLE_CONSTANT);
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
        }
      }

      // Even if all operands are constant, the call itself may
      // be non-deterministic.
      if (!call.getOperator().isDeterministic()) {
        callConstancy = Constancy.NON_CONSTANT;
      } else if (call.getOperator().isDynamicFunction()) {
        // We can reduce the call to a constant, but we can't
        // cache the plan if the function is dynamic.
        // For now, treat it same as non-deterministic.
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

        // if this cast expression can't be reduced to a literal,
        // then see if we can remove the cast
        if (call.getOperator() == SqlStdOperatorTable.CAST) {
          reduceCasts(call);
        }
      }

      // pop operands off of the stack
      operandStack.clear();

      // pop this parent call operator off the stack
      parentCallTypeStack.pop();

      // push constancy result for this call onto stack
      stack.add(callConstancy);
    }

    private void reduceCasts(RexCall outerCast) {
      List<RexNode> operands = outerCast.getOperands();
      if (operands.size() != 1) {
        return;
      }
      RelDataType outerCastType = outerCast.getType();
      RelDataType operandType = operands.get(0).getType();
      if (operandType.equals(outerCastType)) {
        removableCasts.add(outerCast);
        return;
      }

      // See if the reduction
      // CAST((CAST x AS type) AS type NOT NULL)
      // -> CAST(x AS type NOT NULL)
      // applies.  TODO jvs 15-Dec-2008:  consider
      // similar cases for precision changes.
      if (!(operands.get(0) instanceof RexCall)) {
        return;
      }
      RexCall innerCast = (RexCall) operands.get(0);
      if (innerCast.getOperator() != SqlStdOperatorTable.CAST) {
        return;
      }
      if (innerCast.getOperands().size() != 1) {
        return;
      }
      RelDataType outerTypeNullable =
          typeFactory.createTypeWithNullability(outerCastType, true);
      RelDataType innerTypeNullable =
          typeFactory.createTypeWithNullability(operandType, true);
      if (outerTypeNullable != innerTypeNullable) {
        return;
      }
      if (operandType.isNullable()) {
        removableCasts.add(innerCast);
      }
    }

    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
      return pushVariable();
    }

    public Void visitRangeRef(RexRangeRef rangeRef) {
      return pushVariable();
    }

    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
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
}

// End ReduceExpressionsRule.java
