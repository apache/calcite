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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * Planner rule that reduces aggregate functions in
 * {@link org.apache.calcite.rel.core.Aggregate}s to simpler forms.
 *
 * <p>Rewrites:
 * <ul>
 *
 * <li>AVG(x) &rarr; SUM(x) / COUNT(x)
 *
 * <li>STDDEV_POP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *    / COUNT(x))
 *
 * <li>STDDEV_SAMP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
 *
 * <li>VAR_POP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / COUNT(x)
 *
 * <li>VAR_SAMP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *        / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
 *
 * <li>CORR(x, y) &rarr; COVAR_POP(x, y)
 *       / CASE (STDDEV_POP(x) * STDDEV_POP(y)) = 0 THEN NULL
 *         ELSE STDDEV_POP(x) * STDDEV_POP(y) END
 *
 * <li>COVAR_POP(x, y) &rarr;
 *     (SUM(x * y) - (SUM(x) FILTER (WHERE y IS NOT NULL)) * (SUM(y) FILTER (WHERE x IS NOT NULL))
 *        / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
 *
 * <li>COVAR_SAMP(x, y) &rarr;
 *     (SUM(x * y) - (SUM(x) FILTER (WHERE y IS NOT NULL)) * (SUM(y) FILTER (WHERE x IS NOT NULL))
 *     / REGR_COUNT(x, y)) / CASE REGR_COUNT(x, y) WHEN 1 THEN NULL ELSE REGR_COUNT(x, y) - 1 END
 *
 * <li>REGR_AVGX(x, y) &rarr; (SUM(y) FILTER (WHERE x IS NOT NULL)) / REGR_COUNT(x, y)
 *
 * <li>REGR_AVGY(x, y) &rarr; (SUM(x) FILTER (WHERE y IS NOT NULL)) / REGR_COUNT(x, y)
 *
 * <li>REGR_INTERCEPT(x, y) &rarr; REGR_AVGY(x, y) - REGR_SLOPE(x, y) * REGR_AVGX(x, y)
 *
 * <li>REGR_R2(x, y) &rarr; CASE WHEN VAR_POP(y) = 0 THEN NULL
 *     WHEN VAR_POP(x) = 0 AND VAR_POP(y) != 0 THEN 1
 *     WHEN VAR_POP(x) &gt; 0 AND VAR_POP(y) != 0 THEN POWER(CORR(x, y), 2)
 *     ELSE NULL
 *
 * <li>REGR_SLOPE(x, y) &rarr; COVAR_POP(x * y)
 *     / CASE WHEN VAR_POP(y) = 0 THEN NULL ELSE VAR_POP(y) END
 *
 * <li>REGR_SXX(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(y)
 *
 * <li>REGR_SXY(x, y) &rarr; REGR_COUNT(x, y) * COVAR_POP(x, y)
 *
 * <li>REGR_SYY(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(x)
 *
 * </ul>
 *
 * <p>Since many of these rewrites introduce multiple occurrences of simpler
 * forms like {@code COUNT(x)}, the rule gathers common sub-expressions as it
 * goes.
 *
 * @see CoreRules#AGGREGATE_REDUCE_FUNCTIONS
 */
@Value.Enclosing
public class AggregateReduceFunctionsRule
    extends RelRule<AggregateReduceFunctionsRule.Config>
    implements TransformationRule {
  //~ Static fields/initializers ---------------------------------------------

  private static void validateFunction(SqlKind function) {
    if (!isValid(function)) {
      throw new IllegalArgumentException("AggregateReduceFunctionsRule doesn't "
          + "support function: " + function.sql);
    }
  }

  private static boolean isValid(SqlKind function) {
    return SqlKind.AVG_AGG_FUNCTIONS.contains(function)
        || SqlKind.COVAR_AVG_AGG_FUNCTIONS.contains(function)
        || function == SqlKind.SUM;
  }

  private final Set<SqlKind> functionsToReduce;

  //~ Constructors -----------------------------------------------------------

  /** Creates an AggregateReduceFunctionsRule. */
  protected AggregateReduceFunctionsRule(Config config) {
    super(config);
    this.functionsToReduce =
        ImmutableSet.copyOf(config.actualFunctionsToReduce());
  }

  @Deprecated // to be removed before 2.0
  public AggregateReduceFunctionsRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class)
        // reduce all functions handled by this rule
        .withFunctionsToReduce(null));
  }

  @Deprecated // to be removed before 2.0
  public AggregateReduceFunctionsRule(Class<? extends Aggregate> aggregateClass,
      RelBuilderFactory relBuilderFactory, EnumSet<SqlKind> functionsToReduce) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass)
        // reduce specific functions provided by the client
        .withFunctionsToReduce(Objects.requireNonNull(functionsToReduce, "functionsToReduce")));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    Aggregate oldAggRel = (Aggregate) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  @Override public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    return aggCallList.stream().anyMatch(this::canReduce);
  }

  /** Returns whether this rule can reduce a given aggregate function call. */
  public boolean canReduce(AggregateCall call) {
    return functionsToReduce.contains(call.getAggregation().getKind())
        && config.extraCondition().test(call);
  }

  /** Returns whether this rule can reduce some agg-call,
   * which its arg exists in the aggregate's group. */
  public boolean canReduceAggCallByGrouping(Aggregate oldAggRel, AggregateCall call) {
    if (!Aggregate.isSimple(oldAggRel)) {
      return false;
    }
    if (call.hasFilter() || call.distinctKeys != null || call.collation != RelCollations.EMPTY) {
      return false;
    }
    final List<Integer> argList = call.getArgList();
    if (argList.size() != 1) {
      return false;
    }
    if (!oldAggRel.getGroupSet().asSet().contains(argList.get(0))) {
      // arg doesn't exist in aggregate's group.
      return false;
    }
    final SqlKind kind = call.getAggregation().getKind();
    switch (kind) {
    case AVG:
    case MAX:
    case MIN:
    case ANY_VALUE:
    case FIRST_VALUE:
    case LAST_VALUE:
      return true;
    default:
      return false;
    }
  }

  /**
   * Reduces calls to functions AVG, SUM, STDDEV_POP, STDDEV_SAMP, VAR_POP,
   * VAR_SAMP, COVAR_POP, COVAR_SAMP, REGR_SXX, REGR_SYY if the function is
   * present in {@link AggregateReduceFunctionsRule#functionsToReduce}
   *
   * <p>It handles newly generated common subexpressions since this was done
   * at the sql2rel stage.
   */
  private void reduceAggs(
      RelOptRuleCall ruleCall,
      Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    final List<RexNode> projList = new ArrayList<>();

    // pass through group key
    for (int i = 0; i < groupCount; ++i) {
      projList.add(rexBuilder.makeInputRef(oldAggRel, i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new aggregate function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(
              oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount =
        inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    newCalcRel(relBuilder, oldAggRel.getRowType(), projList);
    final RelNode build = relBuilder.build();
    ruleCall.transformTo(build);
  }

  private RexNode reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    if (canReduceAggCallByGrouping(oldAggRel, oldCall)) {
      // replace original MAX/MIN/AVG/ANY_VALUE/FIRST_VALUE/LAST_VALUE(x) with
      // target field of x, when x exists in group
      final RexNode reducedNode = reduceAggCallByGrouping(oldAggRel, oldCall);
      return reducedNode;
    } else if (canReduce(oldCall)) {
      final Integer y;
      final Integer x;
      final SqlKind kind = oldCall.getAggregation().getKind();
      switch (kind) {
      case SUM:
        // replace original SUM(x) with
        // case COUNT(x) when 0 then null else SUM0(x) end
        return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
      case AVG:
        // replace original AVG(x) with SUM(x) / COUNT(x)
        return reduceAvg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case CORR:
        // replace original CORR(x, y) with
        // COVAR_POP(x, y) / CASE (STDDEV_POP(x) * STDDEV_POP(y)) = 0 THEN NULL
        //                    ELSE STDDEV_POP(x) * STDDEV_POP(y) END
        return reduceCorr(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case COVAR_POP:
        // replace original COVAR_POP(x, y) with
        // (SUM(x * y)
        //     - (SUM(y) FILTER (WHERE x IS NOT NULL)) * (SUM(x) FILTER (WHERE y IS NOT NULL))
        //       / REGR_COUNT(x, y))
        //  / REGR_COUNT(x, y))
        return reduceCovariance(oldAggRel, oldCall, true, newCalls,
            aggCallMapping, inputExprs);
      case COVAR_SAMP:
        // replace original COVAR_SAMP(x, y) with
        // (SUM(x * y)
        //     - (SUM(x) FILTER (WHERE y IS NOT NULL)) * (SUM(y) FILTER (WHERE x IS NOT NULL)))
        //      / REGR_COUNT(x, y))
        //  / CASE REGR_COUNT(x, y) WHEN 1 THEN NULL ELSE REGR_COUNT(x, y) - 1 END)
        return reduceCovariance(oldAggRel, oldCall, false, newCalls,
            aggCallMapping, inputExprs);
      case REGR_AVGX:
        // replace original REGR_AVGX(x, y) with
        // SUM(y) FILTER (WHERE x IS NOT NULL) / REGR_COUNT(x, y)
        return reduceAvgZ(oldAggRel, oldCall, newCalls, aggCallMapping, true, inputExprs);
      case REGR_AVGY:
        // replace original REGR_AVGY(x, y) with
        // SUM(x) FILTER (WHERE y IS NOT NULL) / REGR_COUNT(x, y)
        return reduceAvgZ(oldAggRel, oldCall, newCalls, aggCallMapping, false, inputExprs);
      case REGR_SXX:
        // replace original REGR_SXX(x, y) with
        // REGR_COUNT(x, y) * VAR_POP(y)
        assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
        x = oldCall.getArgList().get(0);
        y = oldCall.getArgList().get(1);
        //noinspection SuspiciousNameCombination
        return reduceRegrSzz(oldAggRel, oldCall, newCalls, aggCallMapping,
            inputExprs, y, y, x);
      case REGR_SXY:
        // replace original REGR_SXY(x, y) with
        // REGR_COUNT(x, y) * COVAR_POP(x, y)
        assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
        x = oldCall.getArgList().get(0);
        y = oldCall.getArgList().get(1);
        //noinspection SuspiciousNameCombination
        return reduceRegrSzz(oldAggRel, oldCall, newCalls, aggCallMapping,
            inputExprs, x, y, y);
      case REGR_SYY:
        // replace original REGR_SYY(x, y) with
        // REGR_COUNT(x, y) * VAR_POP(x)
        assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
        x = oldCall.getArgList().get(0);
        y = oldCall.getArgList().get(1);
        //noinspection SuspiciousNameCombination
        return reduceRegrSzz(oldAggRel, oldCall, newCalls, aggCallMapping,
            inputExprs, x, x, y);
      case STDDEV_POP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x))
        return reduceStddev(oldAggRel, oldCall, true, true, newCalls,
            aggCallMapping, inputExprs);
      case STDDEV_SAMP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
        return reduceStddev(oldAggRel, oldCall, false, true, newCalls,
            aggCallMapping, inputExprs);
      case VAR_POP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x)
        return reduceStddev(oldAggRel, oldCall, true, false, newCalls,
            aggCallMapping, inputExprs);
      case REGR_INTERCEPT:
        // replace original REGR_INTERCEPT(x, y) with
        // REGR_AVGX(x, y) - REGR_SLOPE(x, y) * REGR_AVGY(x, y)
        return reduceRegrIntercept(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case REGR_R2:
        // replace original REGR_R2(x, y) with
        // CASE WHEN VAR_POP(y) = 0 THEN NULL
        // WHEN VAR_POP(x) = 0 AND VAR_POP(y) != 0 THEN 1
        // WHEN VAR_POP(x) > 0 AND VAR_POP(y) != 0 THEN POWER(CORR(x, y), 2)
        // ELSE NULL END
        return reduceRegrR2(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case REGR_SLOPE:
        // replace original REGR_SLOPE(X, Y) with
        // COVAR_POP(x * y) / CASE WHEN VAR_POP(y) = 0 THEN NULL ELSE VAR_POP(y) END
        return reduceRegrSlope(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case VAR_SAMP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
        return reduceStddev(oldAggRel, oldCall, false, false, newCalls,
            aggCallMapping, inputExprs);
      default:
        throw Util.unexpected(kind);
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldAggRel.getInput()::fieldIsNullable);
    }
  }

  private static AggregateCall createAggregateCallWithBinding(
      RelDataTypeFactory typeFactory,
      SqlAggFunction aggFunction,
      RelDataType operandType,
      Aggregate oldAggRel,
      AggregateCall oldCall,
      int argOrdinal,
      int filter) {
    final Aggregate.AggCallBinding binding =
        new Aggregate.AggCallBinding(typeFactory, aggFunction,
            ImmutableList.of(operandType), oldAggRel.getGroupCount(),
            filter >= 0);
    return AggregateCall.create(aggFunction,
        oldCall.isDistinct(),
        oldCall.isApproximate(),
        oldCall.ignoreNulls(),
        ImmutableIntList.of(argOrdinal),
        filter,
        oldCall.distinctKeys,
        oldCall.collation,
        aggFunction.inferReturnType(binding),
        null);
  }

  private static RexNode reduceAvg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      @SuppressWarnings("unused") List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final AggregateCall sumCall =
        AggregateCall.create(SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);
    final AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode numeratorRef =
        rexBuilder.addAggCall(sumCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    final RexNode denominatorRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);

    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    final RelDataType avgType = typeFactory.createTypeWithNullability(
        oldCall.getType(), numeratorRef.getType().isNullable());
    numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);
    final RexNode divideRef =
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
    return rexBuilder.makeCast(oldCall.getType(), divideRef);
  }

  private static RexNode reduceSum(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    final AggregateCall sumZeroCall =
        AggregateCall.create(SqlStdOperatorTable.SUM0, oldCall.isDistinct(),
            oldCall.isApproximate(), oldCall.ignoreNulls(),
            oldCall.getArgList(), oldCall.filterArg, oldCall.distinctKeys,
            oldCall.collation, oldAggRel.getGroupCount(), oldAggRel.getInput(),
            null, oldCall.name);
    final AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel,
            null,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode sumZeroRef =
        rexBuilder.addAggCall(sumZeroCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    if (!oldCall.getType().isNullable()) {
      // If SUM(x) is not nullable, the validator must have determined that
      // nulls are impossible (because the group is never empty and x is never
      // null). Therefore we translate to SUM0(x).
      return sumZeroRef;
    }
    RexNode countRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
        rexBuilder.makeNullLiteral(sumZeroRef.getType()),
        sumZeroRef);
  }

  private static RexNode reduceRegrIntercept(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // regr_intercept(x, y) ==>
    //     regr_avgx(x, y) - regr_slope(x, y) * regr_avgy(x, y)
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final RexNode avgX = reduceAvgZ(oldAggRel, oldCall,  newCalls,
        aggCallMapping, true, inputExprs);
    final RexNode avgY = reduceAvgZ(oldAggRel, oldCall,  newCalls,
        aggCallMapping, false, inputExprs);
    final RexNode regrSlope = reduceRegrSlope(oldAggRel, oldCall, newCalls,
        aggCallMapping, inputExprs);
    final RexNode product = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, regrSlope, avgY);
    final RexNode result = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, avgX, product);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceRegrR2(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // regr_r2(x, y) ==>
    //     CASE WHEN VAR_POP(y) = 0 THEN NULL
    //     WHEN VAR_POP(x) = 0 AND VAR_POP(y) != 0 THEN 1
    //     WHEN VAR_POP(x) > 0 AND VAR_POP(y) != 0 THEN POWER(CORR(x, y), 2)
    //     ELSE NULL END
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;

    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argXOrdinal)
                || fieldIsNullable.test(argYOrdinal));
    final RexNode argX =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argXOrdinal), true);
    final RexNode argY =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argYOrdinal), true);
    final RexNode argXArgX = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argX);
    final RexNode argXArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argY);
    final RexNode argYArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argY, argY);
    final int argXXOrdinal = lookupOrAdd(inputExprs, argXArgX);
    final int argXYOrdinal = lookupOrAdd(inputExprs, argXArgY);
    final int argYYOrdinal = lookupOrAdd(inputExprs, argYArgY);
    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode sumX = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, argXOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumY = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, argYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXX = getSumAggregatedRexNodeWithBinding(oldAggRel, oldCall, newCalls,
        aggCallMapping, argXArgX.getType(), argXXOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXY = getSumAggregatedRexNodeWithBinding(oldAggRel, oldCall, newCalls,
        aggCallMapping, argXArgY.getType(), argXYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumYY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argYArgY.getType(),
        argYYOrdinal, argXAndYNotNullFilterOrdinal);
    RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
    final RexNode countNode =
        getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
            ImmutableIntList.of(argXOrdinal, argYOrdinal),
            argXAndYNotNullFilterOrdinal);
    final RexNode count = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countNode, zero),
        rexBuilder.makeNullLiteral(zero.getType()),
        countNode);
    RexNode sumXSumX = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumX);
    RexNode sumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);
    RexNode sumYSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumY, sumY);
    final RexNode covarPop = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXY,
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumY, count));
    final RexNode varPopXNominator =
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXX,
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumX, count));
    final RexNode varPopYNominator =
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumYY,
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumYSumY, count));
    RexNode nul = rexBuilder.makeCast(
        oldCall.getType(), rexBuilder.makeNullLiteral(oldCall.getType()));
    RexLiteral one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexNode result = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, varPopXNominator, zero), nul,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, varPopYNominator, zero), one,
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, varPopXNominator, zero),
                rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, covarPop, covarPop),
                nul);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceRegrSlope(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // regr_slope(x, y) ==>
    //     case when var_pop(y) = 0 then null
    //     else covar_pop(x, y) / var_pop(y)
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;

    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argXOrdinal)
                || fieldIsNullable.test(argYOrdinal));
    final RexNode argX =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argXOrdinal), true);
    final RexNode argY =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argYOrdinal), true);
    final RexNode argXArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argY);
    final RexNode argYArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argY, argY);
    final int argXArgYOrdinal = lookupOrAdd(inputExprs, argXArgY);
    final int argYYOrdinal = lookupOrAdd(inputExprs, argYArgY);
    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode sumXY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argXArgY.getType(),
        argXArgYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumYY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argYArgY.getType(),
        argYYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumX =
        getSumAggregatedRexNode(oldAggRel, oldCall, newCalls,
            aggCallMapping, rexBuilder, argXOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumY = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, argYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);
    final RexNode sumYSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumY, sumY);
    final RexNode countArg = getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        ImmutableIntList.of(argXOrdinal, argYOrdinal),
        argXAndYNotNullFilterOrdinal);
    RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
    RexLiteral nul = rexBuilder.makeNullLiteral(zero.getType());
    final RexNode nominator = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
        sumXY, rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumY, countArg));
    final RexNode denominator = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
        sumYY, rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumYSumY, countArg));
    final RexNode result = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countArg, zero),
            rexBuilder.makeCast(oldCallType, nul),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, denominator, zero),
            rexBuilder.makeCast(oldCallType, nul),
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, nominator, denominator));
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceStddev(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      boolean biased,
      boolean sqrt,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // stddev_pop(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / count(x),
    //     .5)
    //
    // stddev_samp(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / nullif(count(x) - 1, 0),
    //     .5)
    final int nGroups = oldAggRel.getGroupCount();
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    assert oldCall.getArgList().size() == 1 : oldCall.getArgList();
    final int argOrdinal = oldCall.getArgList().get(0);
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argOrdinal));

    final RexNode argRef =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argOrdinal), true);

    final RexNode argSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
        argRef, argRef);
    final int argSquaredOrdinal = lookupOrAdd(inputExprs, argSquared);

    final AggregateCall sumArgSquaredAggCall =
        createAggregateCallWithBinding(typeFactory, SqlStdOperatorTable.SUM,
            argSquared.getType(), oldAggRel, oldCall, argSquaredOrdinal, -1);

    final RexNode sumArgSquared =
        rexBuilder.addAggCall(sumArgSquaredAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);

    final AggregateCall sumArgAggCall =
        AggregateCall.create(SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            ImmutableIntList.of(argOrdinal),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    final RexNode sumArg =
        rexBuilder.addAggCall(sumArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    final RexNode sumArgCast = rexBuilder.ensureType(oldCallType, sumArg, true);
    final RexNode sumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, sumArgCast, sumArgCast);

    final AggregateCall countArgAggCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel,
            null,
            null);

    final RexNode countArg =
        rexBuilder.addAggCall(countArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);

    final RexNode div = divide(biased, rexBuilder, sumArgSquared, sumSquaredArg,
        countArg);

    final RexNode result;
    if (sqrt) {
      final RexNode half =
          rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
      result =
          rexBuilder.makeCall(
              SqlStdOperatorTable.POWER, div, half);
    } else {
      result = div;
    }

    return rexBuilder.makeCast(
        oldCall.getType(), result);
  }

  private static RexNode reduceAggCallByGrouping(
      Aggregate oldAggRel,
      AggregateCall oldCall) {

    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final List<Integer> oldGroups = oldAggRel.getGroupSet().asList();
    final Integer firstArg = oldCall.getArgList().get(0);
    final int index = oldGroups.lastIndexOf(firstArg);
    assert index >= 0;

    final RexInputRef refByGroup = RexInputRef.of(index, oldAggRel.getRowType().getFieldList());
    if (refByGroup.getType().equals(oldCall.getType())) {
      return refByGroup;
    } else {
      return rexBuilder.makeCast(oldCall.getType(), refByGroup);
    }
  }

  private static RexNode getSumAggregatedRexNode(Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      RexBuilder rexBuilder,
      int argOrdinal,
      int filterArg) {
    final AggregateCall aggregateCall =
        AggregateCall.create(SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            ImmutableIntList.of(argOrdinal),
            filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);
    return rexBuilder.addAggCall(aggregateCall,
        oldAggRel.getGroupCount(),
        newCalls,
        aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
  }

  private static RexNode getSumAggregatedRexNodeWithBinding(Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      RelDataType operandType,
      int argOrdinal,
      int filter) {
    RelOptCluster cluster = oldAggRel.getCluster();
    final AggregateCall sumArgSquaredAggCall =
        createAggregateCallWithBinding(cluster.getTypeFactory(),
            SqlStdOperatorTable.SUM, operandType, oldAggRel, oldCall, argOrdinal, filter);

    return cluster.getRexBuilder().addAggCall(sumArgSquaredAggCall,
        oldAggRel.getGroupCount(),
        newCalls,
        aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
  }

  private static RexNode getRegrCountRexNode(Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      ImmutableIntList argOrdinals,
      int filterArg) {
    final AggregateCall countArgAggCall =
        AggregateCall.create(SqlStdOperatorTable.REGR_COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            argOrdinals,
            filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel,
            null,
            null);

    return oldAggRel.getCluster().getRexBuilder().addAggCall(countArgAggCall,
        oldAggRel.getGroupCount(),
        newCalls,
        aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
  }

  private static RexNode reduceRegrSzz(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs,
      int xIndex,
      int yIndex,
      int nullFilterIndex) {
    // regr_sxx(x, y) ==>
    //    regr_count(x, y) * var_pop(y)
    //
    // regr_sxy(x, y) ==>
    //    regr_count(x, y) * covar_pop(x, y)
    //
    // regr_syy(x, y) ==>
    //    regr_count(x, y) * var_pop(x)

    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;

    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(xIndex)
                || fieldIsNullable.test(yIndex)
                || fieldIsNullable.test(nullFilterIndex));

    final RexNode argX =
        rexBuilder.ensureType(oldCallType, inputExprs.get(xIndex), true);
    final RexNode argY =
        rexBuilder.ensureType(oldCallType, inputExprs.get(yIndex), true);
    final RexNode argNullFilter =
        rexBuilder.ensureType(oldCallType, inputExprs.get(nullFilterIndex), true);

    final RexNode argXArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argY);
    final int argSquaredOrdinal = lookupOrAdd(inputExprs, argXArgY);

    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY)),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argNullFilter));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode sumXY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argXArgY.getType(),
        argSquaredOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXYCast = rexBuilder.ensureType(oldCallType, sumXY, true);

    final RexNode sumX = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, xIndex, argXAndYNotNullFilterOrdinal);
    final RexNode sumY = xIndex == yIndex
        ? sumX
        : getSumAggregatedRexNode(oldAggRel, oldCall, newCalls,
            aggCallMapping, rexBuilder, yIndex, argXAndYNotNullFilterOrdinal);

    final RexNode sumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);

    final RexNode countArg = getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        ImmutableIntList.of(xIndex), argXAndYNotNullFilterOrdinal);

    RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
    RexNode nul = rexBuilder.makeNullLiteral(zero.getType());
    final RexNode avgSumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countArg, zero), nul,
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumY, countArg));
    final RexNode avgSumXSumYCast = rexBuilder.ensureType(oldCallType, avgSumXSumY, true);
    final RexNode result =
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXYCast, avgSumXSumYCast);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceAvgZ(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      boolean avgx,
      List<RexNode> inputExprs) {
    // regr_avgx(x, y) ==>
    //     (sum(y) filter (where x is not null)) / regr_count(x, y)
    //
    // regr_avgy(x, y) ==>
    //     (sum(x) filter (where y is not null)) / regr_count(x, y)
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;

    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argXOrdinal)
                || fieldIsNullable.test(argYOrdinal));
    final RexNode argX =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argXOrdinal), true);
    final RexNode argY =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argYOrdinal), true);
    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode sumArg = getSumAggregatedRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        rexBuilder, avgx ? argXOrdinal : argYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode countArg = getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        ImmutableIntList.of(argXOrdinal, argYOrdinal),
        argXAndYNotNullFilterOrdinal);
    final RexNode result =
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumArg, countArg);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceCorr(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // corr(x, y) ==>
    //   covar_pop(x, y) / (stddev_pop(x) * stddev_pop(y))
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;

    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argXOrdinal)
                || fieldIsNullable.test(argYOrdinal));
    final RexNode argX =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argXOrdinal), true);
    final RexNode argY =
        rexBuilder.ensureType(oldCallType, inputExprs.get(argYOrdinal), true);
    final RexNode argXArgX = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argX);
    final int argXSquaredOrdinal = lookupOrAdd(inputExprs, argXArgX);
    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode sumXX = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argXArgX.getType(),
        argXSquaredOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode argYArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argY, argY);
    final RexNode argXArgY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argY);
    final int argYSquaredOrdinal = lookupOrAdd(inputExprs, argYArgY);
    final RexNode sumYY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argYArgY.getType(),
        argYSquaredOrdinal, argXAndYNotNullFilterOrdinal);
    final int argXYOrdinal = lookupOrAdd(inputExprs, argYArgY);
    final RexNode sumXY = getSumAggregatedRexNodeWithBinding(
        oldAggRel, oldCall, newCalls, aggCallMapping, argXArgY.getType(),
        argXYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumX = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, argXOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumY = getSumAggregatedRexNode(oldAggRel, oldCall,
        newCalls, aggCallMapping, rexBuilder, argYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXSumX = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumX);
    final RexNode sumYSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumY, sumY);
    final RexNode sumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);
    final RexNode countArg = getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        ImmutableIntList.of(argXOrdinal, argYOrdinal),
        argXAndYNotNullFilterOrdinal);
    RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
    RexNode nul = rexBuilder.makeNullLiteral(zero.getType());
    final RexNode avgSumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumY, countArg);
    final RexNode avgSumXSumX = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumX, countArg);
    final RexNode avgSumYSumY = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumYSumY, countArg);
    RexLiteral half = rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
    final RexNode denominator = rexBuilder.makeCast(oldCall.getType(),
        rexBuilder.makeCall(SqlStdOperatorTable.POWER,
            rexBuilder.makeCast(oldCall.getType(),
                rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
                    rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXX, avgSumXSumX),
                    rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumYY, avgSumYSumY))), half));
    final RexNode result = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countArg, zero), nul,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, denominator, zero), nul,
        rexBuilder.makeCast(oldCall.getType(),
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXY, avgSumXSumY), denominator)));
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode reduceCovariance(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      boolean biased,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // covar_pop(x, y) ==>
    // (sum(x * y)
    //   - (sum(x) filter (where y is not null)) * (sum(y) filter (where x is not null))
    //     / regr_count(x, y))
    //  / regr_count(x, y)
    //
    // covar_samp(x, y) ==>
    // (sum(x * y)
    //   - (sum(x) filter (where y is not null)) * (sum(y) filter (where x is not null))
    //     / regr_count(x, y))
    //  / (case regr_count(x, y) = 1 then null else regr_count(x, y) - 1 end)
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final IntPredicate fieldIsNullable = oldAggRel.getInput()::fieldIsNullable;
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            fieldIsNullable.test(argXOrdinal)
                || fieldIsNullable.test(argYOrdinal));
    final RexNode argX = rexBuilder.ensureType(oldCallType, inputExprs.get(argXOrdinal), true);
    final RexNode argY = rexBuilder.ensureType(oldCallType, inputExprs.get(argYOrdinal), true);
    final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argX),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, argY));
    final int argXAndYNotNullFilterOrdinal = lookupOrAdd(inputExprs, argXAndYNotNullFilter);
    final RexNode argXY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, argX, argY);
    final int argXYOrdinal = lookupOrAdd(inputExprs, argXY);
    final RexNode sumXY = getSumAggregatedRexNodeWithBinding(oldAggRel, oldCall, newCalls,
        aggCallMapping, argXY.getType(), argXYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumX = getSumAggregatedRexNode(oldAggRel, oldCall, newCalls,
        aggCallMapping, rexBuilder, argXOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumY = getSumAggregatedRexNode(oldAggRel, oldCall, newCalls,
        aggCallMapping, rexBuilder, argYOrdinal, argXAndYNotNullFilterOrdinal);
    final RexNode sumXSumY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);
    final RexNode countArg = getRegrCountRexNode(oldAggRel, oldCall, newCalls, aggCallMapping,
        ImmutableIntList.of(argXOrdinal, argYOrdinal),
        argXAndYNotNullFilterOrdinal);
    final RexNode result = divide(biased, rexBuilder, sumXY, sumXSumY, countArg);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private static RexNode divide(boolean biased, RexBuilder rexBuilder, RexNode sumXY,
      RexNode sumXSumY, RexNode countArg) {
    final RexNode avgSumSquaredArg =
         rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumXSumY, countArg);
    final RexNode diff =
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumXY, avgSumSquaredArg);
    final RexNode denominator;
    if (biased) {
      denominator = countArg;
    } else {
      final RexLiteral one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
      final RexNode nul = rexBuilder.makeNullLiteral(countArg.getType());
      final RexNode countMinusOne =
          rexBuilder.makeCall(SqlStdOperatorTable.MINUS, countArg, one);
      final RexNode countEqOne =
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countArg, one);
      denominator = rexBuilder.makeCall(SqlStdOperatorTable.CASE, countEqOne,
          nul, countMinusOne);
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, diff, denominator);
  }

  /**
   * Finds the ordinal of an element in a list, or adds it.
   *
   * @param list    List
   * @param element Element to lookup or add
   * @param <T>     Element type
   * @return Ordinal of element in list
   */
  private static <T> int lookupOrAdd(List<T> list, T element) {
    int ordinal = list.indexOf(element);
    if (ordinal == -1) {
      ordinal = list.size();
      list.add(element);
    }
    return ordinal;
  }

  /**
   * Does a shallow clone of oldAggRel and updates aggCalls. Could be refactored
   * into Aggregate and subclasses - but it's only needed for some
   * subclasses.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param oldAggregate LogicalAggregate to clone.
   * @param newCalls  New list of AggregateCalls
   */
  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
  }

  /**
   * Adds a calculation with the expressions to compute the original aggregate
   * calls from the decomposed ones.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param rowType The output row type of the original aggregate.
   * @param exprs The expressions to compute the original aggregate calls
   */
  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateReduceFunctionsRule.Config.of()
        .withOperandFor(LogicalAggregate.class);

    Set<SqlKind> DEFAULT_FUNCTIONS_TO_REDUCE =
        ImmutableSet.<SqlKind>builder()
            .addAll(SqlKind.AVG_AGG_FUNCTIONS)
            .addAll(SqlKind.COVAR_AVG_AGG_FUNCTIONS)
            .add(SqlKind.SUM)
            .build();

    @Override default AggregateReduceFunctionsRule toRule() {
      return new AggregateReduceFunctionsRule(this);
    }

    /** The set of aggregate function types to try to reduce.
     *
     * <p>Any aggregate function whose type is omitted from this set, OR which
     * does not pass the {@link #extraCondition}, will be ignored.
     */
    @Nullable Set<SqlKind> functionsToReduce();

    /** A test that must pass before attempting to reduce any aggregate function.
     *
     * <p>Any aggegate function which does not pass, OR whose type is omitted
     * from {@link #functionsToReduce}, will be ignored. The default predicate
     * always passes.
     */
    @Value.Default
    default Predicate<AggregateCall> extraCondition() {
      return ignored -> true;
    }

    /** Sets {@link #functionsToReduce}. */
    Config withFunctionsToReduce(@Nullable Iterable<SqlKind> functionSet);

    default Config withFunctionsToReduce(@Nullable Set<SqlKind> functionSet) {
      return withFunctionsToReduce((Iterable<SqlKind>) functionSet);
    }

    /** Sets {@link #extraCondition}. */
    Config withExtraCondition(Predicate<AggregateCall> test);

    /** Returns the validated set of functions to reduce, or the default set
     * if not specified. */
    default Set<SqlKind> actualFunctionsToReduce() {
      final Set<SqlKind> set =
          Util.first(functionsToReduce(), DEFAULT_FUNCTIONS_TO_REDUCE);
      set.forEach(AggregateReduceFunctionsRule::validateFunction);
      return set;
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b -> b.operand(aggregateClass).anyInputs())
          .as(Config.class);
    }
  }
}
