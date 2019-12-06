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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * <li>COVAR_POP(x, y) &rarr; (SUM(x * y) - SUM(x, y) * SUM(y, x)
 *     / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
 *
 * <li>COVAR_SAMP(x, y) &rarr; (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y))
 *     / CASE REGR_COUNT(x, y) WHEN 1 THEN NULL ELSE REGR_COUNT(x, y) - 1 END
 *
 * <li>REGR_SXX(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(y)
 *
 * <li>REGR_SYY(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(x)
 *
 * </ul>
 *
 * <p>Since many of these rewrites introduce multiple occurrences of simpler
 * forms like {@code COUNT(x)}, the rule gathers common sub-expressions as it
 * goes.
 */
public class AggregateReduceFunctionsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final AggregateReduceFunctionsRule INSTANCE =
      new AggregateReduceFunctionsRule(operand(LogicalAggregate.class, any()),
          RelFactories.LOGICAL_BUILDER);

  private final EnumSet<SqlKind> functionsToReduce;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregateReduceFunctionsRule to reduce all functions
   * handled by this rule
   * @param operand operand to determine if rule can be applied
   * @param relBuilderFactory builder for relational expressions
   */
  public AggregateReduceFunctionsRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
    functionsToReduce = EnumSet.noneOf(SqlKind.class);
    addDefaultSetOfFunctionsToReduce();
  }

  /**
   * Creates an AggregateReduceFunctionsRule with client
   * provided information on which specific functions will
   * be reduced by this rule
   * @param aggregateClass aggregate class
   * @param relBuilderFactory builder for relational expressions
   * @param functionsToReduce client provided information
   *                          on which specific functions
   *                          will be reduced by this rule
   */
  public AggregateReduceFunctionsRule(Class<? extends Aggregate> aggregateClass,
      RelBuilderFactory relBuilderFactory, EnumSet<SqlKind> functionsToReduce) {
    super(operand(aggregateClass, any()), relBuilderFactory, null);
    Objects.requireNonNull(functionsToReduce,
        "Expecting a valid handle for AggregateFunctionsToReduce");
    this.functionsToReduce = EnumSet.noneOf(SqlKind.class);
    for (SqlKind function : functionsToReduce) {
      if (SqlKind.AVG_AGG_FUNCTIONS.contains(function)
          || SqlKind.COVAR_AVG_AGG_FUNCTIONS.contains(function)
          || function == SqlKind.SUM) {
        this.functionsToReduce.add(function);
      } else {
        throw new IllegalArgumentException(
          "AggregateReduceFunctionsRule doesn't support function: " + function.sql);
      }
    }
  }

  //~ Methods ----------------------------------------------------------------

  private void addDefaultSetOfFunctionsToReduce() {
    functionsToReduce.addAll(SqlKind.AVG_AGG_FUNCTIONS);
    functionsToReduce.addAll(SqlKind.COVAR_AVG_AGG_FUNCTIONS);
    functionsToReduce.add(SqlKind.SUM);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    Aggregate oldAggRel = (Aggregate) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    for (AggregateCall call : aggCallList) {
      if (isReducible(call.getAggregation().getKind())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether the aggregate call is a reducible function
   */
  private boolean isReducible(final SqlKind kind) {
    return functionsToReduce.contains(kind);
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
      projList.add(
          rexBuilder.makeInputRef(
              getFieldType(oldAggRel, i),
              i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new agg function calls and rest of project list together
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
    ruleCall.transformTo(relBuilder.build());
  }

  private RexNode reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final SqlKind kind = oldCall.getAggregation().getKind();
    if (isReducible(kind)) {
      final Integer y;
      final Integer x;
      switch (kind) {
      case SUM:
        // replace original SUM(x) with
        // case COUNT(x) when 0 then null else SUM0(x) end
        return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
      case AVG:
        // replace original AVG(x) with SUM(x) / COUNT(x)
        return reduceAvg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case COVAR_POP:
        // replace original COVAR_POP(x, y) with
        //     (SUM(x * y) - SUM(y) * SUM(y) / COUNT(x))
        //     / COUNT(x))
        return reduceCovariance(oldAggRel, oldCall, true, newCalls,
            aggCallMapping, inputExprs);
      case COVAR_SAMP:
        // replace original COVAR_SAMP(x, y) with
        //   SQRT(
        //     (SUM(x * y) - SUM(x) * SUM(y) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
        return reduceCovariance(oldAggRel, oldCall, false, newCalls,
            aggCallMapping, inputExprs);
      case REGR_SXX:
        // replace original REGR_SXX(x, y) with
        // REGR_COUNT(x, y) * VAR_POP(y)
        assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
        x = oldCall.getArgList().get(0);
        y = oldCall.getArgList().get(1);
        //noinspection SuspiciousNameCombination
        return reduceRegrSzz(oldAggRel, oldCall, newCalls, aggCallMapping,
            inputExprs, y, y, x);
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
      List<RelDataType> oldArgTypes =
          SqlTypeUtil.projectTypes(
              oldAggRel.getInput().getRowType(), oldCall.getArgList());
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldArgTypes);
    }
  }

  private AggregateCall createAggregateCallWithBinding(
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
        oldCall.collation,
        aggFunction.inferReturnType(binding),
        null);
  }

  private RexNode reduceAvg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final int iAvgInput = oldCall.getArgList().get(0);
    final RelDataType avgInputType =
        getFieldType(
            oldAggRel.getInput(),
            iAvgInput);
    final AggregateCall sumCall =
        AggregateCall.create(SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
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
            ImmutableList.of(avgInputType));
    final RexNode denominatorRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));

    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    final RelDataType avgType = typeFactory.createTypeWithNullability(
        oldCall.getType(), numeratorRef.getType().isNullable());
    numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);
    final RexNode divideRef =
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
    return rexBuilder.makeCast(oldCall.getType(), divideRef);
  }

  private RexNode reduceSum(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int arg = oldCall.getArgList().get(0);
    RelDataType argType =
        getFieldType(
            oldAggRel.getInput(),
            arg);

    final AggregateCall sumZeroCall =
        AggregateCall.create(SqlStdOperatorTable.SUM0, oldCall.isDistinct(),
            oldCall.isApproximate(), oldCall.ignoreNulls(),
            oldCall.getArgList(), oldCall.filterArg,
            oldCall.collation, oldAggRel.getGroupCount(), oldAggRel.getInput(),
            null, oldCall.name);
    final AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
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
            ImmutableList.of(argType));
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
            ImmutableList.of(argType));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
        rexBuilder.makeNullLiteral(sumZeroRef.getType()),
        sumZeroRef);
  }

  private RexNode reduceStddev(
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
    final RelDataType argOrdinalType = getFieldType(oldAggRel.getInput(), argOrdinal);
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            argOrdinalType.isNullable());

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
            ImmutableList.of(sumArgSquaredAggCall.getType()));

    final AggregateCall sumArgAggCall =
        AggregateCall.create(SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            ImmutableIntList.of(argOrdinal),
            oldCall.filterArg,
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
            ImmutableList.of(sumArgAggCall.getType()));
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
            ImmutableList.of(argOrdinalType));

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

  private RexNode getSumAggregatedRexNode(Aggregate oldAggRel,
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
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);
    return rexBuilder.addAggCall(aggregateCall,
        oldAggRel.getGroupCount(),
        newCalls,
        aggCallMapping,
        ImmutableList.of(aggregateCall.getType()));
  }

  private RexNode getSumAggregatedRexNodeWithBinding(Aggregate oldAggRel,
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
        ImmutableList.of(sumArgSquaredAggCall.getType()));
  }

  private RexNode getRegrCountRexNode(Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      ImmutableIntList argOrdinals,
      ImmutableList<RelDataType> operandTypes,
      int filterArg) {
    final AggregateCall countArgAggCall =
        AggregateCall.create(SqlStdOperatorTable.REGR_COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            argOrdinals,
            filterArg,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel,
            null,
            null);

    return oldAggRel.getCluster().getRexBuilder().addAggCall(countArgAggCall,
        oldAggRel.getGroupCount(),
        newCalls,
        aggCallMapping,
        operandTypes);
  }

  private RexNode reduceRegrSzz(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs,
      int xIndex,
      int yIndex,
      int nullFilterIndex) {
    // regr_sxx(x, y) ==>
    //    sum(y * y, x) - sum(y, x) * sum(y, x) / regr_count(x, y)
    //

    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final RelDataType argXType = getFieldType(oldAggRel.getInput(), xIndex);
    final RelDataType argYType =
        xIndex == yIndex ? argXType : getFieldType(oldAggRel.getInput(), yIndex);
    final RelDataType nullFilterIndexType =
        nullFilterIndex == yIndex ? argYType : getFieldType(oldAggRel.getInput(), yIndex);

    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(),
            argXType.isNullable() || argYType.isNullable() || nullFilterIndexType.isNullable());

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
        ImmutableIntList.of(xIndex), ImmutableList.of(argXType), argXAndYNotNullFilterOrdinal);

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

  private RexNode reduceCovariance(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      boolean biased,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // covar_pop(x, y) ==>
    //     (sum(x * y) - sum(x) * sum(y) / regr_count(x, y))
    //     / regr_count(x, y)
    //
    // covar_samp(x, y) ==>
    //     (sum(x * y) - sum(x) * sum(y) / regr_count(x, y))
    //     / regr_count(count(x, y) - 1, 0)
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
    final int argXOrdinal = oldCall.getArgList().get(0);
    final int argYOrdinal = oldCall.getArgList().get(1);
    final RelDataType argXOrdinalType = getFieldType(oldAggRel.getInput(), argXOrdinal);
    final RelDataType argYOrdinalType = getFieldType(oldAggRel.getInput(), argYOrdinal);
    final RelDataType oldCallType = typeFactory.createTypeWithNullability(oldCall.getType(),
        argXOrdinalType.isNullable() || argYOrdinalType.isNullable());
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
        ImmutableList.of(argXOrdinalType, argYOrdinalType),
        argXAndYNotNullFilterOrdinal);
    final RexNode result = divide(biased, rexBuilder, sumXY, sumXSumY, countArg);
    return rexBuilder.makeCast(oldCall.getType(), result);
  }

  private RexNode divide(boolean biased, RexBuilder rexBuilder, RexNode sumXY,
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
   * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
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
        relBuilder.groupKey(oldAggregate.getGroupSet(),
            oldAggregate.getGroupSets()),
        newCalls);
  }

  /**
   * Add a calc with the expressions to compute the original agg calls from the
   * decomposed ones.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param rowType The output row type of the original aggregate.
   * @param exprs The expressions to compute the original agg calls.
   */
  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }

  private RelDataType getFieldType(RelNode relNode, int i) {
    final RelDataTypeField inputField =
        relNode.getRowType().getFieldList().get(i);
    return inputField.getType();
  }
}

// End AggregateReduceFunctionsRule.java
