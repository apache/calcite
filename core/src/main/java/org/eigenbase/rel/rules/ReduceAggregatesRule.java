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
package org.eigenbase.rel.rules;

import java.math.BigDecimal;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * Rule to reduce aggregates to simpler forms. Currently only AVG(x) to
 * SUM(x)/COUNT(x), but eventually will handle others such as STDDEV.
 */
public class ReduceAggregatesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The singleton.
   */
  public static final ReduceAggregatesRule INSTANCE =
      new ReduceAggregatesRule(operand(AggregateRel.class, any()));

  //~ Constructors -----------------------------------------------------------

  protected ReduceAggregatesRule(RelOptRuleOperand operand) {
    super(operand);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    AggregateRelBase oldAggRel = (AggregateRelBase) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  public void onMatch(RelOptRuleCall ruleCall) {
    AggregateRelBase oldAggRel = (AggregateRelBase) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    for (AggregateCall call : aggCallList) {
      if (call.getAggregation() instanceof SqlAvgAggFunction
          || call.getAggregation() instanceof SqlSumAggFunction) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reduces all calls to AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP in
   * the aggregates list to.
   *
   * <p>It handles newly generated common subexpressions since this was done
   * at the sql2rel stage.
   */
  private void reduceAggs(
      RelOptRuleCall ruleCall,
      AggregateRelBase oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int nGroups = oldAggRel.getGroupCount();

    List<AggregateCall> newCalls = new ArrayList<AggregateCall>();
    Map<AggregateCall, RexNode> aggCallMapping =
        new HashMap<AggregateCall, RexNode>();

    List<RexNode> projList = new ArrayList<RexNode>();

    // pass through group key
    for (int i = 0; i < nGroups; ++i) {
      projList.add(
          rexBuilder.makeInputRef(
              getFieldType(oldAggRel, i),
              i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    RelNode input = oldAggRel.getChild();
    List<RexNode> inputExprs = new ArrayList<RexNode>();
    for (RelDataTypeField field : input.getRowType().getFieldList()) {
      inputExprs.add(
          rexBuilder.makeInputRef(
              field.getType(), inputExprs.size()));
    }

    // create new agg function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(
              oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount =
        inputExprs.size() - input.getRowType().getFieldCount();
    if (extraArgCount > 0) {
      input =
          RelOptUtil.createProject(
              input,
              inputExprs,
              CompositeList.of(
                  input.getRowType().getFieldNames(),
                  Collections.<String>nCopies(
                      extraArgCount,
                      null)));
    }
    AggregateRelBase newAggRel =
        newAggregateRel(
            oldAggRel, input, newCalls);

    RelNode projectRel =
        RelOptUtil.createProject(
            newAggRel,
            projList,
            oldAggRel.getRowType().getFieldNames());

    ruleCall.transformTo(projectRel);
  }

  private RexNode reduceAgg(
      AggregateRelBase oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    if (oldCall.getAggregation() instanceof SqlSumAggFunction) {
      // replace original SUM(x) with
      // case COUNT(x) when 0 then null else SUM0(x) end
      return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
    }
    if (oldCall.getAggregation() instanceof SqlAvgAggFunction) {
      final SqlAvgAggFunction.Subtype subtype =
          ((SqlAvgAggFunction) oldCall.getAggregation()).getSubtype();
      switch (subtype) {
      case AVG:
        // replace original AVG(x) with SUM(x) / COUNT(x)
        return reduceAvg(
            oldAggRel, oldCall, newCalls, aggCallMapping);
      case STDDEV_POP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x))
        return reduceStddev(
            oldAggRel, oldCall, true, true, newCalls, aggCallMapping,
            inputExprs);
      case STDDEV_SAMP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
        return reduceStddev(
            oldAggRel, oldCall, false, true, newCalls, aggCallMapping,
            inputExprs);
      case VAR_POP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x)
        return reduceStddev(
            oldAggRel, oldCall, true, false, newCalls, aggCallMapping,
            inputExprs);
      case VAR_SAMP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
        return reduceStddev(
            oldAggRel, oldCall, false, false, newCalls, aggCallMapping,
            inputExprs);
      default:
        throw Util.unexpected(subtype);
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      List<RelDataType> oldArgTypes = SqlTypeUtil
          .projectTypes(oldAggRel.getRowType(), oldCall.getArgList());
      return rexBuilder.addAggCall(
          oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldArgTypes);
    }
  }

  private RexNode reduceAvg(
      AggregateRelBase oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int iAvgInput = oldCall.getArgList().get(0);
    RelDataType avgInputType =
        getFieldType(
            oldAggRel.getChild(),
            iAvgInput);
    RelDataType sumType =
        typeFactory.createTypeWithNullability(
            avgInputType,
            avgInputType.isNullable() || nGroups == 0);
    SqlAggFunction sumAgg = new SqlSumAggFunction(sumType);
    AggregateCall sumCall =
        new AggregateCall(
            sumAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            sumType,
            null);
    SqlAggFunction countAgg = SqlStdOperatorTable.COUNT;
    RelDataType countType = countAgg.getReturnType(typeFactory);
    AggregateCall countCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode numeratorRef =
        rexBuilder.addAggCall(
            sumCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));
    RexNode denominatorRef =
        rexBuilder.addAggCall(
            countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));
    final RexNode divideRef =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE,
            numeratorRef,
            denominatorRef);
    return rexBuilder.makeCast(
        oldCall.getType(), divideRef);
  }

  private RexNode reduceSum(
      AggregateRelBase oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int arg = oldCall.getArgList().get(0);
    RelDataType argType =
        getFieldType(
            oldAggRel.getChild(),
            arg);
    RelDataType sumType =
        typeFactory.createTypeWithNullability(
            argType, argType.isNullable());
    SqlAggFunction sumZeroAgg = new SqlSumEmptyIsZeroAggFunction(sumType);
    AggregateCall sumZeroCall =
        new AggregateCall(
            sumZeroAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            sumType,
            null);
    SqlAggFunction countAgg = SqlStdOperatorTable.COUNT;
    RelDataType countType = countAgg.getReturnType(typeFactory);
    AggregateCall countCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode sumZeroRef =
        rexBuilder.addAggCall(
            sumZeroCall,
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
        rexBuilder.addAggCall(
            countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
        rexBuilder.constantNull(),
        sumZeroRef);
  }

  private RexNode reduceStddev(
      AggregateRelBase oldAggRel,
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
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    assert oldCall.getArgList().size() == 1 : oldCall.getArgList();
    final int argOrdinal = oldCall.getArgList().get(0);
    final RelDataType argType =
        getFieldType(
            oldAggRel.getChild(),
            argOrdinal);

    final RexNode argRef = inputExprs.get(argOrdinal);
    final RexNode argSquared =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, argRef, argRef);
    final int argSquaredOrdinal = lookupOrAdd(inputExprs, argSquared);

    final RelDataType sumType =
        typeFactory.createTypeWithNullability(
            argType,
            true);
    final AggregateCall sumArgSquaredAggCall =
        new AggregateCall(
            new SqlSumAggFunction(sumType),
            oldCall.isDistinct(),
            ImmutableIntList.of(argSquaredOrdinal),
            sumType,
            null);
    final RexNode sumArgSquared =
        rexBuilder.addAggCall(
            sumArgSquaredAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));

    final AggregateCall sumArgAggCall =
        new AggregateCall(
            new SqlSumAggFunction(sumType),
            oldCall.isDistinct(),
            ImmutableIntList.of(argOrdinal),
            sumType,
            null);
    final RexNode sumArg =
        rexBuilder.addAggCall(
            sumArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));

    final RexNode sumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, sumArg, sumArg);

    final SqlAggFunction countAgg = SqlStdOperatorTable.COUNT;
    final RelDataType countType = countAgg.getReturnType(typeFactory);
    final AggregateCall countArgAggCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);
    final RexNode countArg =
        rexBuilder.addAggCall(
            countArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));

    final RexNode avgSumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE,
            sumSquaredArg, countArg);

    final RexNode diff =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            sumArgSquared, avgSumSquaredArg);

    final RexNode denominator;
    if (biased) {
      denominator = countArg;
    } else {
      final RexLiteral one =
          rexBuilder.makeExactLiteral(BigDecimal.ONE);
      final RexNode nul =
          rexBuilder.makeNullLiteral(countArg.getType().getSqlTypeName());
      final RexNode countMinusOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.MINUS, countArg, one);
      final RexNode countEqOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS, countArg, one);
      denominator =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              countEqOne, nul, countMinusOne);
    }

    final RexNode div =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE, diff, denominator);

    RexNode result = div;
    if (sqrt) {
      final RexNode half =
          rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
      result =
          rexBuilder.makeCall(
              SqlStdOperatorTable.POWER, div, half);
    }

    return rexBuilder.makeCast(
        oldCall.getType(), result);
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
   * into AggregateRelBase and subclasses - but it's only needed for some
   * subclasses.
   *
   * @param oldAggRel AggregateRel to clone.
   * @param inputRel  Input relational expression
   * @param newCalls  New list of AggregateCalls
   * @return shallow clone with new list of AggregateCalls.
   */
  protected AggregateRelBase newAggregateRel(
      AggregateRelBase oldAggRel,
      RelNode inputRel,
      List<AggregateCall> newCalls) {
    return new AggregateRel(
        oldAggRel.getCluster(),
        inputRel,
        oldAggRel.getGroupSet(),
        newCalls);
  }

  private RelDataType getFieldType(RelNode relNode, int i) {
    final RelDataTypeField inputField =
        relNode.getRowType().getFieldList().get(i);
    return inputField.getType();
  }
}

// End ReduceAggregatesRule.java
