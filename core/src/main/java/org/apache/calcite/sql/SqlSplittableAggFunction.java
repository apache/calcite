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
package org.apache.calcite.sql;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate function that can be split into partial aggregates.
 *
 * <p>For example, {@code COUNT(x)} can be split into {@code COUNT(x)} on
 * subsets followed by {@code SUM} to combine those counts.
 */
public interface SqlSplittableAggFunction {
  AggregateCall split(AggregateCall aggregateCall,
      Mappings.TargetMapping mapping);

  /** Called to generate an aggregate for the other side of the join
   * than the side aggregate call's arguments come from. Returns null if
   * no aggregate is required. */
  AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e);

  /** Generates an aggregate call to merge sub-totals.
   *
   * <p>Most implementations will add a single aggregate call to
   * {@code aggCalls}, and return a {@link RexInputRef} that points to it.
   *
   * @param rexBuilder Rex builder
   * @param extra Place to define extra input expressions
   * @param offset Offset due to grouping columns (and indicator columns if
   *     applicable)
   * @param inputRowType Input row type
   * @param aggregateCall Source aggregate call
   * @param leftSubTotal Ordinal of the sub-total coming from the left side of
   *     the join, or -1 if there is no such sub-total
   * @param rightSubTotal Ordinal of the sub-total coming from the right side
   *     of the join, or -1 if there is no such sub-total
   *
   * @return Aggregate call
   */
  AggregateCall topSplit(RexBuilder rexBuilder, Registry<RexNode> extra,
      int offset, RelDataType inputRowType, AggregateCall aggregateCall,
      int leftSubTotal, int rightSubTotal);

  /** Generates an expression for the value of the aggregate function when
   * applied to a single row.
   *
   * <p>For example, if there is one row:
   * <ul>
   *   <li>{@code SUM(x)} is {@code x}
   *   <li>{@code MIN(x)} is {@code x}
   *   <li>{@code MAX(x)} is {@code x}
   *   <li>{@code COUNT(x)} is {@code CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END 1}
   *   which can be simplified to {@code 1} if {@code x} is never null
   *   <li>{@code COUNT(*)} is 1
   * </ul>
   *
   * @param rexBuilder Rex builder
   * @param inputRowType Input row type
   * @param aggregateCall Aggregate call
   *
   * @return Expression for single row
   */
  RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
      AggregateCall aggregateCall);

  /**
   * Merge top and bottom aggregate calls into a single aggregate call,
   * if they are legit to merge.
   *
   * <p>SUM of SUM becomes SUM; SUM of COUNT becomes COUNT;
   * MAX of MAX becomes MAX; MIN of MIN becomes MIN.
   * AVG of AVG would not match, nor would COUNT of COUNT.
   *
   * @param top top aggregate call
   * @param bottom bottom aggregate call
   * @return Merged aggregate call, null if fails to merge aggregate calls
   */
  AggregateCall merge(AggregateCall top, AggregateCall bottom);

  /** Collection in which one can register an element. Registering may return
   * a reference to an existing element.
   *
   * @param <E> element type */
  interface Registry<E> {
    int register(E e);
  }

  /** Splitting strategy for {@code COUNT}.
   *
   * <p>COUNT splits into itself followed by SUM. (Actually
   * SUM0, because the total needs to be 0, not null, if there are 0 rows.)
   * This rule works for any number of arguments to COUNT, including COUNT(*).
   */
  class CountSplitter implements SqlSplittableAggFunction {
    public static final CountSplitter INSTANCE = new CountSplitter();

    public AggregateCall split(AggregateCall aggregateCall,
        Mappings.TargetMapping mapping) {
      return aggregateCall.transform(mapping);
    }

    public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
      return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
          false, ImmutableIntList.of(), -1, RelCollations.EMPTY,
          typeFactory.createSqlType(SqlTypeName.BIGINT), null);
    }

    public AggregateCall topSplit(RexBuilder rexBuilder,
        Registry<RexNode> extra, int offset, RelDataType inputRowType,
        AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal) {
      final List<RexNode> merges = new ArrayList<>();
      if (leftSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, leftSubTotal));
      }
      if (rightSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, rightSubTotal));
      }
      RexNode node;
      switch (merges.size()) {
      case 1:
        node = merges.get(0);
        break;
      case 2:
        node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
        break;
      default:
        throw new AssertionError("unexpected count " + merges);
      }
      int ordinal = extra.register(node);
      return AggregateCall.create(SqlStdOperatorTable.SUM0, false, false,
          false, ImmutableList.of(ordinal), -1, aggregateCall.collation,
          aggregateCall.type, aggregateCall.name);
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code COUNT(*)}, and {@code COUNT} applied to all NOT NULL arguments,
     * become {@code 1}; otherwise
     * {@code CASE WHEN arg0 IS NOT NULL THEN 1 ELSE 0 END}.
     */
    public RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
        AggregateCall aggregateCall) {
      final List<RexNode> predicates = new ArrayList<>();
      for (Integer arg : aggregateCall.getArgList()) {
        final RelDataType type = inputRowType.getFieldList().get(arg).getType();
        if (type.isNullable()) {
          predicates.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                  rexBuilder.makeInputRef(type, arg)));
        }
      }
      final RexNode predicate =
          RexUtil.composeConjunction(rexBuilder, predicates, true);
      final RexNode rexOne = rexBuilder.makeExactLiteral(
          BigDecimal.ONE, aggregateCall.getType());
      if (predicate == null) {
        return rexOne;
      } else {
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE, predicate, rexOne,
            rexBuilder.makeExactLiteral(BigDecimal.ZERO, aggregateCall.getType()));
      }
    }

    public AggregateCall merge(AggregateCall top, AggregateCall bottom) {
      if (bottom.getAggregation().getKind() == SqlKind.COUNT
          && top.getAggregation().getKind() == SqlKind.SUM) {
        return AggregateCall.create(bottom.getAggregation(),
            bottom.isDistinct(), bottom.isApproximate(), false,
            bottom.getArgList(), bottom.filterArg, bottom.getCollation(),
            bottom.getType(), top.getName());
      } else {
        return null;
      }
    }
  }

  /** Aggregate function that splits into two applications of itself.
   *
   * <p>Examples are MIN and MAX. */
  class SelfSplitter implements SqlSplittableAggFunction {
    public static final SelfSplitter INSTANCE = new SelfSplitter();

    public RexNode singleton(RexBuilder rexBuilder,
        RelDataType inputRowType, AggregateCall aggregateCall) {
      final int arg = aggregateCall.getArgList().get(0);
      final RelDataTypeField field = inputRowType.getFieldList().get(arg);
      return rexBuilder.makeInputRef(field.getType(), arg);
    }

    public AggregateCall split(AggregateCall aggregateCall,
        Mappings.TargetMapping mapping) {
      return aggregateCall.transform(mapping);
    }

    public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
      return null; // no aggregate function required on other side
    }

    public AggregateCall topSplit(RexBuilder rexBuilder,
        Registry<RexNode> extra, int offset, RelDataType inputRowType,
        AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal) {
      assert (leftSubTotal >= 0) != (rightSubTotal >= 0);
      assert aggregateCall.collation.getFieldCollations().isEmpty();
      final int arg = leftSubTotal >= 0 ? leftSubTotal : rightSubTotal;
      return aggregateCall.copy(ImmutableIntList.of(arg), -1,
          RelCollations.EMPTY);
    }

    public AggregateCall merge(AggregateCall top, AggregateCall bottom) {
      if (top.getAggregation().getKind() == bottom.getAggregation().getKind()) {
        return AggregateCall.create(bottom.getAggregation(),
            bottom.isDistinct(), bottom.isApproximate(), false,
            bottom.getArgList(), bottom.filterArg, bottom.getCollation(),
            bottom.getType(), top.getName());
      } else {
        return null;
      }
    }
  }

  /** Common splitting strategy for {@code SUM} and {@code SUM0} functions. */
  abstract class AbstractSumSplitter implements SqlSplittableAggFunction {

    public RexNode singleton(RexBuilder rexBuilder,
        RelDataType inputRowType, AggregateCall aggregateCall) {
      final int arg = aggregateCall.getArgList().get(0);
      final RelDataTypeField field = inputRowType.getFieldList().get(arg);
      return rexBuilder.makeInputRef(field.getType(), arg);
    }

    public AggregateCall split(AggregateCall aggregateCall,
        Mappings.TargetMapping mapping) {
      return aggregateCall.transform(mapping);
    }

    public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
      return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
          false,
          ImmutableIntList.of(), -1,
          RelCollations.EMPTY,
          typeFactory.createSqlType(SqlTypeName.BIGINT), null);
    }

    public AggregateCall topSplit(RexBuilder rexBuilder,
        Registry<RexNode> extra, int offset, RelDataType inputRowType,
        AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal) {
      final List<RexNode> merges = new ArrayList<>();
      final List<RelDataTypeField> fieldList = inputRowType.getFieldList();
      if (leftSubTotal >= 0) {
        final RelDataType type = fieldList.get(leftSubTotal).getType();
        merges.add(rexBuilder.makeInputRef(type, leftSubTotal));
      }
      if (rightSubTotal >= 0) {
        final RelDataType type = fieldList.get(rightSubTotal).getType();
        merges.add(rexBuilder.makeInputRef(type, rightSubTotal));
      }
      RexNode node;
      switch (merges.size()) {
      case 1:
        node = merges.get(0);
        break;
      case 2:
        node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
        node = rexBuilder.makeAbstractCast(aggregateCall.type, node);
        break;
      default:
        throw new AssertionError("unexpected count " + merges);
      }
      int ordinal = extra.register(node);
      return AggregateCall.create(getMergeAggFunctionOfTopSplit(), false, false,
          false, ImmutableList.of(ordinal), -1, aggregateCall.collation,
          aggregateCall.type, aggregateCall.name);
    }

    public AggregateCall merge(AggregateCall top, AggregateCall bottom) {
      SqlKind topKind = top.getAggregation().getKind();
      if (topKind == bottom.getAggregation().getKind()
          && (topKind == SqlKind.SUM
              || topKind == SqlKind.SUM0)) {
        return AggregateCall.create(bottom.getAggregation(),
            bottom.isDistinct(), bottom.isApproximate(), false,
            bottom.getArgList(), bottom.filterArg, bottom.getCollation(),
            bottom.getType(), top.getName());
      } else {
        return null;
      }
    }

    protected abstract SqlAggFunction getMergeAggFunctionOfTopSplit();

  }

  /** Splitting strategy for {@code SUM} function. */
  class SumSplitter extends AbstractSumSplitter {
    public static final SumSplitter INSTANCE = new SumSplitter();

    @Override public SqlAggFunction getMergeAggFunctionOfTopSplit() {
      return SqlStdOperatorTable.SUM;
    }
  }

  /** Splitting strategy for {@code SUM0} function. */
  class Sum0Splitter extends AbstractSumSplitter {
    public static final Sum0Splitter INSTANCE = new Sum0Splitter();

    @Override public SqlAggFunction getMergeAggFunctionOfTopSplit() {
      return SqlStdOperatorTable.SUM0;
    }

    @Override public RexNode singleton(RexBuilder rexBuilder,
        RelDataType inputRowType, AggregateCall aggregateCall) {
      final int arg = aggregateCall.getArgList().get(0);
      final RelDataType type = inputRowType.getFieldList().get(arg).getType();
      final RexNode inputRef = rexBuilder.makeInputRef(type, arg);
      if (type.isNullable()) {
        return rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, inputRef,
            rexBuilder.makeExactLiteral(BigDecimal.ZERO, type));
      } else {
        return inputRef;
      }
    }
  }
}

// End SqlSplittableAggFunction.java
