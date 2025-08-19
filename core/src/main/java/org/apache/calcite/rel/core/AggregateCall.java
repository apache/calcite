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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Call to an aggregate function within an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateCall {
  //~ Instance fields --------------------------------------------------------

  /**
   * Some aggregate calls may produce runtime errors.  For these
   * we need to keep around the original source position information
   * so that the runtime can produce error messages pointing to
   * the offending source operation.  For "safe" aggregations
   * this field may be ZERO.
   */
  private final SqlParserPos pos;
  private final SqlAggFunction aggFunction;

  private final boolean distinct;
  private final boolean approximate;
  private final boolean ignoreNulls;
  public final RelDataType type;
  public final @Nullable String name;
  public final List<RexNode> rexList;

  // We considered using ImmutableIntList but we would not save much memory:
  // since all values are small, ImmutableList uses cached Integer values.
  private final ImmutableList<Integer> argList;
  public final int filterArg;
  public final @Nullable ImmutableBitSet distinctKeys;
  public final RelCollation collation;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregateCall.
   *
   * @param aggFunction Aggregate function
   * @param distinct    Whether distinct
   * @param argList     List of ordinals of arguments
   * @param type        Result type
   * @param name        Name (may be null)
   */
  @Deprecated // to be removed before 2.0
  public AggregateCall(
      SqlAggFunction aggFunction,
      boolean distinct,
      List<Integer> argList,
      RelDataType type,
      String name) {
    this(SqlParserPos.ZERO, aggFunction, distinct, false, false,
        ImmutableList.of(), argList, -1, null,
        RelCollations.EMPTY, type, name);
  }

  /**
   * Creates an AggregateCall.
   *
   * @param pos         Source position for this aggregate.
   *                    Ideally it should only be ZERO when the aggregate
   *                    can never fail at runtime.
   * @param aggFunction Aggregate function
   * @param distinct    Whether distinct
   * @param approximate Whether approximate
   * @param rexList     List of pre-arguments
   * @param argList     List of ordinals of arguments
   * @param filterArg   Ordinal of filter argument (the
   *                    {@code FILTER (WHERE ...)} clause in SQL), or -1
   * @param distinctKeys Ordinals of fields to make values distinct on before
   *                    aggregating, or null
   * @param collation   How to sort values before aggregation (the
   *                    {@code WITHIN GROUP} clause in SQL)
   * @param type        Result type
   * @param name        Name (may be null)
   */
  private AggregateCall(SqlParserPos pos, SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList,
      int filterArg, @Nullable ImmutableBitSet distinctKeys,
      RelCollation collation, RelDataType type, @Nullable String name) {
    this.pos = pos;
    this.type = requireNonNull(type, "type");
    this.name = name;
    this.aggFunction = requireNonNull(aggFunction, "aggFunction");
    this.argList = ImmutableList.copyOf(argList);
    this.rexList = ImmutableList.copyOf(rexList);
    this.distinctKeys = distinctKeys;
    this.filterArg = filterArg;
    this.collation = requireNonNull(collation, "collation");
    this.distinct = distinct;
    this.approximate = approximate;
    this.ignoreNulls = ignoreNulls;
    checkArgument(aggFunction.getDistinctOptionality() != Optionality.IGNORED
            || !distinct,
        "DISTINCT has no effect for this aggregate function, so must be false");
    checkArgument(filterArg < 0 || aggFunction.allowsFilter());
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int groupCount, RelNode input,
      @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, false, false,
        ImmutableList.of(), argList, -1,
        null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, false, false,
        ImmutableList.of(), argList, filterArg,
        null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false,
        ImmutableList.of(), argList,
        filterArg, null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false,
        ImmutableList.of(), argList, filterArg,
        null, collation, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, ignoreNulls,
        ImmutableList.of(), argList, filterArg,
        distinctKeys, collation, groupCount, input, type, name);
  }

  /** Creates an AggregateCall, inferring its type if {@code type} is null. */
  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(SqlParserPos.ZERO, aggFunction, distinct, approximate,
        ignoreNulls, rexList, argList, filterArg, distinctKeys, collation, groupCount,
        input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlParserPos pos, SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    if (type == null) {
      final RelDataTypeFactory typeFactory =
          input.getCluster().getTypeFactory();
      final List<RelDataType> preTypes = RexUtil.types(rexList);
      final List<RelDataType> types =
          SqlTypeUtil.projectTypes(input.getRowType(), argList);
      final Aggregate.AggCallBinding callBinding =
          new Aggregate.AggCallBinding(typeFactory, aggFunction, preTypes,
              types, groupCount, filterArg >= 0);
      type = aggFunction.inferReturnType(callBinding);
    }
    return create(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /** Creates an AggregateCall, inferring its type if {@code type} is null. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      boolean hasEmptyGroup,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(SqlParserPos.ZERO, aggFunction, distinct, approximate,
        ignoreNulls, rexList, argList, filterArg, distinctKeys, collation, hasEmptyGroup,
        input, type, name);
  }

  public static AggregateCall create(SqlParserPos pos, SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      boolean hasEmptyGroup,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    if (type == null) {
      final RelDataTypeFactory typeFactory =
          input.getCluster().getTypeFactory();
      final List<RelDataType> preTypes = RexUtil.types(rexList);
      final List<RelDataType> types =
          SqlTypeUtil.projectTypes(input.getRowType(), argList);
      final Aggregate.AggCallBinding callBinding;
      if (aggFunction.getKind() == SqlKind.PERCENTILE_DISC
          || aggFunction.getKind() == SqlKind.PERCENTILE_CONT) {
        callBinding =
            new Aggregate.PercentileDiscAggCallBinding(typeFactory, aggFunction,
                SqlTypeUtil.projectTypes(input.getRowType(), argList),
                SqlTypeUtil.projectTypes(input.getRowType(), collation.getKeys()).get(0),
                hasEmptyGroup, filterArg >= 0);
      } else {
        callBinding =
            new Aggregate.AggCallBinding(typeFactory, aggFunction, preTypes, types,
                hasEmptyGroup, filterArg >= 0);
      }
      type = aggFunction.inferReturnType(callBinding);
    }
    return create(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, RelDataType type,
      @Nullable String name) {
    return create(aggFunction, distinct, false, false,
        ImmutableList.of(), argList, filterArg, null,
        RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false,
        ImmutableList.of(), argList, filterArg,
        null, RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false,
        ImmutableList.of(), argList, filterArg,
        null, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg, RelCollation collation,
      RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, ignoreNulls,
        ImmutableList.of(), argList,
        filterArg, null, collation, type, name);
  }

  /** Creates an AggregateCall. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      RelDataType type, @Nullable String name) {
    return create(SqlParserPos.ZERO, aggFunction, distinct, approximate,
        ignoreNulls, rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  public static AggregateCall create(SqlParserPos pos, SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<RexNode> rexList, List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      RelDataType type, @Nullable String name) {
    final boolean distinct2 = distinct
        && (aggFunction.getDistinctOptionality() != Optionality.IGNORED);
    return new AggregateCall(pos, aggFunction, distinct2, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns whether this AggregateCall is distinct, as in <code>
   * COUNT(DISTINCT empno)</code>.
   *
   * @return whether distinct
   */
  public final boolean isDistinct() {
    return distinct;
  }

  /** Withs {@link #isDistinct()}. */
  public AggregateCall withDistinct(boolean distinct) {
    return distinct == this.distinct ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns whether this AggregateCall is approximate, as in <code>
   * APPROX_COUNT_DISTINCT(empno)</code>.
   *
   * @return whether approximate
   */
  public final boolean isApproximate() {
    return approximate;
  }

  /** Withs {@link #isApproximate()}. */
  public AggregateCall withApproximate(boolean approximate) {
    return approximate == this.approximate ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns whether this AggregateCall ignores nulls.
   *
   * @return whether ignore nulls
   */
  public final boolean ignoreNulls() {
    return ignoreNulls;
  }

  /** Withs {@link #ignoreNulls()}. */
  public AggregateCall withIgnoreNulls(boolean ignoreNulls) {
    return ignoreNulls == this.ignoreNulls ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns the aggregate function.
   *
   * @return aggregate function
   */
  public final SqlAggFunction getAggregation() {
    return aggFunction;
  }

  /**
   * Returns the aggregate ordering definition (the {@code WITHIN GROUP} clause
   * in SQL), or the empty list if not specified.
   *
   * @return ordering definition
   */
  public RelCollation getCollation() {
    return collation;
  }

  /** Withs {@link #getCollation()}. */
  public AggregateCall withCollation(RelCollation collation) {
    return collation.equals(this.collation) ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns the ordinals of the arguments to this call.
   *
   * <p>The list is immutable.
   *
   * @return list of argument ordinals
   */
  public final List<Integer> getArgList() {
    return argList;
  }

  /** Withs {@link #getArgList()}. */
  public AggregateCall withArgList(List<Integer> argList) {
    return argList.equals(this.argList) ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /** Withs {@link #distinctKeys}. */
  public AggregateCall withDistinctKeys(
      @Nullable ImmutableBitSet distinctKeys) {
    return Objects.equals(distinctKeys, this.distinctKeys) ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Returns the result type.
   *
   * @return result type
   */
  public final RelDataType getType() {
    return type;
  }

  /**
   * Returns the name.
   *
   * @return name
   */
  public @Nullable String getName() {
    return name;
  }

  /** Withs {@link #name}. */
  public AggregateCall withName(@Nullable String name) {
    return Objects.equals(name, this.name) ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall rename(@Nullable String name) {
    return withName(name);
  }

  @Override public String toString() {
    StringBuilder buf = new StringBuilder(aggFunction.toString());
    buf.append("(");
    if (approximate) {
      buf.append("APPROXIMATE ");
    }
    if (distinct) {
      buf.append(argList.isEmpty() ? "DISTINCT" : "DISTINCT ");
    }
    int i = -1;
    for (RexNode rexNode : rexList) {
      if (++i > 0) {
        buf.append(", ");
      }
      buf.append(rexNode);
    }
    for (Integer arg : argList) {
      if (++i > 0) {
        buf.append(", ");
      }
      buf.append("$");
      buf.append(arg);
    }
    buf.append(")");
    if (distinctKeys != null) {
      buf.append(" WITHIN DISTINCT (");
      for (Ord<Integer> key : Ord.zip(distinctKeys)) {
        buf.append(key.i > 0 ? ", $" : "$");
        buf.append(key.e);
      }
      buf.append(")");
    }
    if (hasCollation()) {
      buf.append(" WITHIN GROUP (");
      buf.append(collation);
      buf.append(")");
    }
    if (hasFilter()) {
      buf.append(" FILTER $");
      buf.append(filterArg);
    }
    return buf.toString();
  }

  /** Returns whether this AggregateCall has a filter argument. */
  public boolean hasFilter() {
    return filterArg >= 0;
  }

  /** Returns true if this AggregateCall has a non-empty collation. Returns false otherwise. */
  public boolean hasCollation() {
    return !collation.equals(RelCollations.EMPTY);
  }

  /** Withs {@link #filterArg}. */
  public AggregateCall withFilter(int filterArg) {
    return filterArg == this.filterArg ? this
        : new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
            rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  public SqlParserPos getParserPosition() {
    return this.pos;
  }

  @Override public boolean equals(@Nullable Object o) {
    // Intentionally ignore the position
    return o == this
        || o instanceof AggregateCall
        && aggFunction.equals(((AggregateCall) o).aggFunction)
        && distinct == ((AggregateCall) o).distinct
        && approximate == ((AggregateCall) o).approximate
        && ignoreNulls == ((AggregateCall) o).ignoreNulls
        && argList.equals(((AggregateCall) o).argList)
        && filterArg == ((AggregateCall) o).filterArg
        && Objects.equals(distinctKeys, ((AggregateCall) o).distinctKeys)
        && collation.equals(((AggregateCall) o).collation);
  }

  @Override public int hashCode() {
    // Ignore the position!
    return Objects.hash(aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation);
  }

  /**
   * Creates a binding of this call in the context of an
   * {@link org.apache.calcite.rel.logical.LogicalAggregate},
   * which can then be used to infer the return type.
   */
  public Aggregate.AggCallBinding createBinding(
      Aggregate aggregateRelBase) {
    final RelDataType rowType = aggregateRelBase.getInput().getRowType();
    final RelDataTypeFactory typeFactory =
        aggregateRelBase.getCluster().getTypeFactory();

    if (aggFunction.getKind() == SqlKind.PERCENTILE_DISC
        || aggFunction.getKind() == SqlKind.PERCENTILE_CONT) {
      assert collation.getKeys().size() == 1;
      return new Aggregate.PercentileDiscAggCallBinding(typeFactory,
          aggFunction, SqlTypeUtil.projectTypes(rowType, argList),
          SqlTypeUtil.projectTypes(rowType, collation.getKeys()).get(0),
          aggregateRelBase.hasEmptyGroup(), hasFilter());
    }
    return new Aggregate.AggCallBinding(typeFactory, aggFunction,
        RexUtil.types(rexList), SqlTypeUtil.projectTypes(rowType, argList),
        aggregateRelBase.hasEmptyGroup(), hasFilter());
  }

  /**
   * Creates an equivalent AggregateCall with new argument ordinals.
   *
   * @see #transform(Mappings.TargetMapping)
   *
   * @param argList Arguments
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation) {
    return new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> argList, int filterArg,
      RelCollation collation) {
    // ignoring distinctKeys is error-prone
    return new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> argList, int filterArg) {
    // ignoring distinctKeys, collation is error-prone
    return new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> argList) {
    // ignoring filterArg, distinctKeys, collation is error-prone
    return new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation, type, name);
  }

  /**
   * Creates an equivalent AggregateCall that is adapted to a new input types
   * and/or number of columns in GROUP BY.
   *
   * @param input            Relation that will be input of Aggregate
   * @param argList          Argument indices of the new call in the input
   * @param filterArg        Index of the filter, or -1
   * @param oldGroupKeyCount number of columns in GROUP BY of old aggregate
   * @param newGroupKeyCount number of columns in GROUP BY of new aggregate
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  @Deprecated // to be removed before 2.0
  public AggregateCall adaptTo(RelNode input, List<Integer> argList,
      int filterArg, int oldGroupKeyCount, int newGroupKeyCount) {
    // The return type of aggregate call need to be recomputed.
    // Since it might depend on the number of columns in GROUP BY.
    final RelDataType newType =
        oldGroupKeyCount == newGroupKeyCount
            && argList.equals(this.argList)
            && filterArg == this.filterArg
            ? type
            : null;
    return create(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation,
        newGroupKeyCount, input, newType, getName());
  }

  /**
   * Creates an equivalent AggregateCall that is adapted to a new input types
   * and/or number of columns in GROUP BY.
   *
   * @param input            Relation that will be input of Aggregate
   * @param argList          Argument indices of the new call in the input
   * @param filterArg        Index of the filter, or -1
   * @param oldHasEmptyGroup Whether old aggregate contains empty group
   * @param newHasEmptyGroup Whether new aggregate contains empty group
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  public AggregateCall adaptTo(RelNode input, List<Integer> argList,
      int filterArg, boolean oldHasEmptyGroup, boolean newHasEmptyGroup) {
    // The return type of aggregate call need to be recomputed.
    // Since it might depend on the number of columns in GROUP BY.
    final RelDataType newType =
        oldHasEmptyGroup == newHasEmptyGroup
            && argList.equals(this.argList)
            && filterArg == this.filterArg
            ? type
            : null;
    return create(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, argList, filterArg, distinctKeys, collation,
        newHasEmptyGroup, input, newType, getName());
  }

  /** Creates a copy of this aggregate call, applying a mapping to its
   * arguments. */
  public AggregateCall transform(Mappings.TargetMapping mapping) {
    return new AggregateCall(pos, aggFunction, distinct, approximate, ignoreNulls,
        rexList, Mappings.apply2((Mapping) mapping, argList),
        hasFilter() ? Mappings.apply(mapping, filterArg) : -1,
        distinctKeys == null ? null : distinctKeys.permute(mapping),
        RelCollations.permute(collation, mapping), type, name);
  }
}
