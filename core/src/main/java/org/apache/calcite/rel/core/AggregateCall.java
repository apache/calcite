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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Call to an aggregate function within an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateCall {
  //~ Instance fields --------------------------------------------------------

  private final SqlAggFunction aggFunction;

  private final boolean distinct;
  private final boolean approximate;
  private final boolean ignoreNulls;
  public final RelDataType type;
  public final @Nullable String name;

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
    this(aggFunction, distinct, false, false,
        argList, -1, null, RelCollations.EMPTY, type, name);
  }

  /**
   * Creates an AggregateCall.
   *
   * @param aggFunction Aggregate function
   * @param distinct    Whether distinct
   * @param approximate Whether approximate
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
  private AggregateCall(SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, boolean ignoreNulls, List<Integer> argList,
      int filterArg, @Nullable ImmutableBitSet distinctKeys,
      RelCollation collation, RelDataType type, @Nullable String name) {
    this.type = Objects.requireNonNull(type, "type");
    this.name = name;
    this.aggFunction = Objects.requireNonNull(aggFunction, "aggFunction");
    this.argList = ImmutableList.copyOf(argList);
    this.distinctKeys = distinctKeys;
    this.filterArg = filterArg;
    this.collation = Objects.requireNonNull(collation, "collation");
    this.distinct = distinct;
    this.approximate = approximate;
    this.ignoreNulls = ignoreNulls;
    Preconditions.checkArgument(
        aggFunction.getDistinctOptionality() != Optionality.IGNORED || !distinct,
        "DISTINCT has no effect for this aggregate function, so must be false");
    Preconditions.checkArgument(filterArg < 0 || aggFunction.allowsFilter());
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int groupCount, RelNode input,
      @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, false, false, argList, -1,
        null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, false, false, argList, filterArg,
        null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false, argList,
        filterArg, null, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        null, collation, groupCount, input, type, name);
  }

  /** Creates an AggregateCall, inferring its type if {@code type} is null. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      int groupCount,
      RelNode input, @Nullable RelDataType type, @Nullable String name) {
    if (type == null) {
      final RelDataTypeFactory typeFactory =
          input.getCluster().getTypeFactory();
      final List<RelDataType> types =
          SqlTypeUtil.projectTypes(input.getRowType(), argList);
      final Aggregate.AggCallBinding callBinding =
          new Aggregate.AggCallBinding(typeFactory, aggFunction, types,
              groupCount, filterArg >= 0);
      type = aggFunction.inferReturnType(callBinding);
    }
    return create(aggFunction, distinct, approximate, ignoreNulls, argList,
        filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, RelDataType type,
      @Nullable String name) {
    return create(aggFunction, distinct, false, false, argList, filterArg, null,
        RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        null, RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        null, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg, RelCollation collation,
      RelDataType type, @Nullable String name) {
    return create(aggFunction, distinct, approximate, ignoreNulls, argList,
        filterArg, null, collation, type, name);
  }

  /** Creates an AggregateCall. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation,
      RelDataType type, @Nullable String name) {
    final boolean distinct2 = distinct
        && (aggFunction.getDistinctOptionality() != Optionality.IGNORED);
    return new AggregateCall(aggFunction, distinct2, approximate, ignoreNulls,
        argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
  }

  /** Withs {@link #distinctKeys}. */
  public AggregateCall withDistinctKeys(
      @Nullable ImmutableBitSet distinctKeys) {
    return Objects.equals(distinctKeys, this.distinctKeys) ? this
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
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
      buf.append((argList.size() == 0) ? "DISTINCT" : "DISTINCT ");
    }
    int i = -1;
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
    if (!collation.equals(RelCollations.EMPTY)) {
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

  /** Withs {@link #filterArg}. */
  public AggregateCall withFilter(int filterArg) {
    return filterArg == this.filterArg ? this
        : new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
            argList, filterArg, distinctKeys, collation, type, name);
  }

  @Override public boolean equals(@Nullable Object o) {
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
    return Objects.hash(aggFunction, distinct, approximate, ignoreNulls,
        argList, filterArg, distinctKeys, collation);
  }

  /**
   * Creates a binding of this call in the context of an
   * {@link org.apache.calcite.rel.logical.LogicalAggregate},
   * which can then be used to infer the return type.
   */
  public Aggregate.AggCallBinding createBinding(
      Aggregate aggregateRelBase) {
    final RelDataType rowType = aggregateRelBase.getInput().getRowType();

    return new Aggregate.AggCallBinding(
        aggregateRelBase.getCluster().getTypeFactory(), aggFunction,
        SqlTypeUtil.projectTypes(rowType, argList),
        aggregateRelBase.getGroupCount(), hasFilter());
  }

  /**
   * Creates an equivalent AggregateCall with new argument ordinals.
   *
   * @see #transform(Mappings.TargetMapping)
   *
   * @param args Arguments
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args, int filterArg,
      @Nullable ImmutableBitSet distinctKeys, RelCollation collation) {
    return new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
        args, filterArg, distinctKeys, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args, int filterArg,
      RelCollation collation) {
    // ignoring distinctKeys is error-prone
    return copy(args, filterArg, distinctKeys, collation);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args, int filterArg) {
    // ignoring distinctKeys, collation is error-prone
    return copy(args, filterArg, distinctKeys, collation);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args) {
    // ignoring filterArg, distinctKeys, collation is error-prone
    return copy(args, filterArg, distinctKeys, collation);
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
    return create(aggFunction, distinct, approximate, ignoreNulls, argList,
        filterArg, distinctKeys, collation,
        newGroupKeyCount, input, newType, getName());
  }

  /** Creates a copy of this aggregate call, applying a mapping to its
   * arguments. */
  public AggregateCall transform(Mappings.TargetMapping mapping) {
    return copy(Mappings.apply2((Mapping) mapping, argList),
        hasFilter() ? Mappings.apply(mapping, filterArg) : -1,
        distinctKeys == null ? null : distinctKeys.permute(mapping),
        RelCollations.permute(collation, mapping));
  }
}
