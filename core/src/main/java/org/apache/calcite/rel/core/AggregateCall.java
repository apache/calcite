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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
  public final String name;

  // We considered using ImmutableIntList but we would not save much memory:
  // since all values are small, ImmutableList uses cached Integer values.
  private final ImmutableList<Integer> argList;
  public final int filterArg;
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
        argList, -1, RelCollations.EMPTY, type, name);
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
   * @param collation   How to sort values before aggregation (the
   *                    {@code WITHIN GROUP} clause in SQL)
   * @param type        Result type
   * @param name        Name (may be null)
   */
  private AggregateCall(SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, boolean ignoreNulls, List<Integer> argList,
      int filterArg, RelCollation collation, RelDataType type, String name) {
    this.type = Objects.requireNonNull(type);
    this.name = name;
    this.aggFunction = Objects.requireNonNull(aggFunction);
    this.argList = ImmutableList.copyOf(argList);
    this.filterArg = filterArg;
    this.collation = Objects.requireNonNull(collation);
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
      RelDataType type, String name) {
    return create(aggFunction, distinct, false, false, argList, -1,
        RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, int groupCount,
      RelNode input, RelDataType type, String name) {
    return create(aggFunction, distinct, false, false, argList, filterArg,
        RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, int groupCount,
      RelNode input, RelDataType type, String name) {
    return create(aggFunction, distinct, approximate, false, argList,
        filterArg, RelCollations.EMPTY, groupCount, input, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, int groupCount,
      RelNode input, RelDataType type, String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        collation, groupCount, input, type, name);
  }

  /** Creates an AggregateCall, inferring its type if {@code type} is null. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg, RelCollation collation,
      int groupCount,
      RelNode input, RelDataType type, String name) {
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
        filterArg, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int filterArg, RelDataType type,
      String name) {
    return create(aggFunction, distinct, false, false, argList, filterArg,
        RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelDataType type, String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        RelCollations.EMPTY, type, name);
  }

  @Deprecated // to be removed before 2.0
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, List<Integer> argList,
      int filterArg, RelCollation collation, RelDataType type, String name) {
    return create(aggFunction, distinct, approximate, false, argList, filterArg,
        collation, type, name);
  }

  /** Creates an AggregateCall. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, boolean approximate, boolean ignoreNulls,
      List<Integer> argList, int filterArg, RelCollation collation,
      RelDataType type, String name) {
    final boolean distinct2 = distinct
        && (aggFunction.getDistinctOptionality() != Optionality.IGNORED);
    return new AggregateCall(aggFunction, distinct2, approximate, ignoreNulls,
        argList, filterArg, collation, type, name);
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

  /**
   * Returns whether this AggregateCall is approximate, as in <code>
   * APPROX_COUNT_DISTINCT(empno)</code>.
   *
   * @return whether approximate
   */
  public final boolean isApproximate() {
    return approximate;
  }

  /**
   * Returns whether this AggregateCall ignores nulls.
   *
   * @return whether ignore nulls
   */
  public final boolean ignoreNulls() {
    return ignoreNulls;
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
  public String getName() {
    return name;
  }

  /**
   * Creates an equivalent AggregateCall that has a new name.
   *
   * @param name New name (may be null)
   */
  public AggregateCall rename(String name) {
    if (Objects.equals(this.name, name)) {
      return this;
    }
    return new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
        argList,
        filterArg, RelCollations.EMPTY, type,
        name);
  }

  public String toString() {
    StringBuilder buf = new StringBuilder(aggFunction.toString());
    buf.append("(");
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

  @Override public boolean equals(Object o) {
    if (!(o instanceof AggregateCall)) {
      return false;
    }
    AggregateCall other = (AggregateCall) o;
    return aggFunction.equals(other.aggFunction)
        && (distinct == other.distinct)
        && (approximate == other.approximate)
        && (ignoreNulls == other.ignoreNulls)
        && argList.equals(other.argList)
        && filterArg == other.filterArg
        && Objects.equals(collation, other.collation);
  }

  @Override public int hashCode() {
    return Objects.hash(aggFunction,
            distinct,
            approximate,
            ignoreNulls,
            argList,
            filterArg,
            collation);
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
  public AggregateCall copy(List<Integer> args, int filterArg,
      RelCollation collation) {
    return new AggregateCall(aggFunction, distinct, approximate, ignoreNulls,
        args, filterArg, collation, type, name);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args, int filterArg) {
    // ignoring collation is error-prone
    return copy(args, filterArg, collation);
  }

  @Deprecated // to be removed before 2.0
  public AggregateCall copy(List<Integer> args) {
    // ignoring filterArg and collation is error-prone
    return copy(args, filterArg, collation);
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
        filterArg, collation, newGroupKeyCount, input, newType, getName());
  }

  /** Creates a copy of this aggregate call, applying a mapping to its
   * arguments. */
  public AggregateCall transform(Mappings.TargetMapping mapping) {
    return copy(Mappings.apply2((Mapping) mapping, argList),
        hasFilter() ? Mappings.apply(mapping, filterArg) : -1,
        RelCollations.permute(collation, mapping));
  }
}

// End AggregateCall.java
