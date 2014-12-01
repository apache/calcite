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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Call to an aggFunction function within an
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 */
public class AggregateCall {
  //~ Instance fields --------------------------------------------------------

  private final SqlAggFunction aggFunction;

  private final boolean distinct;
  public final RelDataType type;
  public final String name;

  // We considered using ImmutableIntList but we would not save much memory:
  // since all values are small, ImmutableList uses cached Integer values.
  private final ImmutableList<Integer> argList;

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
  public AggregateCall(
      SqlAggFunction aggFunction,
      boolean distinct,
      List<Integer> argList,
      RelDataType type,
      String name) {
    this.type = type;
    this.name = name;
    assert aggFunction != null;
    assert argList != null;
    assert type != null;
    this.aggFunction = aggFunction;

    this.argList = ImmutableList.copyOf(argList);
    this.distinct = distinct;
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates an AggregateCall, inferring its type if {@code type} is null. */
  public static AggregateCall create(SqlAggFunction aggFunction,
      boolean distinct, List<Integer> argList, int groupCount, RelNode input,
      RelDataType type, String name) {
    if (type == null) {
      final RelDataTypeFactory typeFactory =
          input.getCluster().getTypeFactory();
      final List<RelDataType> types =
          SqlTypeUtil.projectTypes(input.getRowType(), argList);
      final Aggregate.AggCallBinding callBinding =
          new Aggregate.AggCallBinding(typeFactory, aggFunction, types,
              groupCount);
      type = aggFunction.inferReturnType(callBinding);
    }
    return new AggregateCall(aggFunction, distinct, argList, type, name);
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
   * Returns the aggregate function.
   *
   * @return aggregate function
   */
  public final SqlAggFunction getAggregation() {
    return aggFunction;
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
    // no need to copy argList - already immutable
    return new AggregateCall(aggFunction, distinct, argList, type, name);
  }

  public String toString() {
    StringBuilder buf = new StringBuilder(aggFunction.getName());
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
    return buf.toString();
  }

  // override Object
  public boolean equals(Object o) {
    if (!(o instanceof AggregateCall)) {
      return false;
    }
    AggregateCall other = (AggregateCall) o;
    return aggFunction.equals(other.aggFunction)
        && (distinct == other.distinct)
        && argList.equals(other.argList);
  }

  // override Object
  public int hashCode() {
    return aggFunction.hashCode() + argList.hashCode();
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
        aggregateRelBase.getCluster().getTypeFactory(),
        (SqlAggFunction) aggFunction,
        SqlTypeUtil.projectTypes(rowType, argList),
        aggregateRelBase.getGroupCount());
  }

  /**
   * Creates an equivalent AggregateCall with new argument ordinals.
   *
   * @param args Arguments
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  public AggregateCall copy(List<Integer> args) {
    return new AggregateCall(aggFunction, distinct, args, type, name);
  }

  /**
   * Creates equivalent AggregateCall that is adapted to a new input types
   * and/or number of columns in GROUP BY.
   *
   * @param input relation that will be used as a child of aggregate
   * @param aggArgs argument indices of the new call in the input
   * @param oldGroupKeyCount number of columns in GROUP BY of old aggregate
   * @param newGroupKeyCount number of columns in GROUP BY of new aggregate
   * @return AggregateCall that suits new inputs and GROUP BY columns
   */
  public AggregateCall adaptTo(RelNode input, List<Integer> aggArgs,
      int oldGroupKeyCount, int newGroupKeyCount) {
    final SqlAggFunction sqlAgg = (SqlAggFunction) aggFunction;
    // The return type of aggregate call need to be recomputed.
    // Since it might depend on the number of columns in GROUP BY.
    final RelDataType newType =
        oldGroupKeyCount == newGroupKeyCount ? type : null;
    return create(sqlAgg, distinct, aggArgs, newGroupKeyCount, input, newType,
        getName());
  }

  /** Creates a copy of this aggregate call, applying a mapping to its
   * arguments. */
  public AggregateCall transform(Mappings.TargetMapping mapping) {
    return copy(Mappings.permute(argList, mapping));
  }
}

// End AggregateCall.java
