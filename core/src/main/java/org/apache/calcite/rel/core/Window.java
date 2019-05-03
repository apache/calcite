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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A Window can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link Window.Group} has a set of
 * {@link org.apache.calcite.rex.RexOver} objects.
 *
 * <p>Created by {@link org.apache.calcite.rel.rules.ProjectToWindowRule}.
 */
public abstract class Window extends SingleRel {
  public final ImmutableList<Group> groups;
  public final ImmutableList<RexLiteral> constants;

  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param input   Input relational expression
   * @param constants List of constants that are additional inputs
   * @param rowType Output row type
   * @param groups Windows
   */
  public Window(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
    super(cluster, traitSet, input);
    this.constants = ImmutableList.copyOf(constants);
    assert rowType != null;
    this.rowType = rowType;
    this.groups = ImmutableList.copyOf(groups);
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    // In the window specifications, an aggregate call such as
    // 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram.
    // (Not its projections.)
    final RelDataType childRowType = getInput().getRowType();

    final int childFieldCount = childRowType.getFieldCount();
    final int inputSize = childFieldCount + constants.size();
    final List<RelDataType> inputTypes =
        new AbstractList<RelDataType>() {
          @Override public RelDataType get(int index) {
            return index < childFieldCount
                ? childRowType.getFieldList().get(index).getType()
                : constants.get(index - childFieldCount).getType();
          }

          @Override public int size() {
            return inputSize;
          }
        };

    final RexChecker checker = new RexChecker(inputTypes, context, litmus);
    int count = 0;
    for (Group group : groups) {
      for (RexWinAggCall over : group.aggCalls) {
        ++count;
        if (!checker.isValid(over)) {
          return litmus.fail(null);
        }
      }
    }
    if (count == 0) {
      return litmus.fail("empty");
    }
    return litmus.succeed();
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<Group> window : Ord.zip(groups)) {
      pw.item("window#" + window.i, window.e.toString());
    }
    return pw;
  }

  public static ImmutableIntList getProjectOrdinals(final List<RexNode> exprs) {
    return ImmutableIntList.copyOf(
        new AbstractList<Integer>() {
          public Integer get(int index) {
            return ((RexSlot) exprs.get(index)).getIndex();
          }

          public int size() {
            return exprs.size();
          }
        });
  }

  public static RelCollation getCollation(
      final List<RexFieldCollation> collations) {
    return RelCollations.of(
        new AbstractList<RelFieldCollation>() {
          public RelFieldCollation get(int index) {
            final RexFieldCollation collation = collations.get(index);
            return new RelFieldCollation(
                ((RexLocalRef) collation.left).getIndex(),
                collation.getDirection(),
                collation.getNullDirection());
          }

          public int size() {
            return collations.size();
          }
        });
  }

  /**
   * Returns constants that are additional inputs of current relation.
   * @return constants that are additional inputs of current relation
   */
  public List<RexLiteral> getConstants() {
    return constants;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Cost is proportional to the number of rows and the number of
    // components (groups and aggregate functions). There is
    // no I/O cost.
    //
    // TODO #1. Add memory cost.
    // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
    final double rowsIn = mq.getRowCount(getInput());
    int count = groups.size();
    for (Group group : groups) {
      count += group.aggCalls.size();
    }
    return planner.getCostFactory().makeCost(rowsIn, rowsIn * count, 0);
  }

  /**
   * Group of windowed aggregate calls that have the same window specification.
   *
   * <p>The specification is defined by an upper and lower bound, and
   * also has zero or more partitioning columns.
   *
   * <p>A window is either logical or physical. A physical window is measured
   * in terms of row count. A logical window is measured in terms of rows
   * within a certain distance from the current sort key.
   *
   * <p>For example:
   *
   * <ul>
   * <li><code>ROWS BETWEEN 10 PRECEDING and 5 FOLLOWING</code> is a physical
   * window with an upper and lower bound;
   * <li><code>RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND UNBOUNDED
   * FOLLOWING</code> is a logical window with only a lower bound;
   * <li><code>RANGE INTERVAL '10' MINUTES PRECEDING</code> (which is
   * equivalent to <code>RANGE BETWEEN INTERVAL '10' MINUTES PRECEDING AND
   * CURRENT ROW</code>) is a logical window with an upper and lower bound.
   * </ul>
   */
  public static class Group {
    public final ImmutableBitSet keys;
    public final boolean isRows;
    public final RexWindowBound lowerBound;
    public final RexWindowBound upperBound;
    public final RelCollation orderKeys;
    private final String digest;

    /**
     * List of {@link Window.RexWinAggCall}
     * objects, each of which is a call to a
     * {@link org.apache.calcite.sql.SqlAggFunction}.
     */
    public final ImmutableList<RexWinAggCall> aggCalls;

    public Group(
        ImmutableBitSet keys,
        boolean isRows,
        RexWindowBound lowerBound,
        RexWindowBound upperBound,
        RelCollation orderKeys,
        List<RexWinAggCall> aggCalls) {
      assert orderKeys != null : "precondition: ordinals != null";
      assert keys != null;
      this.keys = keys;
      this.isRows = isRows;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.orderKeys = orderKeys;
      this.aggCalls = ImmutableList.copyOf(aggCalls);
      this.digest = computeString();
    }

    public String toString() {
      return digest;
    }

    private String computeString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("window(partition ");
      buf.append(keys);
      buf.append(" order by ");
      buf.append(orderKeys);
      buf.append(isRows ? " rows " : " range ");
      if (lowerBound != null) {
        if (upperBound != null) {
          buf.append("between ");
          buf.append(lowerBound);
          buf.append(" and ");
          buf.append(upperBound);
        } else {
          buf.append(lowerBound);
        }
      } else if (upperBound != null) {
        buf.append(upperBound);
      }
      buf.append(" aggs ");
      buf.append(aggCalls);
      buf.append(")");
      return buf.toString();
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj instanceof Group
          && this.digest.equals(((Group) obj).digest);
    }

    @Override public int hashCode() {
      return digest.hashCode();
    }

    public RelCollation collation() {
      return orderKeys;
    }

    /**
     * Returns if the window is guaranteed to have rows.
     * This is useful to refine data type of window aggregates.
     * For instance sum(non-nullable) over (empty window) is NULL.
     * @return true when the window is non-empty
     * @see org.apache.calcite.sql.SqlWindow#isAlwaysNonEmpty()
     * @see org.apache.calcite.sql.SqlOperatorBinding#getGroupCount()
     * @see org.apache.calcite.sql.validate.SqlValidatorImpl#resolveWindow(org.apache.calcite.sql.SqlNode, org.apache.calcite.sql.validate.SqlValidatorScope, boolean)
     */
    public boolean isAlwaysNonEmpty() {
      int lowerKey = lowerBound.getOrderKey();
      int upperKey = upperBound.getOrderKey();
      return lowerKey > -1 && lowerKey <= upperKey;
    }

    /**
     * Presents a view of the {@link RexWinAggCall} list as a list of
     * {@link AggregateCall}.
     */
    public List<AggregateCall> getAggregateCalls(Window windowRel) {
      final List<String> fieldNames =
          Util.skip(windowRel.getRowType().getFieldNames(),
              windowRel.getInput().getRowType().getFieldCount());
      return new AbstractList<AggregateCall>() {
        public int size() {
          return aggCalls.size();
        }

        public AggregateCall get(int index) {
          final RexWinAggCall aggCall = aggCalls.get(index);
          final SqlAggFunction op = (SqlAggFunction) aggCall.getOperator();
          return AggregateCall.create(op, aggCall.distinct, false,
              aggCall.ignoreNulls, getProjectOrdinals(aggCall.getOperands()),
              -1, RelCollations.EMPTY,
              aggCall.getType(), fieldNames.get(aggCall.ordinal));
        }
      };
    }
  }

  /**
   * A call to a windowed aggregate function.
   *
   * <p>Belongs to a {@link Window.Group}.
   *
   * <p>It's a bastard son of a {@link org.apache.calcite.rex.RexCall}; similar
   * enough that it gets visited by a {@link org.apache.calcite.rex.RexVisitor},
   * but it also has some extra data members.
   */
  public static class RexWinAggCall extends RexCall {
    /**
     * Ordinal of this aggregate within its partition.
     */
    public final int ordinal;

   /** Whether to eliminate duplicates before applying aggregate function. */
    public final boolean distinct;

    /** Whether to ignore nulls. */
    public final boolean ignoreNulls;

    @Deprecated // to be removed before 2.0
    public RexWinAggCall(
        SqlAggFunction aggFun,
        RelDataType type,
        List<RexNode> operands,
        int ordinal,
        boolean distinct) {
      this(aggFun, type, operands, ordinal, distinct, false);
    }

    /**
     * Creates a RexWinAggCall.
     *
     * @param aggFun   Aggregate function
     * @param type     Result type
     * @param operands Operands to call
     * @param ordinal  Ordinal within its partition
     * @param distinct Eliminate duplicates before applying aggregate function
     */
    public RexWinAggCall(
        SqlAggFunction aggFun,
        RelDataType type,
        List<RexNode> operands,
        int ordinal,
        boolean distinct,
        boolean ignoreNulls) {
      super(type, aggFun, operands);
      this.ordinal = ordinal;
      this.distinct = distinct;
      this.ignoreNulls = ignoreNulls;
    }

    /** {@inheritDoc}
     *
     * <p>Override {@link RexCall}, defining equality based on identity.
     */
    @Override public boolean equals(Object obj) {
      return this == obj;
    }

    @Override public int hashCode() {
      return Objects.hash(digest, ordinal, distinct);
    }

    @Override public RexCall clone(RelDataType type, List<RexNode> operands) {
      throw new UnsupportedOperationException();
    }
  }
}

// End Window.java
