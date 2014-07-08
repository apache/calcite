/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.rel;

import java.util.*;

import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.rex.*;
import org.apache.optiq.sql.SqlAggFunction;
import org.apache.optiq.util.ImmutableIntList;
import org.apache.optiq.util.Util;

import org.apache.linq4j.Ord;

import com.google.common.collect.ImmutableList;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A window rel can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link org.apache.optiq.rel.WindowRelBase.Window} has a set of
 * {@link org.apache.optiq.rex.RexOver} objects.
 *
 * <p>Created by {@link org.apache.optiq.rel.rules.WindowedAggSplitterRule}.
 */
public abstract class WindowRelBase extends SingleRel {
  public final ImmutableList<Window> windows;
  public final List<RexLiteral> constants;

  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param child   Input relational expression
   * @param constants List of constants that are additional inputs
   * @param rowType Output row type
   * @param windows Windows
   */
  public WindowRelBase(
      RelOptCluster cluster, RelTraitSet traits, RelNode child,
      List<RexLiteral> constants, RelDataType rowType, List<Window> windows) {
    super(cluster, traits, child);
    this.constants = ImmutableList.copyOf(constants);
    assert rowType != null;
    this.rowType = rowType;
    this.windows = ImmutableList.copyOf(windows);
  }

  @Override
  public boolean isValid(boolean fail) {
    // In the window specifications, an aggregate call such as
    // 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram.
    // (Not its projections.)
    final RelDataType childRowType = getChild().getRowType();

    final int childFieldCount = childRowType.getFieldCount();
    final int inputSize = childFieldCount + constants.size();
    final List<RelDataType> inputTypes =
        new AbstractList<RelDataType>() {
          @Override
          public RelDataType get(int index) {
            return index < childFieldCount
                ? childRowType.getFieldList().get(index).getType()
                : constants.get(index - childFieldCount).getType();
          }

          @Override
          public int size() {
            return inputSize;
          }
        };

    final RexChecker checker =
        new RexChecker(inputTypes, fail);
    int count = 0;
    for (Window window : windows) {
      for (RexWinAggCall over : window.aggCalls) {
        ++count;
        if (!checker.isValid(over)) {
          return false;
        }
      }
    }
    if (count == 0) {
      assert !fail : "empty";
      return false;
    }
    return true;
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<Window> window : Ord.zip(windows)) {
      pw.item("window#" + window.i, window.e.toString());
    }
    return pw;
  }

  static ImmutableIntList getProjectOrdinals(final List<RexNode> exprs) {
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

  static RelCollation getCollation(final List<RexFieldCollation> collations) {
    return RelCollationImpl.of(
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

  /**
   * A Window is a range of input rows, defined by an upper and lower bound.
   * It also has zero or more partitioning columns.
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
  public static class Window {
    public final BitSet groupSet;
    public final boolean isRows;
    public final RexWindowBound lowerBound;
    public final RexWindowBound upperBound;
    public final RelCollation orderKeys;
    private final String digest;

    /**
     * List of {@link org.apache.optiq.rel.WindowRelBase.RexWinAggCall}
     * objects, each of which is a call to a
     * {@link org.apache.optiq.sql.SqlAggFunction}.
     */
    public final ImmutableList<RexWinAggCall> aggCalls;

    public Window(
        BitSet groupSet,
        boolean isRows,
        RexWindowBound lowerBound,
        RexWindowBound upperBound,
        RelCollation orderKeys,
        List<RexWinAggCall> aggCalls) {
      assert orderKeys != null : "precondition: ordinals != null";
      assert groupSet != null;
      this.groupSet = groupSet;
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
      buf.append(groupSet);
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

    @Override
    public boolean equals(Object obj) {
      return this == obj
          || obj instanceof Window
          && this.digest.equals(((Window) obj).digest);
    }

    @Override
    public int hashCode() {
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
     * @see org.apache.optiq.sql.SqlWindow#isAlwaysNonEmpty()
     * @see org.apache.optiq.sql.SqlOperatorBinding#getGroupCount()
     * @see org.apache.optiq.sql.validate.SqlValidatorImpl#resolveWindow(org.apache.optiq.sql.SqlNode, org.apache.optiq.sql.validate.SqlValidatorScope, boolean)
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
    public List<AggregateCall> getAggregateCalls(WindowRelBase windowRel) {
      final List<String> fieldNames =
          Util.skip(windowRel.getRowType().getFieldNames(),
              windowRel.getChild().getRowType().getFieldCount());
      return new AbstractList<AggregateCall>() {
        public int size() {
          return aggCalls.size();
        }

        public AggregateCall get(int index) {
          final RexWinAggCall aggCall = aggCalls.get(index);
          return new AggregateCall(
              (Aggregation) aggCall.getOperator(),
              false,
              getProjectOrdinals(aggCall.getOperands()),
              aggCall.getType(),
              fieldNames.get(aggCall.ordinal));
        }
      };
    }
  }

  /**
   * A call to a windowed aggregate function.
   *
   * <p>Belongs to a {@link org.apache.optiq.rel.WindowRelBase.Window}.
   *
   * <p>It's a bastard son of a {@link org.apache.optiq.rex.RexCall}; similar
   * enough that it gets visited by a {@link org.apache.optiq.rex.RexVisitor},
   * but it also has some extra data members.
   */
  public static class RexWinAggCall extends RexCall {
    /**
     * Ordinal of this aggregate within its partition.
     */
    public final int ordinal;

    /**
     * Creates a RexWinAggCall.
     *
     * @param aggFun   Aggregate function
     * @param type     Result type
     * @param operands Operands to call
     * @param ordinal  Ordinal within its partition
     */
    public RexWinAggCall(
        SqlAggFunction aggFun,
        RelDataType type,
        List<RexNode> operands,
        int ordinal) {
      super(type, aggFun, operands);
      this.ordinal = ordinal;
    }

    @Override
    public RexCall clone(RelDataType type, List<RexNode> operands) {
      throw new UnsupportedOperationException();
    }
  }
}

// End WindowRelBase.java
