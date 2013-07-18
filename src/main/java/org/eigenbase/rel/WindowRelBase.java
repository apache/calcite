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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.ImmutableIntList;

import net.hydromatic.linq4j.Ord;

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
 * <p>Each {@link org.eigenbase.rel.WindowRelBase.Window} has a set of {@link org.eigenbase.rel.WindowRelBase.Partition} objects, and each
 * {@link org.eigenbase.rel.WindowRelBase.Partition} object has a set of {@link org.eigenbase.rex.RexOver} objects.
 *
 * <p>Created by {@link org.eigenbase.rel.rules.WindowedAggSplitterRule}.
 */
public abstract class WindowRelBase
    extends SingleRel
{
  public final ImmutableList<Window> windows;

  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param child Input relational expression
   * @param rowType Output row type
   * @param windows Windows
   */
  public WindowRelBase(
      RelOptCluster cluster, RelTraitSet traits, RelNode child,
      RelDataType rowType, List<Window> windows)
  {
    super(cluster, traits, child);
    assert rowType != null;
    this.rowType = rowType;
    this.windows = ImmutableList.copyOf(windows);
  }

  @Override public boolean isValid(boolean fail) {
    // In the window specifications, an aggregate call such as
    // 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram.
    // (Not its projections.)
    final RexChecker checker = new RexChecker(getChild().getRowType(), fail);
    int count = 0;
    for (Window window : windows) {
      for (Partition partition : window.partitionList) {
        for (RexWinAggCall over : partition.overList) {
          ++count;
          if (!checker.isValid(over)) {
            return false;
          }
        }
      }
    }
    if (count == 0) {
      assert !fail : "empty";
      return false;
    }
    return true;
  }

  public void explain(RelOptPlanWriter pw)
  {
    super.explainTerms(pw);
    for (Ord<Window> window : Ord.zip(windows)) {
      pw.item("window#" + window.i, window.e.toString());
    }
  }

  /**
   * A Window is a range of input rows, defined by an upper and lower bound.
   * It also contains a list of {@link net.hydromatic.optiq.rules.java.JavaRules.EnumerableWindowRel.Partition} objects.
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
  public static class Window
  {
    /** The partitions which make up this window. */
    public final List<Partition> partitionList = new ArrayList<Partition>();
    final boolean physical;
    final SqlNode lowerBound;
    final SqlNode upperBound;
    public final ImmutableIntList orderKeys;
    private String digest;

    Window(
        boolean physical,
        SqlNode lowerBound,
        SqlNode upperBound,
        Integer [] ordinals)
    {
      assert ordinals != null : "precondition: ordinals != null";
      this.physical = physical;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.orderKeys = ImmutableIntList.copyOf(ordinals);
    }

    public String toString()
    {
      return digest;
    }

    public void computeDigest()
    {
      final StringBuilder buf = new StringBuilder();
      computeDigest(buf);
      this.digest = buf.toString();
    }

    private void computeDigest(StringBuilder buf)
    {
      buf.append("window(");
      buf.append("order by {");
      for (int i = 0; i < orderKeys.size(); i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append("$");
        buf.append(orderKeys.get(i));
      }
      buf.append("}");
      buf.append(physical ? " rows " : " range ");
      if (lowerBound != null) {
        if (upperBound != null) {
          buf.append("between ");
          buf.append(lowerBound.toString());
          buf.append(" and ");
        } else {
          buf.append(lowerBound.toString());
        }
      }
      if (upperBound != null) {
        buf.append(upperBound.toString());
      }
      buf.append(" partitions(");
      int i = 0;
      for (Partition partition : partitionList) {
        if (i++ > 0) {
          buf.append(", ");
        }
        partition.computeDigest(buf);
      }
      buf.append(")");
      buf.append(")");
    }

    public boolean equals(Object obj)
    {
      return (obj instanceof Window)
          && this.digest.equals(((Window) obj).digest);
    }

    public Partition lookupOrCreatePartition(Integer [] partitionKeys)
    {
      for (Partition partition : partitionList) {
        if (Arrays.equals(partition.partitionKeys, partitionKeys)) {
          return partition;
        }
      }
      Partition partition = new Partition(partitionKeys);
      partitionList.add(partition);
      return partition;
    }
  }

  /**
   * A Partition is a collection of windowed aggregate expressions which
   * belong to the same {@link WindowRelBase.Window} and have the same
   * partitioning keys.
   */
  public static class Partition
  {
    /**
     * Array of {@link org.eigenbase.rel.WindowRelBase.RexWinAggCall} objects, each of which is a call to a
     * {@link org.eigenbase.sql.SqlAggFunction}.
     */
    public final List<RexWinAggCall> overList = new ArrayList<RexWinAggCall>();

    /**
     * The ordinals of the input columns which uniquely identify rows in
     * this partition. May be empty. Must not be null.
     */
    final ImmutableIntList partitionKeys;

    Partition(Integer [] partitionKeys)
    {
      assert partitionKeys != null;
      this.partitionKeys = ImmutableIntList.copyOf(partitionKeys);
    }

    public boolean equals(Object obj)
    {
      return obj == this
          || obj instanceof Partition
          && partitionKeys.equals(((Partition) obj).partitionKeys);
    }

    private void computeDigest(StringBuilder buf)
    {
      buf.append("partition(key ");
      buf.append(partitionKeys);
      buf.append(" aggs ");
      buf.append(overList);
      buf.append(")");
    }

    public RexWinAggCall addOver(
        RelDataType type,
        SqlAggFunction operator,
        List<RexNode> operands,
        RexProgramBuilder programBuilder)
    {
      // Convert operands to inputRefs -- they will refer to the output
      // fields of a lower program.
      final List<RexNode> clonedOperands = new ArrayList<RexNode>(operands);
      for (RexNode operand : operands) {
        List<RexLocalRef> projectList = programBuilder.getProjectList();
        int index = projectList.indexOf(operand);
        if (index < 0) {
          index = projectList.size();
          programBuilder.addProject(operand, null);
        }
        clonedOperands[i] =
            new RexInputRef(
                index,
                operand.getType());
      }
      final RexWinAggCall aggCall =
          new RexWinAggCall(
              operator,
              type,
              clonedOperands,
              overList.size());
      overList.add(aggCall);
      return aggCall;
    }
  }

  /**
   * A call to a windowed aggregate function.
   *
   * <p>Belongs to a {@link org.eigenbase.rel.WindowRel.Partition}.
   *
   * <p>It's a bastard son of a {@link org.eigenbase.rex.RexCall}; similar enough that it gets
   * visited by a {@link org.eigenbase.rex.RexVisitor}, but it also has some extra data members.
   */
  public static class RexWinAggCall
      extends RexCall
  {
    /**
     * Ordinal of this aggregate within its partition.
     */
    public final int ordinal;

    /**
     * Creates a RexWinAggCall.
     *
     * @param aggFun Aggregate function
     * @param type Result type
     * @param operands Operands to call
     * @param ordinal Ordinal within its partition
     */
    public RexWinAggCall(
        SqlAggFunction aggFun,
        RelDataType type,
        List<RexNode> operands,
        int ordinal)
    {
      super(type, aggFun, operands);
      this.ordinal = ordinal;
    }
  }
}

// End WindowRelBase.java
