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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.IntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.BitSet;
import java.util.List;

/**
 * Relational operator that eliminates
 * duplicates and computes totals.
 *
 * <p>It corresponds to the {@code GROUP BY} operator in a SQL query
 * statement, together with the aggregate functions in the {@code SELECT}
 * clause.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public abstract class Aggregate extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final List<AggregateCall> aggCalls;
  protected final BitSet groupSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an Aggregate.
   *
   * @param cluster  Cluster
   * @param traits   Traits
   * @param child    Child
   * @param groupSet Bit set of grouping fields
   * @param aggCalls Collection of calls to aggregate functions
   */
  protected Aggregate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      BitSet groupSet,
      List<AggregateCall> aggCalls) {
    super(cluster, traits, child);
    assert aggCalls != null;
    this.aggCalls = ImmutableList.copyOf(aggCalls);
    this.groupSet = groupSet;
    assert groupSet != null;
    assert groupSet.isEmpty() == (groupSet.cardinality() == 0)
        : "See https://bugs.openjdk.java.net/browse/JDK-6222207, "
        + "BitSet internal invariants may be violated";
    assert groupSet.length() <= child.getRowType().getFieldCount();
    for (AggregateCall aggCall : aggCalls) {
      assert typeMatchesInferred(aggCall, true);
    }
  }

  /**
   * Creates an Aggregate by parsing serialized output.
   */
  protected Aggregate(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBitSet("group"), input.getAggregateCalls("aggs"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), groupSet, aggCalls);
  }

  /** Creates a copy of this aggregate.
   *
   * @see #copy(org.apache.calcite.plan.RelTraitSet, java.util.List)
   */
  public abstract Aggregate copy(RelTraitSet traitSet, RelNode input,
      BitSet groupSet, List<AggregateCall> aggCalls);

  // implement RelNode
  public boolean isDistinct() {
    // we never return duplicate rows
    return true;
  }

  /**
   * Returns a list of calls to aggregate functions.
   *
   * @return list of calls to aggregate functions
   */
  public List<AggregateCall> getAggCallList() {
    return aggCalls;
  }

  /**
   * Returns the number of grouping fields.
   * These grouping fields are the leading fields in both the input and output
   * records.
   *
   * <p>NOTE: The {@link #getGroupSet()} data structure allows for the
   * grouping fields to not be on the leading edge. New code should, if
   * possible, assume that grouping fields are in arbitrary positions in the
   * input relational expression.
   *
   * @return number of grouping fields
   */
  public int getGroupCount() {
    return groupSet.cardinality();
  }

  /**
   * Returns a bitmap of the grouping fields.
   *
   * @return bitset of ordinals of grouping fields
   */
  public BitSet getGroupSet() {
    return groupSet;
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw)
        .item("group", groupSet)
        .itemIf("aggs", aggCalls, pw.nest());
    if (!pw.nest()) {
      for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
        pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    return pw;
  }

  // implement RelNode
  public double getRows() {
    // Assume that each sort column has 50% of the value count.
    // Therefore one sort column has .5 * rowCount,
    // 2 sort columns give .75 * rowCount.
    // Zero sort columns yields 1 row (or 0 if the input is empty).
    final int groupCount = groupSet.cardinality();
    if (groupCount == 0) {
      return 1;
    } else {
      double rowCount = super.getRows();
      rowCount *= 1.0 - Math.pow(.5, groupCount);
      return rowCount;
    }
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 24-Aug-2008:  This is bogus, but no more bogus
    // than what's currently in Join.
    double rowCount = RelMetadataQuery.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }

  protected RelDataType deriveRowType() {
    return deriveRowType(
        getCluster().getTypeFactory(),
        getInput().getRowType(),
        groupSet,
        aggCalls);
  }

  /** Computes the row type of an {@code Aggregate} before it exists. */
  public static RelDataType deriveRowType(RelDataTypeFactory typeFactory,
      final RelDataType inputRowType, BitSet groupSet,
      final List<AggregateCall> aggCalls) {
    final IntList groupList = BitSets.toList(groupSet);
    assert groupList.size() == groupSet.cardinality();
    return typeFactory.createStructType(
        CompositeList.of(
            // fields derived from grouping columns
            new AbstractList<RelDataTypeField>() {
              public int size() {
                return groupList.size();
              }

              public RelDataTypeField get(int index) {
                return inputRowType.getFieldList().get(
                    groupList.get(index));
              }
            },

            // fields derived from aggregate calls
            new AbstractList<RelDataTypeField>() {
              public int size() {
                return aggCalls.size();
              }

              public RelDataTypeField get(int index) {
                final AggregateCall aggCall = aggCalls.get(index);
                String name;
                if (aggCall.name != null) {
                  name = aggCall.name;
                } else {
                  name = "$f" + (groupList.size() + index);
                }
                return new RelDataTypeFieldImpl(name, index, aggCall.type);
              }
            }));
  }

  /**
   * Returns whether the inferred type of an {@link AggregateCall} matches the
   * type it was given when it was created.
   *
   * @param aggCall Aggregate call
   * @param fail    Whether to fail if the types do not match
   * @return Whether the inferred and declared types match
   */
  private boolean typeMatchesInferred(
      final AggregateCall aggCall,
      final boolean fail) {
    SqlAggFunction aggFunction = (SqlAggFunction) aggCall.getAggregation();
    AggCallBinding callBinding = aggCall.createBinding(this);
    RelDataType type = aggFunction.inferReturnType(callBinding);
    RelDataType expectedType = aggCall.type;
    return RelOptUtil.eq(
        "aggCall type",
        expectedType,
        "inferred type",
        type,
        fail);
  }

  /**
   * Returns whether any of the aggregates are DISTINCT.
   */
  public boolean containsDistinctCall() {
    for (AggregateCall call : aggCalls) {
      if (call.isDistinct()) {
        return true;
      }
    }
    return false;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Implementation of the {@link SqlOperatorBinding} interface for an
   * {@link AggregateCall aggregate call} applied to a set of operands in the
   * context of a {@link org.apache.calcite.rel.logical.LogicalAggregate}.
   */
  public static class AggCallBinding extends SqlOperatorBinding {
    private final List<RelDataType> operands;
    private final int groupCount;

    /**
     * Creates an AggCallBinding
     *
     * @param typeFactory  Type factory
     * @param aggFunction  Aggregate function
     * @param operands     Data types of operands
     * @param groupCount   Number of columns in the GROUP BY clause
     */
    public AggCallBinding(
        RelDataTypeFactory typeFactory,
        SqlAggFunction aggFunction,
        List<RelDataType> operands,
        int groupCount) {
      super(typeFactory, aggFunction);
      this.operands = operands;
      this.groupCount = groupCount;
      assert operands != null
          : "operands of aggregate call should not be null";
      assert groupCount >= 0
          : "number of group by columns should be greater than zero in "
          + "aggregate call. Got " + groupCount;
    }

    @Override public int getGroupCount() {
      return groupCount;
    }

    public int getOperandCount() {
      return operands.size();
    }

    public RelDataType getOperandType(int ordinal) {
      return operands.get(ordinal);
    }

    public CalciteException newError(
        Resources.ExInst<SqlValidatorException> e) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
  }
}

// End Aggregate.java
