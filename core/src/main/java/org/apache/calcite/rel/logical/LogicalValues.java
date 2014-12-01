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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Values}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalValues extends Values {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new LogicalValues.
   *
   * <p>Note that tuples passed in become owned by this
   * rel (without a deep copy), so caller must not modify them after this
   * call, otherwise bad things will happen.
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param rowType Row type for tuples produced by this rel
   * @param tuples  2-dimensional array of tuple values to be produced; outer
   *                list contains tuples; each inner list is one tuple; all
   *                tuples must be of same length, conforming to rowType
   */
  public LogicalValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    super(cluster, rowType, tuples, cluster.traitSetOf(Convention.NONE));
  }

  /**
   * Creates a LogicalValues by parsing serialized output.
   */
  public LogicalValues(RelInput input) {
    super(input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return new LogicalValues(
        getCluster(),
        rowType,
        tuples);
  }

  /** Creates a LogicalValues that outputs no rows of a given row type. */
  public static LogicalValues createEmpty(RelOptCluster cluster,
      RelDataType rowType) {
    return new LogicalValues(cluster, rowType,
        ImmutableList.<ImmutableList<RexLiteral>>of());
  }

  /** Creates a LogicalValues that outputs one row and one column. */
  public static LogicalValues createOneRow(RelOptCluster cluster) {
    final RelDataType rowType =
        cluster.getTypeFactory().builder()
            .add("ZERO", SqlTypeName.INTEGER).nullable(false)
            .build();
    final ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO,
                    rowType.getFieldList().get(0).getType())));
    return new LogicalValues(cluster, rowType, tuples);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalValues.java
