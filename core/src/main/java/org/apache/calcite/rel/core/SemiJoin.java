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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.ImmutableIntList;

import com.google.common.collect.ImmutableSet;

/**
 * A SemiJoinRel represents two relational expressions joined according to some
 * condition, where the output only contains the columns from the left join
 * input.
 */
public class SemiJoinRel extends EquiJoinRel {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinRel.
   *
   * @param cluster   cluster that join belongs to
   * @param traitSet  Traits
   * @param left      left join input
   * @param right     right join input
   * @param condition join condition
   * @param leftKeys  left keys of the semijoin
   * @param rightKeys right keys of the semijoin
   */
  public SemiJoinRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      ImmutableIntList leftKeys,
      ImmutableIntList rightKeys) {
    super(
        cluster,
        traitSet,
        left,
        right,
        condition,
        leftKeys,
        rightKeys,
        JoinRelType.INNER,
        ImmutableSet.<String>of());
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public SemiJoinRel copy(RelTraitSet traitSet, RexNode condition,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    assert joinType == JoinRelType.INNER;
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    assert joinInfo.isEqui();
    return new SemiJoinRel(getCluster(), traitSet, left, right, condition,
        joinInfo.leftKeys, joinInfo.rightKeys);
  }

  // implement RelNode
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 9-Apr-2006:  Just for now...
    return planner.getCostFactory().makeTinyCost();
  }

  // implement RelNode
  public double getRows() {
    // TODO:  correlation factor
    return RelMetadataQuery.getRowCount(left)
        * RexUtil.getSelectivity(condition);
  }

  /**
   * @return returns rowtype representing only the left join input
   */
  public RelDataType deriveRowType() {
    return deriveJoinRowType(
        left.getRowType(),
        null,
        JoinRelType.INNER,
        getCluster().getTypeFactory(),
        null,
        Collections.<RelDataTypeField>emptyList());
  }
}

// End SemiJoinRel.java
