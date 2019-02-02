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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableMap;

import java.util.List;

/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins
 * represent strictly binary joins.
 */
public class LogicalMultiJoin extends MultiJoin implements LogicalRel {

  /**
   * Constructs a LogicalMultiJoin.
   *
   * @param cluster               cluster that join belongs to
   * @param traitSet              Trait set
   * @param inputs                inputs into this multi-join
   * @param joinFilter            join filter applicable to this join node
   * @param rowType               row type of the join result of this node
   * @param isFullOuterJoin       true if the join is a full outer join
   * @param outerJoinConditions   outer join condition associated with each join
   *                              input, if the input is null-generating in a
   *                              left or right outer join; null otherwise
   * @param joinTypes             the join type corresponding to each input; if
   *                              an input is null-generating in a left or right
   *                              outer join, the entry indicates the type of
   *                              outer join; otherwise, the entry is set to
   *                              INNER
   * @param projFields            fields that will be projected from each input;
   *                              if null, projection information is not
   *                              available yet so it's assumed that all fields
   *                              from the input are projected
   * @param joinFieldRefCountsMap counters of the number of times each field
   *                              is referenced in join conditions, indexed by
   *                              the input #
   * @param postJoinFilter        filter to be applied after the joins are
   */
  public LogicalMultiJoin(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode joinFilter, RelDataType rowType,
      boolean isFullOuterJoin, List<RexNode> outerJoinConditions,
      List<JoinRelType> joinTypes,
      List<ImmutableBitSet> projFields,
      ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
      RexNode postJoinFilter) {
    super(cluster, traitSet, inputs, joinFilter, rowType, isFullOuterJoin, outerJoinConditions,
        joinTypes, projFields, joinFieldRefCountsMap, postJoinFilter);
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputsSatisfy(Convention.NONE, Litmus.THROW);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalMultiJoin(
        getCluster(),
        traitSet,
        inputs,
        joinFilter,
        rowType,
        isFullOuterJoin,
        outerJoinConditions,
        joinTypes,
        projFields,
        joinFieldRefCountsMap,
        postJoinFilter);
  }

  public RelNode accept(RexShuttle shuttle) {
    RexNode joinFilter = shuttle.apply(this.joinFilter);
    List<RexNode> outerJoinConditions = shuttle.apply(this.outerJoinConditions);
    RexNode postJoinFilter = shuttle.apply(this.postJoinFilter);

    if (joinFilter == this.joinFilter
        && outerJoinConditions == this.outerJoinConditions
        && postJoinFilter == this.postJoinFilter) {
      return this;
    }

    return new LogicalMultiJoin(
        getCluster(),
        traitSet,
        inputs,
        joinFilter,
        rowType,
        isFullOuterJoin,
        outerJoinConditions,
        joinTypes,
        projFields,
        joinFieldRefCountsMap,
        postJoinFilter);
  }

  /** Creates a LogicalMultiJoin. */
  public static LogicalMultiJoin create(List<RelNode> inputs,
      RexNode joinFilter, RelDataType rowType,
      boolean isFullOuterJoin, List<RexNode> outerJoinConditions,
      List<JoinRelType> joinTypes,
      List<ImmutableBitSet> projFields,
      ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
      RexNode postJoinFilter) {
    assert inputs.size() > 1 : "MultiJoin should have at least two inputs. Inputs are: " + inputs;
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalMultiJoin(cluster, traitSet, inputs, joinFilter, rowType, isFullOuterJoin,
        outerJoinConditions, joinTypes, projFields, joinFieldRefCountsMap, postJoinFilter);
  }

}

// End LogicalMultiJoin.java
