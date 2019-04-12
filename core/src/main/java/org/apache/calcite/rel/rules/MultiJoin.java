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
package org.apache.calcite.rel.rules;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins
 * represent strictly binary joins.
 */
public final class MultiJoin extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  private final List<RelNode> inputs;
  private final RexNode joinFilter;
  private final RelDataType rowType;
  private final boolean isFullOuterJoin;
  private final List<RexNode> outerJoinConditions;
  private final ImmutableList<JoinRelType> joinTypes;
  private final List<ImmutableBitSet> projFields;
  public final ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap;
  private final RexNode postJoinFilter;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a MultiJoin.
   *
   * @param cluster               cluster that join belongs to
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
  public MultiJoin(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode joinFilter,
      RelDataType rowType,
      boolean isFullOuterJoin,
      List<RexNode> outerJoinConditions,
      List<JoinRelType> joinTypes,
      List<ImmutableBitSet> projFields,
      ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
      RexNode postJoinFilter) {
    super(cluster, cluster.traitSetOf(Convention.NONE));
    this.inputs = Lists.newArrayList(inputs);
    this.joinFilter = joinFilter;
    this.rowType = rowType;
    this.isFullOuterJoin = isFullOuterJoin;
    this.outerJoinConditions =
        ImmutableNullableList.copyOf(outerJoinConditions);
    assert outerJoinConditions.size() == inputs.size();
    this.joinTypes = ImmutableList.copyOf(joinTypes);
    this.projFields = ImmutableNullableList.copyOf(projFields);
    this.joinFieldRefCountsMap = joinFieldRefCountsMap;
    this.postJoinFilter = postJoinFilter;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new MultiJoin(
        getCluster(),
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

  /**
   * Returns a deep copy of {@link #joinFieldRefCountsMap}.
   */
  private Map<Integer, int[]> cloneJoinFieldRefCountsMap() {
    Map<Integer, int[]> clonedMap = new HashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      clonedMap.put(i, joinFieldRefCountsMap.get(i).toIntArray());
    }
    return clonedMap;
  }

  public RelWriter explainTerms(RelWriter pw) {
    List<String> joinTypeNames = new ArrayList<>();
    List<String> outerJoinConds = new ArrayList<>();
    List<String> projFieldObjects = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      joinTypeNames.add(joinTypes.get(i).name());
      if (outerJoinConditions.get(i) == null) {
        outerJoinConds.add("NULL");
      } else {
        outerJoinConds.add(outerJoinConditions.get(i).toString());
      }
      if (projFields.get(i) == null) {
        projFieldObjects.add("ALL");
      } else {
        projFieldObjects.add(projFields.get(i).toString());
      }
    }

    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw.item("joinFilter", joinFilter)
        .item("isFullOuterJoin", isFullOuterJoin)
        .item("joinTypes", joinTypeNames)
        .item("outerJoinConditions", outerJoinConds)
        .item("projFields", projFieldObjects)
        .itemIf("postJoinFilter", postJoinFilter, postJoinFilter != null);
  }

  public RelDataType deriveRowType() {
    return rowType;
  }

  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(joinFilter);
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

    return new MultiJoin(
        getCluster(),
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

  /**
   * @return join filters associated with this MultiJoin
   */
  public RexNode getJoinFilter() {
    return joinFilter;
  }

  /**
   * @return true if the MultiJoin corresponds to a full outer join.
   */
  public boolean isFullOuterJoin() {
    return isFullOuterJoin;
  }

  /**
   * @return outer join conditions for null-generating inputs
   */
  public List<RexNode> getOuterJoinConditions() {
    return outerJoinConditions;
  }

  /**
   * @return join types of each input
   */
  public List<JoinRelType> getJoinTypes() {
    return joinTypes;
  }

  /**
   * @return bitmaps representing the fields projected from each input; if an
   * entry is null, all fields are projected
   */
  public List<ImmutableBitSet> getProjFields() {
    return projFields;
  }

  /**
   * @return the map of reference counts for each input, representing the
   * fields accessed in join conditions
   */
  public ImmutableMap<Integer, ImmutableIntList> getJoinFieldRefCountsMap() {
    return joinFieldRefCountsMap;
  }

  /**
   * @return a copy of the map of reference counts for each input,
   * representing the fields accessed in join conditions
   */
  public Map<Integer, int[]> getCopyJoinFieldRefCountsMap() {
    return cloneJoinFieldRefCountsMap();
  }

  /**
   * @return post-join filter associated with this MultiJoin
   */
  public RexNode getPostJoinFilter() {
    return postJoinFilter;
  }

  boolean containsOuter() {
    for (JoinRelType joinType : joinTypes) {
      if (joinType.isOuterJoin()) {
        return true;
      }
    }
    return false;
  }
}

// End MultiJoin.java
