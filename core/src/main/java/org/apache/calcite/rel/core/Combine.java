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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A relational operator that combines multiple relational expressions into a single root.
 * This is used for multi-root optimization in the VolcanoPlanner.
 *
 * @see org.apache.calcite.adapter.enumerable.EnumerableCombine
 */
public class Combine extends AbstractRelNode {
  protected ImmutableList<RelNode> inputs;

  /** Creates a Combine. */
  public static Combine create(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
    return new Combine(cluster, traitSet, inputs);
  }


  public Combine(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
    super(cluster, traitSet);
    this.inputs = ImmutableList.copyOf(inputs);
  }

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Combine(getCluster(), traitSet, inputs);
  }

  @Override public void replaceInput(int ordinalInParent, RelNode rel) {
    // Combine has multiple inputs stored in an immutable list.
    // To replace an input, we need to create a new list with the replacement.
    ImmutableList.Builder<RelNode> newInputs = ImmutableList.builder();
    for (int i = 0; i < inputs.size(); i++) {
      if (i == ordinalInParent) {
        newInputs.add(rel);
      } else {
        newInputs.add(inputs.get(i));
      }
    }
    inputs = newInputs.build();
  }


  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw;
  }

  @Override protected RelDataType deriveRowType() {
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    RelDataTypeFactory.Builder builder = typeFactory.builder();

    // One column per input query (QUERY_0, QUERY_1, etc.)
    // Each cell is a nullable MAP representing a struct with column names as keys
    RelDataType anyType = typeFactory.createJavaType(Object.class);
    RelDataType mapType =
        typeFactory.createMapType(typeFactory.createJavaType(String.class), anyType);
    RelDataType nullableMapType = typeFactory.createTypeWithNullability(mapType, true);

    for (int i = 0; i < inputs.size(); i++) {
      builder.add("QUERY_" + i, nullableMapType);
    }

    return builder.build();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // The self cost of Combine is minimal - it's just a structural operator
    // that binds multiple queries together for optimization purposes.
    // The real cost comes from executing all the inputs (handled by getCumulativeCost).

    // We add a tiny cost to represent the overhead of managing multiple result sets
    double rowCount = 0;
    for (RelNode input : inputs) {
      Double inputRows = mq.getRowCount(input);
      rowCount += inputRows;
    }

    // Very small CPU cost for result set management
    // No I/O cost since Combine doesn't read data itself
    return planner.getCostFactory().makeCost(
        rowCount,
        rowCount * 0.01, // minimal CPU cost
        0); // no I/O
  }
}
