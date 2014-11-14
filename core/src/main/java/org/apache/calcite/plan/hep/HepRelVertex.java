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
package org.eigenbase.relopt.hep;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

/**
 * HepRelVertex wraps a real {@link RelNode} as a vertex in a DAG representing
 * the entire query expression.
 */
public class HepRelVertex extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  /**
   * Wrapped rel currently chosen for implementation of expression.
   */
  private RelNode currentRel;

  //~ Constructors -----------------------------------------------------------

  HepRelVertex(RelNode rel) {
    super(
        rel.getCluster(),
        rel.getTraitSet());
    currentRel = rel;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void explain(RelWriter pw) {
    currentRel.explain(pw);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.equals(this.traitSet);
    assert inputs.equals(this.getInputs());
    return this;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // HepRelMetadataProvider is supposed to intercept this
    // and redirect to the real rels.
    throw Util.newInternal("should never get here");
  }

  @Override
  public double getRows() {
    return RelMetadataQuery.getRowCount(currentRel);
  }

  @Override
  protected RelDataType deriveRowType() {
    return currentRel.getRowType();
  }

  @Override
  public boolean isDistinct() {
    return currentRel.isDistinct();
  }

  @Override
  public boolean isKey(BitSet columns) {
    return currentRel.isKey(columns);
  }

  @Override
  protected String computeDigest() {
    return "HepRelVertex(" + currentRel + ")";
  }

  /**
   * Replaces the implementation for this expression with a new one.
   *
   * @param newRel new expression
   */
  void replaceRel(RelNode newRel) {
    currentRel = newRel;
  }

  /**
   * @return current implementation chosen for this vertex
   */
  public RelNode getCurrentRel() {
    return currentRel;
  }
}

// End HepRelVertex.java
