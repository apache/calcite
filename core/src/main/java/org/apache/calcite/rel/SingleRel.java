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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Abstract base class for relational expressions with a single input.
 *
 * <p>It is not required that single-input relational expressions use this
 * class as a base class. However, default implementations of methods make life
 * easier.
 */
public abstract class SingleRel extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  protected RelNode input;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param input   Input relational expression
   */
  protected SingleRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, traits);
    this.input = input;
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode getInput() {
    return input;
  }

  @Override public List<RelNode> getInputs() {
    return ImmutableList.of(input);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // Not necessarily correct, but a better default than AbstractRelNode's 1.0
    return mq.getRowCount(input);
  }

  @Override public void childrenAccept(RelVisitor visitor) {
    visitor.visit(input, 0, this);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .input("input", getInput());
  }

  @Override public void replaceInput(
      int ordinalInParent,
      RelNode rel) {
    assert ordinalInParent == 0;
    this.input = rel;
  }

  protected RelDataType deriveRowType() {
    return input.getRowType();
  }
}

// End SingleRel.java
