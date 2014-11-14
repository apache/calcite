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
package org.eigenbase.rel;

import java.util.Collections;
import java.util.List;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/**
 * A <code>SingleRel</code> is a base class single-input relational expressions.
 */
public abstract class SingleRel extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  private RelNode child;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster {@link RelOptCluster}  this relational expression belongs
   *                to
   * @param child   input relational expression
   */
  protected SingleRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child) {
    super(cluster, traits);
    this.child = child;
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode getChild() {
    return child;
  }

  // implement RelNode
  public List<RelNode> getInputs() {
    return Collections.singletonList(child);
  }

  public double getRows() {
    // Not necessarily correct, but a better default than Rel's 1.0
    return RelMetadataQuery.getRowCount(child);
  }

  public void childrenAccept(RelVisitor visitor) {
    visitor.visit(child, 0, this);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .input("child", getChild());
  }

  // override Rel
  public void replaceInput(
      int ordinalInParent,
      RelNode rel) {
    assert ordinalInParent == 0;
    this.child = rel;
  }

  protected RelDataType deriveRowType() {
    return child.getRowType();
  }
}

// End SingleRel.java
