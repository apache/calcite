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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Nullify is an unary operation that performs nullification to the given attribute
 * list of the input relation based on the given predicate.
 */
public abstract class Nullify extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode predicate;
  protected final ImmutableList<RexNode> attributes;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a nullification operator.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   */
  protected Nullify(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode predicate,
      List<? extends RexNode> attributes) {
    super(cluster, traits, child);
    this.predicate = predicate;
    this.attributes = ImmutableList.copyOf(attributes);
  }

  /**
   * Creates a Nullify by parsing serialized output.
   */
  protected Nullify(RelInput input) {
    this(input.getCluster(),
        input.getTraitSet(),
        input.getInput(),
        input.getExpression("condition"),
        input.getExpressionList("exprs"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), predicate, attributes);
  }

  public abstract Nullify copy(RelTraitSet traitSet, RelNode input,
      RexNode predicate, List<RexNode> attributes);

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(predicate);
  }

  public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw)
        .item("predicate", predicate)
        .item("attributes", attributes);
  }

  public RexNode getPredicate() {
    return predicate;
  }

  public ImmutableList<RexNode> getAttributes() {
    return ImmutableList.copyOf(attributes);
  }
}

// End Nullify.java
