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
import org.apache.calcite.rel.SingleRel;

import java.util.List;

/**
 * BestMatch is a unary operation that is always at the top of a join tree
 * to eliminate spurious tuples.
 */
public abstract class BestMatch extends SingleRel {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a best-match operator.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  the traits of this rel
   * @param input   Input relational expression
   */
  protected BestMatch(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, traits, input);
  }

  /**
   * Creates a best match by its input.
   */
  protected BestMatch(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public abstract BestMatch copy(RelTraitSet traitSet, RelNode input);
}

// End BestMatch.java
