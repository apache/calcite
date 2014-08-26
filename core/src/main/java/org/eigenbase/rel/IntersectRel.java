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

import java.util.List;

import org.eigenbase.relopt.*;

/**
 * <code>IntersectRel</code> returns the intersection of the rows of its inputs.
 * If "all" is true, then multiset intersection is performed; otherwise, set
 * intersection is performed (implying no duplicates in the results).
 */
public final class IntersectRel extends IntersectRelBase {
  //~ Constructors -----------------------------------------------------------

  public IntersectRel(
      RelOptCluster cluster,
      List<RelNode> inputs,
      boolean all) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        inputs,
        all);
  }

  /**
   * Creates an IntersectRel by parsing serialized output.
   */
  public IntersectRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public IntersectRel copy(
      RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new IntersectRel(
        getCluster(),
        inputs,
        all);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End IntersectRel.java
