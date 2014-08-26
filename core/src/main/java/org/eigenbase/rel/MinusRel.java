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
 * <code>MinusRel</code> returns the rows of its first input minus any matching
 * rows from its other inputs. If "all" is true, then multiset subtraction is
 * performed; otherwise, set subtraction is performed (implying no duplicates in
 * the results).
 */
public final class MinusRel extends MinusRelBase {
  //~ Constructors -----------------------------------------------------------

  public MinusRel(
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
   * Creates a MinusRel by parsing serialized output.
   */
  public MinusRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public MinusRel copy(
      RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new MinusRel(
        getCluster(),
        inputs,
        all);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End MinusRel.java
