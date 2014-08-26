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
 * <code>OneRowRel</code> always returns one row, one column (containing the
 * value 0).
 */
public final class OneRowRel extends OneRowRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>OneRowRel</code>.
   *
   * @param cluster {@link RelOptCluster}  this relational expression belongs
   *                to
   */
  public OneRowRel(RelOptCluster cluster) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE));
  }

  /**
   * Creates a OneRowRel by parsing serialized output.
   */
  public OneRowRel(RelInput input) {
    super(input.getCluster(), input.getTraitSet());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return this;
  }
}

// End OneRowRel.java
