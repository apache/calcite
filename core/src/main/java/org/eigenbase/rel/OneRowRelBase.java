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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * <code>OneRowRelBase</code> is an abstract base class for implementations of
 * {@link OneRowRel}.
 */
public abstract class OneRowRelBase extends AbstractRelNode {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>OneRowRelBase</code> with specific traits.
   *
   * @param cluster {@link RelOptCluster}  this relational expression belongs
   *                to
   * @param traits  for this rel
   */
  protected OneRowRelBase(RelOptCluster cluster, RelTraitSet traits) {
    super(cluster, traits);
  }

  /**
   * Creates a OneRowRelBase by parsing serialized output.
   */
  protected OneRowRelBase(RelInput input) {
    this(input.getCluster(), input.getTraitSet());
  }

  //~ Methods ----------------------------------------------------------------

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeTinyCost();
  }

  protected RelDataType deriveRowType() {
    return deriveOneRowType(getCluster().getTypeFactory());
  }

  public static RelDataType deriveOneRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder().add("ZERO", SqlTypeName.INTEGER).build();
  }
}

// End OneRowRelBase.java
