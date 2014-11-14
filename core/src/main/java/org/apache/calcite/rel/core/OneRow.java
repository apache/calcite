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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Relational expression that always returns one row.
 *
 * <p>It has one column, called "ZERO", containing the value 0.
 *
 * @see Values
 */
public abstract class OneRow extends AbstractRelNode {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>OneRow</code>.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    Traits
   */
  protected OneRow(RelOptCluster cluster, RelTraitSet traits) {
    super(cluster, traits);
  }

  /**
   * Creates a OneRow by parsing serialized output.
   */
  protected OneRow(RelInput input) {
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

// End OneRow.java
