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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalExchange;

/**
 * Definition of the distribution trait.
 *
 * <p>Distribution is a physical property (i.e. a trait) because it can be
 * changed without loss of information. The converter to do this is the
 * {@link Exchange} operator.
 */
public class RelDistributionTraitDef extends RelTraitDef<RelDistribution> {
  public static final RelDistributionTraitDef INSTANCE =
      new RelDistributionTraitDef();

  private RelDistributionTraitDef() {
  }

  public Class<RelDistribution> getTraitClass() {
    return RelDistribution.class;
  }

  public String getSimpleName() {
    return "dist";
  }

  public RelDistribution getDefault() {
    return RelDistributions.ANY;
  }

  public RelNode convert(RelOptPlanner planner, RelNode rel,
      RelDistribution toDistribution, boolean allowInfiniteCostConverters) {
    if (toDistribution == RelDistributions.ANY) {
      return rel;
    }

    // Create a logical sort, then ask the planner to convert its remaining
    // traits (e.g. convert it to an EnumerableSortRel if rel is enumerable
    // convention)
    final Exchange exchange = LogicalExchange.create(rel, toDistribution);
    RelNode newRel = planner.register(exchange, rel);
    final RelTraitSet newTraitSet = rel.getTraitSet().replace(toDistribution);
    if (!newRel.getTraitSet().equals(newTraitSet)) {
      newRel = planner.changeTraits(newRel, newTraitSet);
    }
    return newRel;
  }

  public boolean canConvert(RelOptPlanner planner, RelDistribution fromTrait,
      RelDistribution toTrait) {
    return true;
  }
}

// End RelDistributionTraitDef.java
