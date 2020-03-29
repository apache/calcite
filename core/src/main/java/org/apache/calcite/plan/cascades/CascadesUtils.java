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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Utilities.
 */
public class CascadesUtils {

  private CascadesUtils() {
  }

  public static boolean isLogical(RelNode relNode) {
    return relNode.getConvention() == Convention.NONE;
  }

  public static RelTraitSet differenceWithoutConvention(RelTraitSet originalTraits,
      RelTraitSet traits) {
    RelTraitSet result = RelTraitSet.createEmpty();
    for (RelTrait trait : traits) {
      // Exclude conventions.
      if (!conventionTrait(trait)
          && !originalTraits.getTrait(trait.getTraitDef()).satisfies(trait)) {
        result = result.plus(trait);
      }
    }
    return result;
  }

  public static RelTraitSet toDefaultTraits(RelTraitSet traits) {
    RelTraitSet result = RelTraitSet.createEmpty();
    for (RelTrait trait : traits) {
      if (conventionTrait(trait)) {
        result = result.plus(trait);
      } else  {
        // Replace with top traits (i.e. empty collation or RelDistribution.ANY).
        result = result.plus(trait.getTraitDef().getDefault());
      }
    }
    return result;
  }

  public static boolean conventionTrait(RelTrait trait) {
    return trait.getTraitDef() == ConventionTraitDef.INSTANCE;
  }

  public static boolean isTopTrait(RelTrait trait) {
    return !(trait instanceof RelMultipleTrait) || ((RelMultipleTrait) trait).isTop();
  }
}
