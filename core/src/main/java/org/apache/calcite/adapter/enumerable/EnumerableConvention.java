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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Family of calling conventions that return results as an
 * {@link org.apache.calcite.linq4j.Enumerable}.
 */
public enum EnumerableConvention implements Convention {
  INSTANCE;

  /** Cost of an enumerable node versus implementing an equivalent node in a
   * "typical" calling convention. */
  public static final double COST_MULTIPLIER = 1.0d;

  @Override public String toString() {
    return getName();
  }

  public Class getInterface() {
    return EnumerableRel.class;
  }

  public String getName() {
    return "ENUMERABLE";
  }

  public RelTraitDef getTraitDef() {
    return ConventionTraitDef.INSTANCE;
  }

  public boolean satisfies(RelTrait trait) {
    return this == trait;
  }

  public void register(RelOptPlanner planner) {}

  public boolean canConvertConvention(Convention toConvention) {
    return false;
  }

  public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
      RelTraitSet toTraits) {
    return false;
  }
}

// End EnumerableConvention.java
