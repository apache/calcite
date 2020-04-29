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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;

/**
 * Calling convention trait.
 */
public interface Convention extends RelTrait {
  /**
   * Convention that for a relational expression that does not support any
   * convention. It is not implementable, and has to be transformed to
   * something else in order to be implemented.
   *
   * <p>Relational expressions generally start off in this form.</p>
   *
   * <p>Such expressions always have infinite cost.</p>
   */
  Convention NONE = new Impl("NONE", RelNode.class);

  Class getInterface();

  String getName();

  /**
   * Given an input and required traits, returns the corresponding
   * enforcer rel nodes, like physical Sort, Exchange etc.
   *
   * @param input The input RelNode
   * @param required The required traits
   * @return Physical enforcer that satisfies the required traitSet,
   * or {@code null} if trait enforcement is not allowed or the
   * required traitSet can't be satisfied.
   */
  default RelNode enforce(RelNode input, RelTraitSet required) {
    throw new RuntimeException(getClass().getName()
        + "#enforce() is not implemented.");
  }

  /**
   * Returns whether we should convert from this convention to
   * {@code toConvention}. Used by {@link ConventionTraitDef}.
   *
   * @param toConvention Desired convention to convert to
   * @return Whether we should convert from this convention to toConvention
   */
  default boolean canConvertConvention(Convention toConvention) {
    return false;
  }

  /**
   * Returns whether we should convert from this trait set to the other trait
   * set.
   *
   * <p>The convention decides whether it wants to handle other trait
   * conversions, e.g. collation, distribution, etc.  For a given convention, we
   * will only add abstract converters to handle the trait (convention,
   * collation, distribution, etc.) conversions if this function returns true.
   *
   * @param fromTraits Traits of the RelNode that we are converting from
   * @param toTraits Target traits
   * @return Whether we should add converters
   */
  default boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
      RelTraitSet toTraits) {
    return false;
  }

  /**
   * Default implementation.
   */
  class Impl implements Convention {
    private final String name;
    private final Class<? extends RelNode> relClass;

    public Impl(String name, Class<? extends RelNode> relClass) {
      this.name = name;
      this.relClass = relClass;
    }

    @Override public String toString() {
      return getName();
    }

    public void register(RelOptPlanner planner) {}

    public boolean satisfies(RelTrait trait) {
      return this == trait;
    }

    public Class getInterface() {
      return relClass;
    }

    public String getName() {
      return name;
    }

    public RelTraitDef getTraitDef() {
      return ConventionTraitDef.INSTANCE;
    }

    @Override public RelNode enforce(final RelNode input,
        final RelTraitSet required) {
      return null;
    }

    public boolean canConvertConvention(Convention toConvention) {
      return false;
    }

    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
        RelTraitSet toTraits) {
      return false;
    }
  }
}
