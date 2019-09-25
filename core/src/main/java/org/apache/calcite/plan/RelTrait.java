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

/**
 * RelTrait represents the manifestation of a relational expression trait within
 * a trait definition. For example, a {@code CallingConvention.JAVA} is a trait
 * of the {@link ConventionTraitDef} trait definition.
 *
 * <h2><a id="EqualsHashCodeNote">Note about equals() and hashCode()</a></h2>
 *
 * <p>If all instances of RelTrait for a particular RelTraitDef are defined in
 * an {@code enum} and no new RelTraits can be introduced at runtime, you need
 * not override {@link #hashCode()} and {@link #equals(Object)}. If, however,
 * new RelTrait instances are generated at runtime (e.g. based on state external
 * to the planner), you must implement {@link #hashCode()} and
 * {@link #equals(Object)} for proper {@link RelTraitDef#canonize canonization}
 * of your RelTrait objects.</p>
 */
public interface RelTrait {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the RelTraitDef that defines this RelTrait.
   *
   * @return the RelTraitDef that defines this RelTrait
   */
  RelTraitDef getTraitDef();

  /**
   * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
   */
  int hashCode();

  /**
   * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
   */
  boolean equals(Object o);

  /**
   * Returns whether this trait satisfies a given trait.
   *
   * <p>A trait satisfies another if it is the same or stricter. For example,
   * {@code ORDER BY x, y} satisfies {@code ORDER BY x}.
   *
   * <p>A trait's {@code satisfies} relation must be a partial order (reflexive,
   * anti-symmetric, transitive). Many traits cannot be "loosened"; their
   * {@code satisfies} is an equivalence relation, where only X satisfies X.
   *
   * <p>If a trait has multiple values
   * (see {@link org.apache.calcite.plan.RelCompositeTrait})
   * a collection (T0, T1, ...) satisfies T if any Ti satisfies T.
   *
   * @param trait Given trait
   * @return Whether this trait subsumes a given trait
   */
  boolean satisfies(RelTrait trait);

  /**
   * Returns a succinct name for this trait. The planner may use this String
   * to describe the trait.
   */
  String toString();

  /**
   * Registers a trait instance with the planner.
   *
   * <p>This is an opportunity to add rules that relate to that trait. However,
   * typical implementations will do nothing.</p>
   *
   * @param planner Planner
   */
  void register(RelOptPlanner planner);
}

// End RelTrait.java
