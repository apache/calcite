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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical node in a planner that is capable of doing
 * physical trait propagation and derivation.
 *
 * <p>How to use?</p>
 *
 * <ol>
 *   <li>Enable top-down optimization by setting
 *   {@link org.apache.calcite.plan.volcano.VolcanoPlanner#setTopDownOpt(boolean)}.
 *   </li>
 *
 *   <li>Let your convention's rel interface extends {@link PhysicalNode},
 *   see {@link org.apache.calcite.adapter.enumerable.EnumerableRel} as
 *   an example.</li>
 *
 *   <li>Each physical operator overrides any one of the two methods:
 *   {@link PhysicalNode#passThrough(RelTraitSet)} or
 *   {@link PhysicalNode#passThroughTraits(RelTraitSet)} depending on
 *   your needs.</li>
 *
 *   <li>Choose derive mode for each physical operator by overriding
 *   {@link PhysicalNode#getDeriveMode()}.</li>
 *
 *   <li>If the derive mode is {@link DeriveMode#OMAKASE}, override
 *   method {@link PhysicalNode#derive(List)} in the physical operator,
 *   otherwise, override {@link PhysicalNode#derive(RelTraitSet, int)}
 *   or {@link PhysicalNode#deriveTraits(RelTraitSet, int)}.</li>
 *
 *   <li>Mark your enforcer operator by overriding {@link RelNode#isEnforcer()},
 *   see {@link Sort#isEnforcer()} as an example. This is important,
 *   because it can help {@code VolcanoPlanner} avoid unnecessary
 *   trait propagation and derivation, therefore improve optimization
 *   efficiency.</li>
 *
 *   <li>Implement {@link Convention#enforce(RelNode, RelTraitSet)}
 *   in your convention, which generates appropriate physical enforcer.
 *   See {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
 *   as example. Simply return {@code null} if you don't want physical
 *   trait enforcement.</li>
 * </ol>
 */
public interface PhysicalNode extends RelNode {

  /**
   * Pass required traitset from parent node to child nodes,
   * returns new node after traits is passed down.
   */
  default @Nullable RelNode passThrough(RelTraitSet required) {
    Pair<RelTraitSet, List<RelTraitSet>> p = passThroughTraits(required);
    if (p == null) {
      return null;
    }
    int size = getInputs().size();
    assert size == p.right.size();
    List<RelNode> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      RelNode n = RelOptRule.convert(getInput(i), p.right.get(i));
      list.add(n);
    }
    return copy(p.left, list);
  }

  /**
   * Pass required traitset from parent node to child nodes,
   * returns a pair of traits after traits is passed down.
   *
   * <p>Pair.left: the new traitset
   * <p>Pair.right: the list of required traitsets for child nodes
   */
  default @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      RelTraitSet required) {
    throw new RuntimeException(getClass().getName()
        + "#passThroughTraits() is not implemented.");
  }

  /**
   * Derive traitset from child node, returns new node after
   * traits derivation.
   */
  default @Nullable RelNode derive(RelTraitSet childTraits, int childId) {
    Pair<RelTraitSet, List<RelTraitSet>> p = deriveTraits(childTraits, childId);
    if (p == null) {
      return null;
    }
    int size = getInputs().size();
    assert size == p.right.size();
    List<RelNode> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      RelNode node = getInput(i);
      node = RelOptRule.convert(node, p.right.get(i));
      list.add(node);
    }
    return copy(p.left, list);
  }

  /**
   * Derive traitset from child node, returns a pair of traits after
   * traits derivation.
   *
   * <p>Pair.left: the new traitset
   * <p>Pair.right: the list of required traitsets for child nodes
   */
  default @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    throw new RuntimeException(getClass().getName()
        + "#deriveTraits() is not implemented.");
  }

  /**
   * Given a list of child traitsets,
   * inputTraits.size() == getInput().size(),
   * returns node list after traits derivation. This method is called
   * ONLY when the derive mode is OMAKASE.
   */
  default List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
    throw new RuntimeException(getClass().getName()
        + "#derive() is not implemented.");
  }

  /**
   * Returns mode of derivation.
   */
  default DeriveMode getDeriveMode() {
    return DeriveMode.LEFT_FIRST;
  }
}
