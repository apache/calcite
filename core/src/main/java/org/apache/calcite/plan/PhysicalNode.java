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
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical node in a planner that is capable of doing
 * physical trait propagation and derivation.
 */
public interface PhysicalNode extends RelNode {

  /**
   * Pass required traitset from parent node to child nodes,
   * returns new node after traits is passed down.
   */
  default RelNode passThrough(RelTraitSet required) {
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
  default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      RelTraitSet required) {
    throw new RuntimeException("Not implemented!");
  }

  /**
   * Derive traitset from child node, returns new node after
   * traits derivation.
   */
  default RelNode derive(RelTraitSet childTraits, int childId) {
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
  default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    throw new RuntimeException("Not implemented!");
  }

  /**
   * Given a list of child traitsets,
   * inputTraits.size() == getInput().size(),
   * returns node list after traits derivation. This method is called
   * ONLY when the derive mode is OMAKASE.
   */
  default List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
    throw new RuntimeException("Not implemented!");
  }

  /**
   * Returns mode of derivation.
   */
  default DeriveMode getDeriveMode() {
    return DeriveMode.LEFT_FIRST;
  }
}
