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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Group of equivalent {@link RelNode}s.
 */
public class RelGroup {

  private final int id;

  private final Set<RelNode> logicalRels = new LinkedHashSet<>();

  private final Set<RelNode> physicalRels = new LinkedHashSet<>();

  private final Map<RelTraitSet, RelSubGroup> subGroups = new LinkedHashMap<>();

  private final RelDataType dataType;

  private RelNode originalRel;

  private boolean expanded;

  /**
   * Traits for which this group was optimized.
   */
  private Set<RelTraitSet> optimizedTraits = new HashSet<>();

  public RelGroup(int id, RelDataType dataType) {
    this.id = id;
    this.dataType = dataType;
  }

  RelSubGroup getSubGroup(RelTraitSet traits) {
    return subGroups.get(traits);
  }

  public Set<RelNode> logicalRels() {
    return logicalRels;
  }

  public Set<RelNode> physicalRels() {
    return physicalRels;
  }

  public RelSubGroup add(RelNode rel) {
    // TODO assert same rowtype
    final RelTraitSet traitSet = rel.getTraitSet();
    final RelSubGroup subGroup = getOrCreateSubGroup(rel.getCluster(), traitSet);

    if (isLogical(rel)) {
      logicalRels.add(rel);
    } else {
      physicalRels.add(rel);

      updateWinners(rel);
    }
    for (RelTrait trait : rel.getTraitSet()) {
      assert trait == trait.getTraitDef().canonize(trait);
    }

    if (originalRel == null) {
      originalRel = rel;
    }

    return subGroup;
  }

  RelSubGroup getOrCreateSubGroup(RelOptCluster cluster, RelTraitSet traits) {
    RelSubGroup subGroup = getSubGroup(traits);
    if (subGroup != null) {
      return subGroup;
    }

    subGroup = new RelSubGroup(cluster, traits, this);

    for (RelNode rel : physicalRels) {
      if (rel.getTraitSet().satisfies(traits)) {
        subGroup.onRelAdded(rel);
      }
    }

    // TODO Canonize traits?
    RelSubGroup old = subGroups.put(traits, subGroup);

    assert old == null;

    return subGroup;
  }

  public RelDataType rowType() {
    return dataType;
  }

  public Collection<RelSubGroup> allSubGroups() {
    return subGroups.values();
  }

  public Set<RelNode> getRels() {
    Set<RelNode> allRels = new HashSet<>(physicalRels.size() + logicalRels.size());
    allRels.addAll(physicalRels);
    allRels.addAll(logicalRels);
    return allRels;
  }

  public int getId() {
    return id;
  }

  public RelNode originalRel() {
    return originalRel;
  }

  public boolean isExpanded() {
    return expanded;
  }

  public void markExpanded() {
    this.expanded = true;
  }

  public boolean isOptimized(RelTraitSet traits) {
    return optimizedTraits.contains(traits);
  }

  public void markOptimized(RelTraitSet traits) {
    this.optimizedTraits.add(traits);
  }

  private void updateWinners(RelNode rel) {
    if (isLogical(rel)) {
      return;
    }
    for (RelSubGroup subGroup : subGroups.values()) {
      if (rel.getTraitSet().satisfies(subGroup.getTraitSet())) {
        subGroup.onRelAdded(rel);
      }
    }
  }

  public <T extends RelTrait> Set<T> traitsFlatten(RelTraitDef<T> traitDef) {
    Set<T> result = new HashSet<>(subGroups.size());
    for (RelNode relNode : physicalRels) {
      RelTraitSet traits = relNode.getTraitSet();
      if (traitDef.multiple()) {
        List<T> t = traits.getTraits((RelTraitDef) traitDef);
        result.addAll(t);
      } else {
        T t = traits.getTrait(traitDef);
        result.add(t);
      }
    }
    return result;
  }

  @Override public String toString() {
    return "RelGroup{"
        + "id=" + id
        + ", originalRel=" + originalRel
        + '}';
  }
}
