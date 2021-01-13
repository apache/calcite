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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that ensures that we do not add enforcers for the already satisfied traits.
 * See https://issues.apache.org/jira/browse/CALCITE-4466 for more information.
 */
public class MultipleTraitConversionTest {
  @SuppressWarnings("ConstantConditions")
  @Test public void testMultipleTraitConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.addRelTraitDef(CustomTraitDef.INSTANCE);
    planner.setNoneConventionHasInfiniteCost(false);

    RelOptCluster cluster = newCluster(planner);

    RelTraitSet fromTraits = cluster.traitSetOf(RelCollations.of(ImmutableIntList.of(0, 1)));

    RelTraitSet toTraits = fromTraits
        .plus(RelCollations.of(0))
        .plus(CustomTrait.TO);

    CustomSingleRel rel = new CustomSingleRel(cluster, fromTraits);
    planner.setRoot(rel);
    planner.changeTraitsUsingConverters(rel, toTraits);

    // Make sure that the equivalence set contains only the original and converted rels.
    // It should not contain the collation enforcer, because the "from" collation already
    // satisfies the "to" collation.
    List<RelNode> rels = planner.getSubset(rel).set.rels;
    assertEquals(2, rels.size());
    assertTrue(rels.stream().anyMatch((r) -> r instanceof CustomSingleRel));
    assertTrue(rels.stream().anyMatch((r) -> r instanceof CustomTraitEnforcer));
  }

  private static class CustomSingleRel extends PlannerTests.TestLeafRel {
    CustomSingleRel(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, traits, CustomSingleRel.class.getSimpleName());
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new CustomSingleRel(getCluster(), traitSet);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }
  }

  private static class CustomTraitEnforcer extends SingleRel {
    public CustomTraitEnforcer(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, traits, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new CustomTraitEnforcer(getCluster(), traitSet, inputs.get(0));
    }
  }

  private static class CustomTrait implements RelTrait {

    private static final CustomTrait FROM = new CustomTrait("FROM");
    private static final CustomTrait TO = new CustomTrait("TO");

    private final String label;

    public CustomTrait(String label) {
      this.label = label;
    }

    @SuppressWarnings("rawtypes")
    @Override public RelTraitDef getTraitDef() {
      return CustomTraitDef.INSTANCE;
    }

    @Override public boolean satisfies(RelTrait trait) {
      return this == trait;
    }

    @Override public void register(RelOptPlanner planner) {
      // No-op
    }

    @Override public String toString() {
      return label;
    }
  }

  private static class CustomTraitDef extends RelTraitDef<CustomTrait> {

    private static final CustomTraitDef INSTANCE = new CustomTraitDef();

    @Override public Class<CustomTrait> getTraitClass() {
      return CustomTrait.class;
    }

    @Override public String getSimpleName() {
      return "custom";
    }

    @Override public @Nullable RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        CustomTrait toTrait,
        boolean allowInfiniteCostConverters
    ) {
      return new CustomTraitEnforcer(
          rel.getCluster(),
          rel.getTraitSet().plus(toTrait),
          rel
      );
    }

    @Override public boolean canConvert(RelOptPlanner planner, CustomTrait fromTrait, CustomTrait toTrait) {
      return true;
    }

    @Override public CustomTrait getDefault() {
      return CustomTrait.FROM;
    }
  }
}
