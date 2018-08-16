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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AbstractRelNode}.
 */
public class AbstractRelNodeTest {
  private static final DummyTraitDef DUMMY_TRAIT_DEF_INSTANCE = new DummyTraitDef();

  private static final DummyTrait TRAIT1_INSTANCE = new DummyTrait("trait1");
  private static final DummyTrait TRAIT2_INSTANCE = new DummyTrait("trait2");

  /**
   * Dummy trait for test
   */
  private static class DummyTrait implements RelTrait {
    private final String name;

    DummyTrait(String name) {
      this.name = name;
    }

    @Override public String toString() {
      return name;
    }

    @Override public RelTraitDef getTraitDef() {
      return DUMMY_TRAIT_DEF_INSTANCE;
    }

    @Override public boolean satisfies(RelTrait trait) {
      return trait == this;

    }

    @Override public void register(RelOptPlanner planner) {}
  }

  /**
   * Dummy trait def for test
   */
  private static class DummyTraitDef
      extends RelTraitDef<DummyTrait> {

    @Override public Class<DummyTrait> getTraitClass() {
      return DummyTrait.class;
    }

    @Override public String toString() {
      return getSimpleName();
    }

    @Override public String getSimpleName() {
      return "ConvertRelDistributionTraitDef";
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel,
        DummyTrait toTrait, boolean allowInfiniteCostConverters) {
      return null;
    }

    @Override public boolean canConvert(RelOptPlanner planner,
        DummyTrait fromTrait, DummyTrait toTrait) {
      return fromTrait == toTrait;

    }

    @Override public DummyTrait getDefault() {
      return TRAIT1_INSTANCE;
    }
  }

  /**
   * Dummy relnode class for test
   */
  private static class DummyRelNode extends AbstractRelNode {
    private final ImmutableList<RelNode> inputs;
    private final ImmutableMap<String, Object> attributes;

    private DummyRelNode(RelTraitSet traits, ImmutableList<RelNode> inputs,
        ImmutableMap<String, Object> attributes) {
      super(null, traits);
      this.inputs = inputs;
      this.attributes = attributes;
    }

    @Override public List<RelNode> getInputs() {
      return inputs;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      super.explainTerms(pw);
      for (Ord<RelNode> input: Ord.zip(inputs)) {
        pw.input("input#" + input.i, input.e);
      }
      for (Map.Entry<String, Object> attribute: attributes.entrySet()) {
        pw.item(attribute.getKey(), attribute.getValue());
      }
      return pw;
    }
  }

  /**
   * Dummy relnode subclass for test
   */
  private static final class OtherDummyRelNode extends DummyRelNode {
    private OtherDummyRelNode(RelTraitSet traits, ImmutableList<RelNode> inputs,
        ImmutableMap<String, Object> attributes) {
      super(traits, inputs, attributes);
    }
  }

  private static final RelTraitSet EMPTY_TRAITSET_INSTANCE = RelTraitSet.createEmpty();
  private static final RelTraitSet TRAITSET1_INSTANCE = RelTraitSet.createEmpty()
      .plus(TRAIT1_INSTANCE);
  private static final RelTraitSet TRAITSET2_INSTANCE = RelTraitSet.createEmpty()
      .plus(TRAIT2_INSTANCE);

  @Test public void testRecomputeDigest() {
    DummyRelNode node1 = new DummyRelNode(EMPTY_TRAITSET_INSTANCE, ImmutableList.of(),
        ImmutableMap.of("foo", "bar", "foo2", "bar2"));
    DummyRelNode node2 = new OtherDummyRelNode(TRAITSET1_INSTANCE, ImmutableList.of(node1),
        ImmutableMap.of());
    DummyRelNode node3 = new DummyRelNode(TRAITSET2_INSTANCE, ImmutableList.of(),
        ImmutableMap.of("integer", 10, "long", 20L));
    DummyRelNode node4 = new OtherDummyRelNode(EMPTY_TRAITSET_INSTANCE,
        ImmutableList.of(node2, node3), ImmutableMap.of("other", "value"));

    assertEquals("DummyRelNode#" + node1.getId(), node1.getDigest());
    assertEquals("DummyRelNode#" + node1.getId(), node1.getDescription());
    assertEquals("OtherDummyRelNode#" + node2.getId(), node2.getDigest());
    assertEquals("OtherDummyRelNode#" + node2.getId(), node2.getDescription());
    assertEquals("DummyRelNode#" + node3.getId(), node3.getDigest());
    assertEquals("DummyRelNode#" + node3.getId(), node3.getDescription());
    assertEquals("OtherDummyRelNode#" + node4.getId(), node4.getDigest());
    assertEquals("OtherDummyRelNode#" + node4.getId(), node4.getDescription());

    // recompute all digests
    node1.recomputeDigest();
    node2.recomputeDigest();
    node3.recomputeDigest();
    node4.recomputeDigest();

    String expectedDigestNode1 = "DummyRelNode(foo=bar,foo2=bar2)";
    assertEquals(expectedDigestNode1, node1.getDigest());
    assertEquals("rel#" + node1.getId() + ":" + expectedDigestNode1, node1.getDescription());

    String expectedDigestNode2 = "OtherDummyRelNode.trait1(input#0=DummyRelNode#" + node1.getId()
        + ")";
    assertEquals(expectedDigestNode2, node2.getDigest());
    assertEquals("rel#" + node2.getId() + ":" + expectedDigestNode2, node2.getDescription());

    String expectedDigestNode3 = "DummyRelNode.trait2(integer=10,long=20)";
    assertEquals(expectedDigestNode3, node3.getDigest());
    assertEquals("rel#" + node3.getId() + ":" + expectedDigestNode3, node3.getDescription());

    String expectedDigestNode4 = "OtherDummyRelNode(input#0=OtherDummyRelNode#" + node2.getId()
        + ",input#1=DummyRelNode#" + node3.getId() + ",other=value)";
    assertEquals(expectedDigestNode4, node4.getDigest());
    assertEquals("rel#" + node4.getId() + ":" + expectedDigestNode4, node4.getDescription());

  }
}

// End AbstractRelNodeTest.java
