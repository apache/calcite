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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link RelNode#getInputCount()}, {@link SingleRel}, and
 * {@link BiRel} input access.
 */
class SingleBiRelTest {

  private static RelOptCluster newCluster() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  private static RelNode newValues(RelOptCluster cluster, String value) {
    final RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    return builder.values(new String[]{"x"}, value).build();
  }

  @Test void testDefaultGetInputCount() {
    final RelOptCluster cluster = newCluster();
    final RelNode leaf = newValues(cluster, "leaf");
    assertThat(leaf.getInputCount(), is(0));
    assertThrows(IndexOutOfBoundsException.class, () -> leaf.getInput(0));

    final RelNode input1 = newValues(cluster, "a");
    final RelNode input2 = newValues(cluster, "b");
    final RelNode input3 = newValues(cluster, "c");
    final RelNode union = LogicalUnion.create(Arrays.asList(input1, input2, input3), true);
    assertThat(union.getInputCount(), is(3));
    assertThat(union.getInput(0), sameInstance(input1));
    assertThat(union.getInput(1), sameInstance(input2));
    assertThat(union.getInput(2), sameInstance(input3));
    assertThrows(IndexOutOfBoundsException.class, () -> union.getInput(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> union.getInput(3));
  }

  @Test void testSingleRelInputAccess() {
    final RelOptCluster cluster = newCluster();
    final RelNode input1 = newValues(cluster, "a");
    final RelNode input2 = newValues(cluster, "b");
    final RelNode input3 = newValues(cluster, "c");

    final MutableSingleRel rel = new MutableSingleRel(cluster, input1);
    assertThat(rel.getInputCount(), is(1));
    assertThat(rel.getInput(0), sameInstance(input1));
    assertThrows(IndexOutOfBoundsException.class, () -> rel.getInput(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> rel.getInput(1));

    final List<RelNode> inputs1 = rel.getInputs();
    assertThat(inputs1, is(Collections.singletonList(input1)));

    // replaceInput updates input without mutating previously returned snapshots
    rel.replaceInput(0, input2);
    assertThat(rel.getInputCount(), is(1));
    assertThat(rel.getInput(0), sameInstance(input2));
    final List<RelNode> inputs2 = rel.getInputs();
    assertThat(inputs2, is(Collections.singletonList(input2)));
    assertThat(inputs1, is(Collections.singletonList(input1)));

    // Direct subclass mutation of the protected input field is reflected immediately
    rel.setInputDirectly(input3);
    assertThat(rel.getInputCount(), is(1));
    assertThat(rel.getInput(0), sameInstance(input3));
    final List<RelNode> inputs3 = rel.getInputs();
    assertThat(inputs3, is(Collections.singletonList(input3)));
    assertThat(inputs2, is(Collections.singletonList(input2)));
  }

  @Test void testBiRelInputAccess() {
    final RelOptCluster cluster = newCluster();
    final RelNode left1 = newValues(cluster, "l1");
    final RelNode right1 = newValues(cluster, "r1");
    final RelNode left2 = newValues(cluster, "l2");
    final RelNode right2 = newValues(cluster, "r2");
    final RelNode left3 = newValues(cluster, "l3");
    final RelNode right3 = newValues(cluster, "r3");

    final MutableBiRel rel = new MutableBiRel(cluster, left1, right1);
    assertThat(rel.getInputCount(), is(2));
    assertThat(rel.getInput(0), sameInstance(left1));
    assertThat(rel.getInput(1), sameInstance(right1));
    assertThrows(IndexOutOfBoundsException.class, () -> rel.getInput(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> rel.getInput(2));

    final List<RelNode> inputs1 = rel.getInputs();
    assertThat(inputs1, is(Arrays.asList(left1, right1)));

    // replaceInput updates inputs without mutating previously returned snapshots
    rel.replaceInput(0, left2);
    assertThat(rel.getInputCount(), is(2));
    assertThat(rel.getInput(0), sameInstance(left2));
    assertThat(rel.getInput(1), sameInstance(right1));
    final List<RelNode> inputs2 = rel.getInputs();
    assertThat(inputs2, is(Arrays.asList(left2, right1)));
    assertThat(inputs1, is(Arrays.asList(left1, right1)));

    rel.replaceInput(1, right2);
    assertThat(rel.getInputCount(), is(2));
    assertThat(rel.getInput(0), sameInstance(left2));
    assertThat(rel.getInput(1), sameInstance(right2));
    final List<RelNode> inputs3 = rel.getInputs();
    assertThat(inputs3, is(Arrays.asList(left2, right2)));
    assertThat(inputs2, is(Arrays.asList(left2, right1)));

    // Direct subclass mutation of protected left/right fields is reflected immediately
    rel.setLeftDirectly(left3);
    assertThat(rel.getInputCount(), is(2));
    assertThat(rel.getInput(0), sameInstance(left3));
    assertThat(rel.getInput(1), sameInstance(right2));
    final List<RelNode> inputs4 = rel.getInputs();
    assertThat(inputs4, is(Arrays.asList(left3, right2)));
    assertThat(inputs3, is(Arrays.asList(left2, right2)));

    rel.setRightDirectly(right3);
    assertThat(rel.getInputCount(), is(2));
    assertThat(rel.getInput(0), sameInstance(left3));
    assertThat(rel.getInput(1), sameInstance(right3));
    final List<RelNode> inputs5 = rel.getInputs();
    assertThat(inputs5, is(Arrays.asList(left3, right3)));
    assertThat(inputs4, is(Arrays.asList(left3, right2)));
  }

  /** Subclass of {@link SingleRel} that mutates {@code input} directly. */
  private static class MutableSingleRel extends SingleRel {
    MutableSingleRel(RelOptCluster cluster, RelNode input) {
      super(cluster, cluster.traitSet(), input);
    }

    void setInputDirectly(RelNode newInput) {
      this.input = newInput;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MutableSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Subclass of {@link BiRel} that mutates {@code left} and {@code right} directly. */
  private static class MutableBiRel extends BiRel {
    MutableBiRel(RelOptCluster cluster, RelNode left, RelNode right) {
      super(cluster, cluster.traitSet(), left, right);
    }

    void setLeftDirectly(RelNode newLeft) {
      this.left = newLeft;
    }

    void setRightDirectly(RelNode newRight) {
      this.right = newRight;
    }

    @Override protected RelDataType deriveRowType() {
      return left.getRowType();
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MutableBiRel(getCluster(), inputs.get(0), inputs.get(1));
    }
  }
}
