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
package org.apache.calcite.test;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for
 * {@link org.apache.calcite.rel.core.Aggregate.Group#induce(ImmutableBitSet, List)}.
 */
class InduceGroupingTypeTest {
  @Test void testInduceGroupingType() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(1, 2, 4, 5);

    // SIMPLE
    final List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.SIMPLE));

    // CUBE (has only one bit, so could also be ROLLUP)
    groupSets.clear();
    final ImmutableBitSet groupSet0 = ImmutableBitSet.of(2);
    groupSets.add(groupSet0);
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet0, groupSets),
        is(Aggregate.Group.CUBE));
    assertThat(Aggregate.Group.isRollup(groupSet0, groupSets), is(true));
    assertThat(Aggregate.Group.getRollup(groupSets),
        hasToString("[2]"));

    // CUBE
    final List<ImmutableBitSet> groupSets0 =
        ImmutableBitSet.ORDERING.sortedCopy(groupSet.powerSet());
    assertThat(Aggregate.Group.induce(groupSet, groupSets0),
        is(Aggregate.Group.CUBE));
    assertThat(Aggregate.Group.isRollup(groupSet, groupSets0), is(false));

    // ROLLUP
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.ROLLUP));
    assertThat(Aggregate.Group.isRollup(groupSet, groupSets), is(true));
    assertThat(Aggregate.Group.getRollup(groupSets),
        hasToString("[1, 2, 4, 5]"));

    // ROLLUP, not removing bits in order
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 4, 5));
    groupSets.add(ImmutableBitSet.of(4, 5));
    groupSets.add(ImmutableBitSet.of(4));
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.ROLLUP));
    assertThat(Aggregate.Group.getRollup(groupSets),
        hasToString("[4, 5, 1, 2]"));

    // ROLLUP, removing bits in reverse order
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(2, 4, 5));
    groupSets.add(ImmutableBitSet.of(4, 5));
    groupSets.add(ImmutableBitSet.of(5));
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.ROLLUP));
    assertThat(Aggregate.Group.getRollup(groupSets),
        hasToString("[5, 4, 2, 1]"));

    // OTHER
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1, 4));
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    groupSets.add(ImmutableBitSet.of());

    try {
      final Aggregate.Group x = Aggregate.Group.induce(groupSet, groupSets);
      fail("expected error, got " + x);
    } catch (IllegalArgumentException ignore) {
      // ok
    }

    List<ImmutableBitSet> groupSets1 =
        ImmutableBitSet.ORDERING.sortedCopy(groupSets);
    assertThat(Aggregate.Group.induce(groupSet, groupSets1),
        is(Aggregate.Group.OTHER));

    groupSets.clear();
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));
  }

  /** Tests a singleton grouping set {2}, whose power set has only two elements,
   * { {2}, {} }. */
  @Test void testInduceGroupingType1() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(2);

    // Could be ROLLUP but we prefer CUBE
    List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.CUBE));

    groupSets = new ArrayList<>();
    groupSets.add(ImmutableBitSet.of());
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));

    groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.SIMPLE));

    groupSets = new ArrayList<>();
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));
  }

  @Test void testInduceGroupingType0() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of();

    // Could be CUBE or ROLLUP but we choose SIMPLE
    List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.SIMPLE));

    groupSets = new ArrayList<>();
    assertThat(Aggregate.Group.induce(groupSet, groupSets),
        is(Aggregate.Group.OTHER));
  }
}
