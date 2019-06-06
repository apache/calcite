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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for
 * {@link org.apache.calcite.rel.core.Aggregate.Group#induce(ImmutableBitSet, List)}.
 */
public class InduceGroupingTypeTest {
  @Test public void testInduceGroupingType() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(1, 2, 4, 5);

    // SIMPLE
    final List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    // CUBE (has only one bit, so could also be ROLLUP)
    groupSets.clear();
    final ImmutableBitSet groupSet0 = ImmutableBitSet.of(2);
    groupSets.add(groupSet0);
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.CUBE,
        Aggregate.Group.induce(groupSet0, groupSets));
    assertThat(Aggregate.Group.isRollup(groupSet0, groupSets), is(true));
    assertThat(Aggregate.Group.getRollup(groupSets).toString(),
        is("[2]"));

    // CUBE
    final List<ImmutableBitSet> groupSets0 =
        ImmutableBitSet.ORDERING.sortedCopy(groupSet.powerSet());
    assertEquals(Aggregate.Group.CUBE,
        Aggregate.Group.induce(groupSet, groupSets0));
    assertThat(Aggregate.Group.isRollup(groupSet, groupSets0), is(false));

    // ROLLUP
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.ROLLUP,
        Aggregate.Group.induce(groupSet, groupSets));
    assertThat(Aggregate.Group.isRollup(groupSet, groupSets), is(true));
    assertThat(Aggregate.Group.getRollup(groupSets).toString(),
        is("[1, 2, 4, 5]"));

    // ROLLUP, not removing bits in order
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 4, 5));
    groupSets.add(ImmutableBitSet.of(4, 5));
    groupSets.add(ImmutableBitSet.of(4));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.ROLLUP,
        Aggregate.Group.induce(groupSet, groupSets));
    assertThat(Aggregate.Group.getRollup(groupSets).toString(),
        is("[4, 5, 1, 2]"));

    // ROLLUP, removing bits in reverse order
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(2, 4, 5));
    groupSets.add(ImmutableBitSet.of(4, 5));
    groupSets.add(ImmutableBitSet.of(5));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.ROLLUP,
        Aggregate.Group.induce(groupSet, groupSets));
    assertThat(Aggregate.Group.getRollup(groupSets).toString(),
        is("[5, 4, 2, 1]"));

    // OTHER
    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1, 4));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

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
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets1));

    groupSets.clear();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets.clear();
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }

  /** Tests a singleton grouping set {2}, whose power set has only two elements,
   * { {2}, {} }. */
  @Test public void testInduceGroupingType1() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(2);

    // Could be ROLLUP but we prefer CUBE
    List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.CUBE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = new ArrayList<>();
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = new ArrayList<>();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }

  @Test public void testInduceGroupingType0() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of();

    // Could be CUBE or ROLLUP but we choose SIMPLE
    List<ImmutableBitSet> groupSets = new ArrayList<>();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = new ArrayList<>();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }
}

// End InduceGroupingTypeTest.java
