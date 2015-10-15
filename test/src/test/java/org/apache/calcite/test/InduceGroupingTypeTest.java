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

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit test for
 * {@link org.apache.calcite.rel.core.Aggregate.Group#induce(ImmutableBitSet, List)}.
 */
public class InduceGroupingTypeTest {
  @Test public void testInduceGroupingType() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(1, 2, 4, 5);

    // SIMPLE
    List<ImmutableBitSet> groupSets = Lists.newArrayList();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    // CUBE
    groupSets = ImmutableBitSet.ORDERING.sortedCopy(groupSet.powerSet());
    assertEquals(Aggregate.Group.CUBE,
        Aggregate.Group.induce(groupSet, groupSets));

    // ROLLUP
    groupSets = Lists.newArrayList();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.ROLLUP,
        Aggregate.Group.induce(groupSet, groupSets));

    // OTHER
    groupSets = Lists.newArrayList();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    groupSets.add(ImmutableBitSet.of(1, 2, 4, 5));
    groupSets.add(ImmutableBitSet.of(1, 2, 4));
    groupSets.add(ImmutableBitSet.of(1, 2));
    groupSets.add(ImmutableBitSet.of(1));
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
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

    groupSets = ImmutableBitSet.ORDERING.sortedCopy(groupSets);
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }

  /** Tests a singleton grouping set {2}, whose power set has only two elements,
   * { {2}, {} }. */
  @Test public void testInduceGroupingType1() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(2);

    // Could be ROLLUP but we prefer CUBE
    List<ImmutableBitSet> groupSets = Lists.newArrayList();
    groupSets.add(groupSet);
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.CUBE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    groupSets.add(ImmutableBitSet.of());
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }

  @Test public void testInduceGroupingType0() {
    final ImmutableBitSet groupSet = ImmutableBitSet.of();

    // Could be CUBE or ROLLUP but we choose SIMPLE
    List<ImmutableBitSet> groupSets = Lists.newArrayList();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
        Aggregate.Group.induce(groupSet, groupSets));

    groupSets = Lists.newArrayList();
    assertEquals(Aggregate.Group.OTHER,
        Aggregate.Group.induce(groupSet, groupSets));
  }
}

// End InduceGroupingTypeTest.java
