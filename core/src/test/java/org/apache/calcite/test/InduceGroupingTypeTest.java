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

/**
 * Unit test for {@link org.apache.calcite.rel.core.Aggregate.
 * Group#induce(ImmutableBitSet, List)}.
 */
public class InduceGroupingTypeTest {

  @Test public void testInduceGroupingType() {
    ImmutableBitSet groupSet = ImmutableBitSet.of(1, 2, 4, 5);

    // SIMPLE
    List<ImmutableBitSet> groupSets = Lists.newArrayList();
    groupSets.add(groupSet);
    assertEquals(Aggregate.Group.SIMPLE,
            Aggregate.Group.induce(groupSet, groupSets));

    // CUBE
    groupSets = Lists.newArrayList(groupSet.powerSet());
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

}
