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

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.calcite.rel.RelDistributions.ANY;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link RelDistribution}.
 */
class RelDistributionTest {
  @Test void testRelDistributionSatisfy() {
    RelDistribution distribution1 = RelDistributions.hash(ImmutableList.of(0));
    RelDistribution distribution2 = RelDistributions.hash(ImmutableList.of(1));

    RelTraitSet traitSet = RelTraitSet.createEmpty();
    RelTraitSet simpleTrait1 = traitSet.plus(distribution1);
    RelTraitSet simpleTrait2 = traitSet.plus(distribution2);
    RelTraitSet compositeTrait =
        traitSet.replace(RelDistributionTraitDef.INSTANCE,
            ImmutableList.of(distribution1, distribution2));

    assertThat(compositeTrait.satisfies(simpleTrait1), is(true));
    assertThat(compositeTrait.satisfies(simpleTrait2), is(true));

    assertThat(distribution1.compareTo(distribution2), is(-1));
    assertThat(distribution2.compareTo(distribution1), is(1));
    //noinspection EqualsWithItself
    assertThat(distribution2.compareTo(distribution2), is(0));
  }

  @Test void testRelDistributionMapping() {
    final int n = 10; // Mapping source count.

    // hash[0]
    RelDistribution hash0 = hash(0);
    assertThat(hash0.apply(mapping(n, 0)), is(hash0));
    assertThat(hash0.apply(mapping(n, 1)), is(ANY));
    assertThat(hash0.apply(mapping(n, 2, 1, 0)), is(hash(2)));

    // hash[0,1]
    RelDistribution hash01 = hash(0, 1);
    assertThat(hash01.apply(mapping(n, 0)), is(ANY));
    assertThat(hash01.apply(mapping(n, 1)), is(ANY));
    assertThat(hash01.apply(mapping(n, 0, 1)), is(hash01));
    assertThat(hash01.apply(mapping(n, 1, 2)), is(ANY));
    assertThat(hash01.apply(mapping(n, 1, 0)), is(hash01));
    assertThat(hash01.apply(mapping(n, 2, 1, 0)), is(hash(2, 1)));

    // hash[2]
    RelDistribution hash2 = hash(2);
    assertThat(hash2.apply(mapping(n, 0)), is(ANY));
    assertThat(hash2.apply(mapping(n, 1)), is(ANY));
    assertThat(hash2.apply(mapping(n, 2)), is(hash(0)));
    assertThat(hash2.apply(mapping(n, 1, 2)), is(hash(1)));

    // hash[9] , 9 < mapping.sourceCount()
    RelDistribution hash9 = hash(n - 1);
    assertThat(hash9.apply(mapping(n, 0)), is(ANY));
    assertThat(hash9.apply(mapping(n, 1)), is(ANY));
    assertThat(hash9.apply(mapping(n, 2)), is(ANY));
    assertThat(hash9.apply(mapping(n, n - 1)), is(hash(0)));
  }

  private static Mapping mapping(int sourceCount, int... sources) {
    return Mappings.target(ImmutableIntList.of(sources), sourceCount);
  }

  private static RelDistribution hash(int... keys) {
    return RelDistributions.hash(ImmutableIntList.of(keys));
  }

  @Test void testRangeRelDistributionKeys() {
    RelDistributions.range(Arrays.asList(0, 1));
  }

  @Test void testHashRelDistributionKeys() {
    RelDistributions.hash(Arrays.asList(0, 1));
  }
}
