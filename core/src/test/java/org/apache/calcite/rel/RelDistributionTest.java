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

import static org.apache.calcite.rel.RelDistributions.RANDOM_DISTRIBUTED;

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
    // hash[0]
    RelDistribution hash0 = hash(0);
    assertThat(hash0.apply(mapping(0)), is(hash0));
    assertThat(hash0.apply(mapping(1)), is(RANDOM_DISTRIBUTED));
    assertThat(hash0.apply(mapping(2, 1, 0)), is(hash(2)));

    // hash[0,1]
    RelDistribution hash01 = hash(0, 1);
    assertThat(hash01.apply(mapping(0)), is(RANDOM_DISTRIBUTED));
    assertThat(hash01.apply(mapping(1)), is(RANDOM_DISTRIBUTED));
    assertThat(hash01.apply(mapping(0, 1)), is(hash01));
    assertThat(hash01.apply(mapping(1, 2)), is(RANDOM_DISTRIBUTED));
    assertThat(hash01.apply(mapping(1, 0)), is(hash01));
    assertThat(hash01.apply(mapping(2, 1, 0)), is(hash(2, 1)));

    // hash[2]
    RelDistribution hash2 = hash(2);
    assertThat(hash2.apply(mapping(0)), is(RANDOM_DISTRIBUTED));
    assertThat(hash2.apply(mapping(1)), is(RANDOM_DISTRIBUTED));
    assertThat(hash2.apply(mapping(2)), is(hash(0)));
    assertThat(hash2.apply(mapping(1, 2)), is(hash(1)));
  }

  private static Mapping mapping(int... sources) {
    return Mappings.target(ImmutableIntList.of(sources), 10);
  }

  private static RelDistribution hash(int... keys) {
    return RelDistributions.hash(ImmutableIntList.of(keys));
  }
}
