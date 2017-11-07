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

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RelDistribution}.
 */
public class RelDistributionTest {
  @Test public void testRelDistributionSatisfy() {
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
}

// End RelDistributionTest.java
