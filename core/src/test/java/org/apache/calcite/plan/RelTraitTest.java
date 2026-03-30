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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;

/**
 * Test to verify {@link RelCompositeTrait} and {@link RelTraitSet}.
 */
class RelTraitTest {
  private static final RelCollationTraitDef COLLATION = RelCollationTraitDef.INSTANCE;

  private void assertCanonical(String message, Supplier<List<RelCollation>> collation) {
    RelTrait trait1 = RelCompositeTrait.of(COLLATION, collation.get());
    RelTrait trait2 = RelCompositeTrait.of(COLLATION, collation.get());

    assertThat("RelCompositeTrait.of should return the same instance for "
            + message,
        trait1 + " @" + toHexString(identityHashCode(trait1)),
        is(trait2 + " @" + toHexString(identityHashCode(trait2))));
  }

  @Test void compositeEmpty() {
    assertCanonical("empty composite", ImmutableList::of);
  }

  @Test void compositeOne() {
    assertCanonical("composite with one element",
        () -> ImmutableList.of(RelCollations.of(ImmutableList.of())));
  }

  @Test void compositeTwo() {
    assertCanonical("composite with two elements",
        () -> ImmutableList.of(RelCollations.of(0), RelCollations.of(1)));
  }

  @Test void testTraitSetDefault() {
    RelTraitSet traits = RelTraitSet.createEmpty();
    traits = traits.plus(Convention.NONE).plus(RelCollations.EMPTY);
    assertThat(traits, hasSize(2));
    assertTrue(traits.isDefault());
    traits = traits.replace(EnumerableConvention.INSTANCE);
    assertFalse(traits.isDefault());
    assertTrue(traits.isDefaultSansConvention());
    traits = traits.replace(RelCollations.of(0));
    assertFalse(traits.isDefault());
    assertFalse(traits.replace(Convention.NONE).isDefaultSansConvention());
    assertTrue(traits.getDefault().isDefault());
    traits = traits.getDefaultSansConvention();
    assertFalse(traits.isDefault());
    assertThat(EnumerableConvention.INSTANCE, is(traits.getConvention()));
    assertTrue(traits.isDefaultSansConvention());
    assertThat("ENUMERABLE.[]", is(traits.toString()));
  }

  @Test void testTraitSetEqual() {
    RelTraitSet traits = RelTraitSet.createEmpty();
    RelTraitSet traits1 = traits.plus(Convention.NONE).plus(RelCollations.of(0));
    assertThat(traits1, hasSize(2));
    RelTraitSet traits2 = traits1.replace(EnumerableConvention.INSTANCE);
    assertThat(traits2, hasSize(2));
    assertNotEquals(traits1, traits2);
    assertTrue(traits1.equalsSansConvention(traits2));
    RelTraitSet traits3 = traits2.replace(RelCollations.of(1));
    assertFalse(traits3.equalsSansConvention(traits2));
  }

  /** Test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7431">[CALCITE-7431]
   * RelTraitSet#getTrait seems to mishandle RelCompositeTrait</a>. */
  @Test void testRelCompositeTrait() {
    // Build:  EMP -> Sort(MGR asc) -> Project(MGR, MGR as MGR2)
    // The project maps both output columns 0 and 1 back to input column 3
    // (MGR), so the planner derives two collations: [0 ASC] and [1 ASC], which
    // are stored as a RelCompositeTrait in the output trait set.
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder b = RelBuilder.create(config);
    final RelNode in = b
        .scan("EMP")
        .sort(3)                                          // MGR asc
        .project(b.field(3), b.alias(b.field(3), "MGR2")) // MGR, MGR as MGR2
        .build();

    final RelTraitSet traitSet = in.getTraitSet();

    final List<RelCollation> collations = traitSet.getCollations();
    assertTrue(collations.size() >= 2,
        "getCollations() should expose all composite collations");

    assertThrows(IllegalStateException.class, traitSet::getCollation,
        "getCollation() should throw when a RelCompositeTrait is present");

    assertThrows(IllegalStateException.class,
        () -> traitSet.getTrait(RelCollationTraitDef.INSTANCE),
        "getTrait() should throw when a RelCompositeTrait is present");
  }
}
