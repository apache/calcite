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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

/**
 * Test to verify {@link RelCompositeTrait}.
 */
public class RelTraitTest {
  private static final RelCollationTraitDef COLLATION = RelCollationTraitDef.INSTANCE;

  private void assertCanonical(String message, Supplier<List<RelCollation>> collation) {
    RelTrait trait1 = RelCompositeTrait.of(COLLATION, collation.get());
    RelTrait trait2 = RelCompositeTrait.of(COLLATION, collation.get());

    Assert.assertEquals(
        "RelCompositeTrait.of should return the same instance for " + message,
        trait1 + " @" + Integer.toHexString(System.identityHashCode(trait1)),
        trait2 + " @" + Integer.toHexString(System.identityHashCode(trait2)));
  }

  @Test public void compositeEmpty() {
    assertCanonical("empty composite", ImmutableList::of);
  }

  @Test public void compositeOne() {
    assertCanonical("composite with one element",
        () -> ImmutableList.of(RelCollations.of(ImmutableList.of())));
  }

  @Test public void compositeTwo() {
    assertCanonical("composite with two elements",
        () -> ImmutableList.of(RelCollations.of(0), RelCollations.of(1)));
  }

  private void assertSatisfies(RelTrait a, RelTrait b) {
    Assert.assertTrue(a + ".satisfies(" + b + ")", a.satisfies(b));
    if (!a.equals(b)) {
      // a should be "the same or stricter" than b, so b cannot be stricter than a at the same time
      Assert.assertTrue(b + ".NOTsatisfies(" + a + ")", !b.satisfies(a));
    }
  }

  /**
   * Tests for {@link RelCompositeTrait#satisfies(RelTrait)}.
   */
  @Test public void compositeSatisfies() {
    //noinspection unchecked
    RelCompositeTrait<RelCollation> abc_bc_c = (RelCompositeTrait) RelCompositeTrait.of(COLLATION,
        ImmutableList.of(
            RelCollations.of(
                new RelFieldCollation(0),
                new RelFieldCollation(1),
                new RelFieldCollation(2)),
            RelCollations.of(
                new RelFieldCollation(1),
                new RelFieldCollation(2)),
            RelCollations.of(
                new RelFieldCollation(2))));

    // A composite trait must satisfy its sub-traits
    for (RelCollation collation : abc_bc_c.traitList()) {
      assertSatisfies(abc_bc_c, collation);
    }

    // A trait must satisfy to itself
    assertSatisfies(abc_bc_c, abc_bc_c);

    //noinspection unchecked
    RelCompositeTrait<RelCollation> bc_c = (RelCompositeTrait) RelCompositeTrait.of(COLLATION,
        ImmutableList.of(
            RelCollations.of(
                new RelFieldCollation(1),
                new RelFieldCollation(2)),
            RelCollations.of(
                new RelFieldCollation(2))));

    assertSatisfies(abc_bc_c, bc_c);

    //noinspection unchecked
    RelCompositeTrait<RelCollation> b_c = (RelCompositeTrait) RelCompositeTrait.of(COLLATION,
        ImmutableList.of(
            RelCollations.of(
                new RelFieldCollation(1)),
            RelCollations.of(
                new RelFieldCollation(2))));

    assertSatisfies(abc_bc_c, b_c);
  }
}

// End RelTraitTest.java
