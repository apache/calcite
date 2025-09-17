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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for {@link FunctionalDependency} and {@link FunctionalDependencySet}.
 */
public class FunctionalDependencyTest {

  @Test void testFunctionalDependencyBasic() {
    // Test FD creation and basic properties
    FunctionalDependency fd = FunctionalDependency.of(new int[]{0}, new int[]{1});
    assertThat(fd.getDeterminants(), equalTo(ImmutableBitSet.of(0)));
    assertThat(fd.getDependents(), equalTo(ImmutableBitSet.of(1)));
    assertThat(fd.isTrivial(), is(false));

    // Test trivial FD
    FunctionalDependency trivialFd = FunctionalDependency.of(new int[]{0, 1}, new int[]{0});
    assertThat(trivialFd.isTrivial(), is(true));
  }

  @Test void testFunctionalDependencySet() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Add some FDs: 0 -> 1, 1 -> 2
    fdSet.addFD(0, 1); // 0 -> 1
    fdSet.addFD(1, 2); // 1 -> 2

    // Test closure: {0}+ should include 0, 1, 2
    ImmutableBitSet closure = fdSet.closure(ImmutableBitSet.of(0));
    assertThat(closure.get(0), is(true)); // 0
    assertThat(closure.get(1), is(true)); // 1 (0 -> 1)
    assertThat(closure.get(2), is(true)); // 2 (0 -> 1, 1 -> 2, so 0 -> 2 by transitivity)

    // Test determines
    assertThat(fdSet.determines(0, 1), is(true)); // 0 -> 1 (direct)
    assertThat(fdSet.determines(0, 2), is(true)); // 0 -> 2 (transitive)
    assertThat(fdSet.determines(2, 0), is(false)); // 2 doesn't determine 0
  }

  @Test void testMinimalCover() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Add redundant FDs
    fdSet.addFD(ImmutableBitSet.of(0), ImmutableBitSet.of(1)); // 0 -> 1
    fdSet.addFD(ImmutableBitSet.of(1), ImmutableBitSet.of(2)); // 1 -> 2
    fdSet.addFD(ImmutableBitSet.of(0), ImmutableBitSet.of(2)); // 0 -> 2

    FunctionalDependencySet minimal = fdSet.minimalCover();

    // The minimal cover should not contain 0 -> 2 since it's implied by 0 -> 1 and 1 -> 2
    assertThat(
        minimal.implies(ImmutableBitSet.of(0),
        ImmutableBitSet.of(1)), is(true)); // 0 -> 1
    assertThat(
        minimal.implies(ImmutableBitSet.of(1),
        ImmutableBitSet.of(2)), is(true)); // 1 -> 2
    assertThat(
        minimal.implies(ImmutableBitSet.of(0),
        ImmutableBitSet.of(2)), is(true)); // 0 -> 2 (derived)

    // Should be equivalent to original
    assertThat(fdSet.equalTo(minimal), is(true));
  }

  @Test void testKeyFinding() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Schema: 0, 1, 2, 3 with FDs: 0 -> 1, {1,2} -> 3
    fdSet.addFD(0, 1); // 0 -> 1
    fdSet.addFD(ImmutableBitSet.of(1, 2), ImmutableBitSet.of(3)); // {1,2} -> 3

    ImmutableBitSet allAttributes = FunctionalDependencySet.allAttributesFromFds(fdSet);
    Set<ImmutableBitSet> keys = fdSet.findKeys(allAttributes);

    // {0,2} should be a key: 0 -> 1, and {1,2} -> 3, so {0,2} -> {0,1,2,3}
    assertThat(keys, containsInAnyOrder(ImmutableBitSet.of(0, 2)));

    // Verify it's actually a key
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 2), allAttributes), is(true));

    // Verify non-keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0), allAttributes),
        is(false)); // 0 alone is not a key
    assertThat(fdSet.isKey(ImmutableBitSet.of(2), allAttributes),
        is(false)); // 2 alone is not a key
  }

  @Test void testSplit() {
    FunctionalDependency fd = FunctionalDependency.of(new int[]{0}, new int[]{1, 2, 3});
    Set<FunctionalDependency> split = fd.split();

    assertThat(split, hasSize(3));
    assertThat(
        split,
        containsInAnyOrder(FunctionalDependency.of(0, 1),
            FunctionalDependency.of(0, 2),
            FunctionalDependency.of(0, 3)));
  }

  @Test void testEquivalence() {
    FunctionalDependencySet fdSet1 = new FunctionalDependencySet();
    fdSet1.addFD(0, 1); // 0 -> 1
    fdSet1.addFD(1, 2); // 1 -> 2

    FunctionalDependencySet fdSet2 = new FunctionalDependencySet();
    fdSet2.addFD(0, 1); // 0 -> 1
    fdSet2.addFD(1, 2); // 1 -> 2
    fdSet2.addFD(0, 2); // 0 -> 2 (redundant)

    // Should be equivalent despite fdSet2 having a redundant FD
    assertThat(fdSet1.equalTo(fdSet2), is(true));
    assertThat(fdSet2.equalTo(fdSet1), is(true));
  }

  @Test void testUnion() {
    FunctionalDependencySet fdSet1 = new FunctionalDependencySet();
    fdSet1.addFD(0, 1); // 0 -> 1

    FunctionalDependencySet fdSet2 = new FunctionalDependencySet();
    fdSet2.addFD(1, 2); // 1 -> 2

    FunctionalDependencySet union = fdSet1.union(fdSet2);

    assertThat(union.determines(0, 1), is(true)); // 0 -> 1
    assertThat(union.determines(1, 2), is(true)); // 1 -> 2
    assertThat(union.determines(0, 2), is(true)); // 0 -> 2 (transitive)
  }

  @Test void testMultipleCandidateKeys() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // FDs: 0 <-> 1 (bidirectional unique mapping)
    //      {0,2} -> 3
    //      {1,2} -> 3
    fdSet.addFD(0, 1); // 0 -> 1
    fdSet.addFD(1, 0); // 1 -> 0
    fdSet.addFD(ImmutableBitSet.of(0, 2), ImmutableBitSet.of(3)); // {0,2} -> 3
    fdSet.addFD(ImmutableBitSet.of(1, 2), ImmutableBitSet.of(3)); // {1,2} -> 3

    ImmutableBitSet allAttributes = ImmutableBitSet.of(0, 1, 2, 3);
    Set<ImmutableBitSet> keys = fdSet.findKeys(allAttributes);

    // Should have two candidate keys
    assertThat(keys, hasSize(2));

    // Both {0,2} and {1,2} should be candidate keys
    assertThat(
        keys, containsInAnyOrder(ImmutableBitSet.of(0, 2), ImmutableBitSet.of(1, 2)));

    // Verify both are actually keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 2), allAttributes), is(true));
    assertThat(fdSet.isKey(ImmutableBitSet.of(1, 2), allAttributes), is(true));

    // Verify that individual attributes are not keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0), allAttributes), is(false)); // 0 alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(1), allAttributes), is(false)); // 1 alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(2), allAttributes), is(false)); // 2 alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(3), allAttributes), is(false)); // 3 alone

    // Verify superkeys (should not be minimal keys)
    assertThat(fdSet.isSuperkey(ImmutableBitSet.of(0, 1, 2), allAttributes), is(true));
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 1, 2), allAttributes), is(false));
  }

  @Test void testProjectFunctionalDependencies() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();
    fdSet.addFD(0, 1);  // 0 -> 1
    fdSet.addFD(1, 2);  // 1 -> 2

    // Test closure: {0}+ should include {0, 1, 2}
    ImmutableBitSet closure = fdSet.closure(ImmutableBitSet.of(0));
    assertThat(closure, equalTo(ImmutableBitSet.of(0, 1, 2)));

    // Test key finding
    Set<ImmutableBitSet> keys = fdSet.findKeys(ImmutableBitSet.of(0, 1, 2));
    assertThat(keys, hasSize(1));
    assertThat(keys, containsInAnyOrder(ImmutableBitSet.of(0)));
  }
}
