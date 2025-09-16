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

    // Add some FDs: A -> B, B -> C
    fdSet.addFD(0, 1); // A -> B
    fdSet.addFD(1, 2); // B -> C

    // Test closure: {A}+ should include A, B, C
    ImmutableBitSet closure = fdSet.closure(ImmutableBitSet.of(0));
    assertThat(closure.get(0), is(true)); // A
    assertThat(closure.get(1), is(true)); // B (A -> B)
    assertThat(closure.get(2), is(true)); // C (A -> B, B -> C, so A -> C by transitivity)

    // Test determines
    assertThat(fdSet.determines(0, 1), is(true)); // A -> B (direct)
    assertThat(fdSet.determines(0, 2), is(true)); // A -> C (transitive)
    assertThat(fdSet.determines(2, 0), is(false)); // C does not determine A
  }

  @Test void testMinimalCover() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Add redundant FDs
    fdSet.addFD(ImmutableBitSet.of(0), ImmutableBitSet.of(1)); // A -> B
    fdSet.addFD(ImmutableBitSet.of(1), ImmutableBitSet.of(2)); // B -> C
    fdSet.addFD(ImmutableBitSet.of(0), ImmutableBitSet.of(2)); // A -> C

    FunctionalDependencySet minimal = fdSet.minimalCover();

    // The minimal cover should not contain A -> C since it's implied by A -> B and B -> C
    assertThat(
        minimal.implies(ImmutableBitSet.of(0),
        ImmutableBitSet.of(1)), is(true)); // A -> B
    assertThat(
        minimal.implies(ImmutableBitSet.of(1),
        ImmutableBitSet.of(2)), is(true)); // B -> C
    assertThat(
        minimal.implies(ImmutableBitSet.of(0),
        ImmutableBitSet.of(2)), is(true)); // A -> C (derived)

    // Should be equivalent to original
    assertThat(fdSet.equalTo(minimal), is(true));
  }

  @Test void testKeyFinding() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Schema: A, B, C, D with FDs: A -> B, BC -> D
    fdSet.addFD(0, 1); // A -> B
    fdSet.addFD(ImmutableBitSet.of(1, 2), ImmutableBitSet.of(3)); // BC -> D

    ImmutableBitSet allAttributes = ImmutableBitSet.of(0, 1, 2, 3);
    Set<ImmutableBitSet> keys = fdSet.findKeys(allAttributes);

    // AC should be a key: A -> B, and BC -> D, so AC -> ABCD
    assertThat(keys, containsInAnyOrder(ImmutableBitSet.of(0, 2)));

    // Verify it's actually a key
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 2), allAttributes), is(true));

    // Verify non-keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0), allAttributes),
        is(false)); // A alone is not a key
    assertThat(fdSet.isKey(ImmutableBitSet.of(2), allAttributes),
        is(false)); // C alone is not a key
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
    fdSet1.addFD(0, 1); // A -> B
    fdSet1.addFD(1, 2); // B -> C

    FunctionalDependencySet fdSet2 = new FunctionalDependencySet();
    fdSet2.addFD(0, 1); // A -> B
    fdSet2.addFD(1, 2); // B -> C
    fdSet2.addFD(0, 2); // A -> C (redundant)

    // Should be equivalent despite fdSet2 having a redundant FD
    assertThat(fdSet1.equalTo(fdSet2), is(true));
    assertThat(fdSet2.equalTo(fdSet1), is(true));
  }

  @Test void testUnion() {
    FunctionalDependencySet fdSet1 = new FunctionalDependencySet();
    fdSet1.addFD(0, 1); // A -> B

    FunctionalDependencySet fdSet2 = new FunctionalDependencySet();
    fdSet2.addFD(1, 2); // B -> C

    FunctionalDependencySet union = fdSet1.union(fdSet2);

    assertThat(union.determines(0, 1), is(true)); // A -> B
    assertThat(union.determines(1, 2), is(true)); // B -> C
    assertThat(union.determines(0, 2), is(true)); // A -> C (transitive)
  }

  @Test void testMultipleCandidateKeys() {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    // Schema: StudentID(0), Email(1), CourseID(2), Grade(3)
    // FDs: StudentID <-> Email (bidirectional unique mapping)
    //      {StudentID, CourseID} -> Grade
    //      {Email, CourseID} -> Grade
    fdSet.addFD(0, 1); // StudentID -> Email
    fdSet.addFD(1, 0); // Email -> StudentID
    fdSet.addFD(ImmutableBitSet.of(0, 2),
        ImmutableBitSet.of(3)); // {StudentID, CourseID} -> Grade
    fdSet.addFD(ImmutableBitSet.of(1, 2),
        ImmutableBitSet.of(3)); // {Email, CourseID} -> Grade

    ImmutableBitSet allAttributes = ImmutableBitSet.of(0, 1, 2, 3);
    Set<ImmutableBitSet> keys = fdSet.findKeys(allAttributes);

    // Should have two candidate keys
    assertThat(keys, hasSize(2));

    // Both {StudentID, CourseID} and {Email, CourseID} should be candidate keys
    assertThat(
        keys,
        containsInAnyOrder(ImmutableBitSet.of(0, 2),
            ImmutableBitSet.of(1, 2)));

    // Verify both are actually keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 2), allAttributes), is(true));
    assertThat(fdSet.isKey(ImmutableBitSet.of(1, 2), allAttributes), is(true));

    // Verify that individual attributes are not keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0), allAttributes),
        is(false)); // StudentID alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(1), allAttributes),
        is(false)); // Email alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(2), allAttributes),
        is(false)); // CourseID alone
    assertThat(fdSet.isKey(ImmutableBitSet.of(3), allAttributes),
        is(false)); // Grade alone

    // Verify superkeys (should not be minimal keys)
    assertThat(fdSet.isSuperkey(ImmutableBitSet.of(0, 1, 2), allAttributes),
        is(true)); // Contains both candidate keys
    assertThat(fdSet.isKey(ImmutableBitSet.of(0, 1, 2), allAttributes),
        is(false)); // Not minimal
  }

  @Test void testProjectFunctionalDependencies() {
    // Test basic FD set operations to ensure our implementation works
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
