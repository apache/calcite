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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Test fot ArrowSet.
 */
public class ArrowSetTest {
  @Test void testFDEqualTo() {
    ArrowSet fds1 = new ArrowSet.Builder()
        .addArrow(0, 1)
        .addArrow(1, 2)
        .build();

    ArrowSet fds2 = new ArrowSet.Builder()
        .addArrow(0, 1)
        .addArrow(1, 2)
        .addArrow(0, 2)
        .build();

    ArrowSet fds3 = new ArrowSet.Builder()
        .addArrow(1, 0)
        .addArrow(2, 1)
        .build();

    assertThat(fds1.equalTo(fds2), is(true));
    assertThat(fds2.equalTo(fds1), is(true));
    assertThat(fds1.equalTo(fds3), is(false));
    assertThat(fds3.equalTo(fds1), is(false));
  }

  @Test void testFDUnion() {
    ArrowSet fds1 = new ArrowSet.Builder()
        .addArrow(0, 1)
        .addArrow(1, 2)
        .build();

    ArrowSet fds2 = new ArrowSet.Builder()
        .addArrow(2, 3)
        .addArrow(3, 4)
        .build();

    ArrowSet union = fds1.union(fds2);

    assertThat(union.implies(ImmutableBitSet.of(0), ImmutableBitSet.of(1)), is(true));
    assertThat(union.implies(ImmutableBitSet.of(1), ImmutableBitSet.of(2)), is(true));
    assertThat(union.implies(ImmutableBitSet.of(2), ImmutableBitSet.of(3)), is(true));
    assertThat(union.implies(ImmutableBitSet.of(3), ImmutableBitSet.of(4)), is(true));
    assertThat(union.implies(ImmutableBitSet.of(0), ImmutableBitSet.of(3)), is(true));
    assertThat(union.implies(ImmutableBitSet.of(0), ImmutableBitSet.of(4)), is(true));
  }

  @Test void testDeterminantsNoFD() {
    // FD: empty
    ArrowSet fds = new ArrowSet.Builder().build();
    ImmutableBitSet ordinals = ImmutableBitSet.of(0, 1);
    Set<ImmutableBitSet> keys = fds.determinants(ordinals);
    assertThat(ImmutableSet.of(ImmutableBitSet.of(ordinals)).equals(keys), is(true));
  }

  @Test void testDeterminantsLargeStarKey() {
    // FD: 0 -> i (i = 1 .. n - 1)
    int n = 1024;
    ImmutableBitSet ordinals = ImmutableBitSet.of(IntStream.range(0, n).toArray());
    ArrowSet.Builder builder = new ArrowSet.Builder();
    for (int i = 1; i < n; i++) {
      builder.addArrow(0, i);
    }
    builder.addArrow(88, 0);
    ArrowSet fds = builder.build();
    Set<ImmutableBitSet> keys = fds.determinants(ordinals);
    assertThat(ImmutableSet.of(ImmutableBitSet.of(0), ImmutableBitSet.of(88)).equals(keys),
        is(true));
  }

  @Test public void testDeterminantsLargeAttributeSet() {
    // FD: 0 -> 1, 1 -> 2, ..., n - 2 -> n - 1
    int n = 1024;
    ImmutableBitSet ordinals = ImmutableBitSet.of(IntStream.range(0, n).toArray());
    ArrowSet.Builder builder = new ArrowSet.Builder();
    for (int i = 0; i < n; i++) {
      builder.addArrow(i, i + 1);
    }
    ArrowSet fds = builder.build();
    Set<ImmutableBitSet> keys = fds.determinants(ordinals);
    assertThat(keys, hasSize(1));
    assertThat(keys.iterator().next(), equalTo(ImmutableBitSet.of(0)));
  }

  @Test void testDependentsWithLargeRelation() {
    int numAttrs = 1024;
    ArrowSet.Builder builder = new ArrowSet.Builder();
    for (int i = 0; i < numAttrs; i++) {
      builder.addArrow(i, i + 1);
    }
    ArrowSet fds = builder.build();
    ImmutableBitSet closure = fds.dependents(ImmutableBitSet.of(0));
    for (int i = 0; i <= numAttrs; i++) {
      assertThat(closure.get(i), is(true));
    }
  }

  @Test void testTransitive() {
    // Given axioms:
    //   {a, b} -> {c}
    //   {a, e} -> {d}
    //   {c} -> {e}
    // We can prove that:
    //   {a, b} -> {e}
    //   {a, b} -> {c, e}
    //   {a, b} -> {a, c, e}
    // But not that:
    //   {a} -> {c}
    //   {a} -> {f}
    //   {a, e} -> {c}
    //   {b} -> {c}
    final ImmutableBitSet a = ImmutableBitSet.of(0);
    final ImmutableBitSet ab = ImmutableBitSet.of(0, 1);
    final ImmutableBitSet ace = ImmutableBitSet.of(0, 2, 4);
    final ImmutableBitSet b = ImmutableBitSet.of(1);
    final ImmutableBitSet c = ImmutableBitSet.of(2);
    final ImmutableBitSet ce = ImmutableBitSet.of(2, 4);
    final ImmutableBitSet d = ImmutableBitSet.of(3);
    final ImmutableBitSet e = ImmutableBitSet.of(4);
    final ImmutableBitSet ae = ImmutableBitSet.of(0, 4);
    final ImmutableBitSet f = ImmutableBitSet.of(5);

    ArrowSet fds = new ArrowSet.Builder()
        .addArrow(ab, c)
        .addArrow(ae, d)
        .addArrow(c, e)
        .build();
    assertThat(fds.implies(ab, e), is(true));
    assertThat(fds.implies(ab, ce), is(true));
    assertThat(fds.implies(ab, ace), is(true));
    assertThat(fds.implies(a, c), is(false));
    assertThat(fds.implies(a, f), is(false));
    assertThat(fds.implies(ae, c), is(false));
    assertThat(fds.implies(b, c), is(false));
  }
}
