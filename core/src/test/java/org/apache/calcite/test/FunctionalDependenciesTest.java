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

import org.apache.calcite.rel.metadata.FunctionalDependence;
import org.apache.calcite.rel.metadata.FunctionalDependencies;

import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * FunctionalDependenciesTest.
 */
public class FunctionalDependenciesTest {

  @Test void testFDEqualTo() {
    FunctionalDependencies fds1 = new FunctionalDependencies.Builder()
        .addFD(0, 1)
        .addFD(1, 2)
        .build();

    FunctionalDependencies fds2 = new FunctionalDependencies.Builder()
        .addFD(0, 1)
        .addFD(1, 2)
        .addFD(0, 2)
        .build();

    FunctionalDependencies fds3 = new FunctionalDependencies.Builder()
        .addFD(1, 0)
        .addFD(2, 1)
        .build();

    assertThat(fds1.equalTo(fds2), is(true));
    assertThat(fds2.equalTo(fds1), is(true));
    assertThat(fds1.equalTo(fds3), is(false));
    assertThat(fds3.equalTo(fds1), is(false));
  }

  @Test void testFDUnion() {
    FunctionalDependencies fds1 = new FunctionalDependencies.Builder()
        .addFD(0, 1)
        .addFD(1, 2)
        .build();

    FunctionalDependencies fds2 = new FunctionalDependencies.Builder()
        .addFD(2, 3)
        .addFD(3, 4)
        .build();

    FunctionalDependencies union = fds1.union(fds2);

    assertThat(union.implies(ImmutableSet.of(0), ImmutableSet.of(1)), is(true));
    assertThat(union.implies(ImmutableSet.of(1), ImmutableSet.of(2)), is(true));
    assertThat(union.implies(ImmutableSet.of(2), ImmutableSet.of(3)), is(true));
    assertThat(union.implies(ImmutableSet.of(3), ImmutableSet.of(4)), is(true));
    assertThat(union.implies(ImmutableSet.of(0), ImmutableSet.of(3)), is(true));
    assertThat(union.implies(ImmutableSet.of(0), ImmutableSet.of(4)), is(true));
  }

  @Test public void testFindCandidateKeysNoFD() {
    // FD: empty
    FunctionalDependencies fds = new FunctionalDependencies.Builder().build();
    Set<Integer> attrs = ImmutableSet.of(0, 1);
    Set<Set<Integer>> keys = fds.findCandidateKeys(attrs, true);
    assertThat(ImmutableSet.of(attrs).equals(keys), is(true));
  }

  @Test public void testFindCandidateKeysLargeStarKey() {
    // FD: 0 -> i (i = 1 .. n - 1)
    int n = 1000;
    Set<Integer> attributes = IntStream.range(0, n).boxed().collect(Collectors.toSet());
    FunctionalDependencies.Builder builder = new FunctionalDependencies.Builder();
    for (int i = 1; i < n; i++) {
      builder.addFD(0, i);
    }
    builder.addFD(88, 0);
    FunctionalDependencies fds = builder.build();
    Set<Set<Integer>> keys = fds.findCandidateKeys(attributes, true);
    assertThat(ImmutableSet.of(ImmutableSet.of(0), ImmutableSet.of(88)).equals(keys), is(true));
  }

  @Test public void testFindCandidateKeysLargeAttributeSet() {
    // FD: 0 -> 1, 1 -> 2, ..., n - 2 -> n - 1
    int n = 10000;
    Set<Integer> attributes = IntStream.range(0, n).boxed().collect(Collectors.toSet());
    FunctionalDependencies.Builder builder = new FunctionalDependencies.Builder();
    for (int i = 0; i < n; i++) {
      builder.addFD(i, i + 1);
    }
    FunctionalDependencies fds = builder.build();
    Set<Set<Integer>> keys = fds.findCandidateKeys(attributes, true);
    assertThat(keys, hasSize(1));
    assertThat(keys.iterator().next(), equalTo(ImmutableSet.of(0)));
  }

  @Test void testComputeClosureWithLargeRelation() {
    int numAttrs = 10000;
    Set<FunctionalDependence> fdSet = new HashSet<>();
    for (int i = 0; i < numAttrs; i++) {
      fdSet.add(FunctionalDependence.of(i, i + 1));
    }
    FunctionalDependencies dfs = new FunctionalDependencies(fdSet);
    Set<Integer> closure = dfs.computeClosure(ImmutableSet.of(0));
    for (int i = 0; i <= numAttrs; i++) {
      assertThat(closure.contains(i), is(true));
    }
  }
}
