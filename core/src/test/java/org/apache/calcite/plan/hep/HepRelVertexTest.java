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
package org.apache.calcite.plan.hep;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link HepRelVertex}.
 */
class HepRelVertexTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7957">[CALCITE-7957]
   * The plan that HepPlanner returns depends on whether the JVM runs with
   * assertions enabled</a>.
   *
   * <p>A rule can replace the node of a vertex with an equivalent node whose
   * fields have other names. The row type of the vertex must not depend on
   * the time of the first call of {@code getRowType()}: before or after the
   * replacement. */
  @Test void testRowTypeDoesNotDependOnTimeOfFirstCall() {
    final RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode deptnoAsA = b.scan("DEPT")
        .project(ImmutableList.of(b.field("DEPTNO")), ImmutableList.of("A"))
        .build();
    final RelNode deptnoAsB = b.scan("DEPT")
        .project(ImmutableList.of(b.field("DEPTNO")), ImmutableList.of("B"))
        .build();

    // The first call is before the replacement
    final HepRelVertex early = new HepRelVertex(deptnoAsA);
    assertThat(early.getRowType().getFieldNames(), is(ImmutableList.of("A")));
    early.replaceRel(deptnoAsB);
    assertThat(early.getRowType().getFieldNames(), is(ImmutableList.of("A")));

    // The first call is after the replacement
    final HepRelVertex late = new HepRelVertex(deptnoAsA);
    late.replaceRel(deptnoAsB);
    assertThat(late.getRowType().getFieldNames(), is(ImmutableList.of("A")));
  }
}
