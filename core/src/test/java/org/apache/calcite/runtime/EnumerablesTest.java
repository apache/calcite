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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate2;

import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static com.google.common.collect.Lists.newArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link org.apache.calcite.runtime.Enumerables}.
 */
public class EnumerablesTest {
  private static final Enumerable<Emp> EMPS = Linq4j.asEnumerable(
      Arrays.asList(
          new Emp(10, "Fred"),
          new Emp(20, "Theodore"),
          new Emp(20, "Sebastian"),
          new Emp(30, "Joe")));

  private static final Enumerable<Dept> DEPTS = Linq4j.asEnumerable(
      Arrays.asList(
          new Dept(20, "Sales"),
          new Dept(15, "Marketing")));

  private static final Function2<Emp, Dept, String> EMP_DEPT_TO_STRING =
      (v0, v1) -> "{" + (v0 == null ? null : v0.name)
          + ", " + (v0 == null ? null : v0.deptno)
          + ", " + (v1 == null ? null : v1.deptno)
          + ", " + (v1 == null ? null : v1.name)
          + "}";

  private static final Predicate2<Emp, Dept> EQUAL_DEPTNO =
      (e, d) -> e.deptno == d.deptno;

  @Test public void testSemiJoin() {
    assertThat(
        EnumerableDefaults.semiJoin(EMPS, DEPTS, e -> e.deptno, d -> d.deptno,
            Functions.identityComparer()).toList().toString(),
        equalTo("[Emp(20, Theodore), Emp(20, Sebastian)]"));
  }

  @Test public void testMergeJoin() {
    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"),
                    new Emp(30, "Greg"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Research"),
                    new Dept(30, "Development"))),
            e -> e.deptno,
            d -> d.deptno,
            (v0, v1) -> v0 + ", " + v1, false, false).toList().toString(),
        equalTo("[Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " Emp(30, Joe), Dept(30, Research),"
            + " Emp(30, Joe), Dept(30, Development),"
            + " Emp(30, Greg), Dept(30, Research),"
            + " Emp(30, Greg), Dept(30, Development)]"));
  }

  @Test public void testMergeJoin2() {
    // Matching keys at start
    assertThat(
        intersect(Lists.newArrayList(1, 3, 4),
            Lists.newArrayList(1, 4)).toList().toString(),
        equalTo("[1, 4]"));
    // Matching key at start and end of right, not of left
    assertThat(
        intersect(Lists.newArrayList(0, 1, 3, 4, 5),
            Lists.newArrayList(1, 4)).toList().toString(),
        equalTo("[1, 4]"));
    // Matching key at start and end of left, not right
    assertThat(
        intersect(Lists.newArrayList(1, 3, 4),
            Lists.newArrayList(0, 1, 4, 5)).toList().toString(),
        equalTo("[1, 4]"));
    // Matching key not at start or end of left or right
    assertThat(
        intersect(Lists.newArrayList(0, 2, 3, 4, 5),
            Lists.newArrayList(1, 3, 4, 6)).toList().toString(),
        equalTo("[3, 4]"));
  }

  @Test public void testMergeJoin3() {
    // No overlap
    assertThat(
        intersect(Lists.newArrayList(0, 2, 4),
            Lists.newArrayList(1, 3, 5)).toList().toString(),
        equalTo("[]"));
    // Left empty
    assertThat(
        intersect(new ArrayList<>(),
            newArrayList(1, 3, 4, 6)).toList().toString(),
        equalTo("[]"));
    // Right empty
    assertThat(
        intersect(newArrayList(3, 7),
            new ArrayList<>()).toList().toString(),
        equalTo("[]"));
    // Both empty
    assertThat(
        intersect(new ArrayList<Integer>(),
            new ArrayList<>()).toList().toString(),
        equalTo("[]"));
  }

  private static <T extends Comparable<T>> Enumerable<T> intersect(
      List<T> list0, List<T> list1) {
    return EnumerableDefaults.mergeJoin(
        Linq4j.asEnumerable(list0),
        Linq4j.asEnumerable(list1),
        Functions.identitySelector(),
        Functions.identitySelector(), (v0, v1) -> v0, false, false);
  }

  @Test public void testThetaJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.INNER).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}]"));
  }

  @Test public void testThetaLeftJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.LEFT).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaRightJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.RIGHT).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoinLeftEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS.take(0), DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL)
            .orderBy(Functions.identitySelector()).toList().toString(),
        equalTo("[{null, null, 15, Marketing}, {null, null, 20, Sales}]"));
  }

  @Test public void testThetaFullJoinRightEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, null, null}, "
            + "{Sebastian, 20, null, null}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaFullJoinBothEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS.take(0), DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[]"));
  }

  @Test
  @Ignore // TODO fix this
  public void testMatch() {
    final Enumerable<Emp> emps = Linq4j.asEnumerable(
        Arrays.asList(
            new Emp(20, "Theodore"),
            new Emp(10, "Fred"),
            new Emp(20, "Sebastian"),
            new Emp(30, "Joe")));

    final Pattern p =
        Pattern.builder()
            .symbol("A")
            .symbol("B").seq()
            .build();

    final Matcher<Emp> matcher =
        Matcher.<Emp>builder(p.toAutomaton())
            .add("A", s -> s.get().deptno == 20)
            .add("B", s -> s.get().deptno != 20)
            .build();

    final Enumerables.Emitter<Emp, String> emitter =
        (rows, rowStates, rowSymbols, match, consumer) -> {
          for (int i = 0; i < rows.size(); i++) {
            if (rowSymbols == null) {
              continue;
            }
            if ("A".equals(rowSymbols.get(i))) {
              consumer.accept(
                  String.format(Locale.ENGLISH, "%s %s %d", rows, rowStates,
                      match));
            }
          }
        };

    final Enumerable<String> matches =
        Enumerables.match(emps, emp -> 0L, matcher, emitter, 1, 1);
    assertThat(matches.toList().toString(),
        equalTo("[[Emp(20, Theodore), Emp(10, Fred)] null 1, "
            + "[Emp(20, Sebastian), Emp(30, Joe)] null 2]"));
  }

  @Test public void testInnerHashJoin() {
    assertThat(
        EnumerableDefaults.hashJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"),
                    new Emp(30, "Greg"))),
            Linq4j.asEnumerable(
                Arrays.asList(new Dept(15, "Marketing"), new Dept(20, "Sales"),
                    new Dept(30, "Research"), new Dept(30, "Development"))),
            e -> e.deptno,
            d -> d.deptno,
            (v0, v1) -> v0 + ", " + v1, null)
            .toList()
            .toString(),
        equalTo("[Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " Emp(30, Joe), Dept(30, Research),"
            + " Emp(30, Joe), Dept(30, Development),"
            + " Emp(30, Greg), Dept(30, Research),"
            + " Emp(30, Greg), Dept(30, Development)]"));
  }

  @Test public void testLeftHashJoinWithNonEquiConditions() {
    assertThat(
        EnumerableDefaults.hashJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"),
                    new Emp(30, "Greg"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Research"),
                    new Dept(30, "Development"))),
            e -> e.deptno,
            d -> d.deptno,
            (v0, v1) -> v0 + ", " + v1, null, false, true,
            (v0, v1) -> v0.deptno < 30)
            .toList()
            .toString(),
        equalTo("[Emp(10, Fred), null,"
            + " Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " Emp(30, Joe), null,"
            + " Emp(30, Greg), null]"));
  }

  @Test public void testRightHashJoinWithNonEquiConditions() {
    assertThat(
        EnumerableDefaults.hashJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Greg"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Research"),
                    new Dept(30, "Development"))),
            e -> e.deptno,
            d -> d.deptno,
            (v0, v1) -> v0 + ", " + v1, null, true, false,
            (v0, v1) -> v0.deptno < 30)
            .toList()
            .toString(),
        equalTo("[Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " null, Dept(15, Marketing),"
            + " null, Dept(30, Research),"
            + " null, Dept(30, Development)]"));
  }

  @Test public void testFullHashJoinWithNonEquiConditions() {
    assertThat(
        EnumerableDefaults.hashJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Greg"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Research"),
                    new Dept(30, "Development"))),
            e -> e.deptno,
            d -> d.deptno,
            (v0, v1) -> v0 + ", " + v1, null, true, true,
            (v0, v1) -> v0.deptno < 30)
            .toList()
            .toString(),
        equalTo("[Emp(10, Fred), null,"
            + " Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " Emp(30, Greg), null,"
            + " null, Dept(15, Marketing),"
            + " null, Dept(30, Research),"
            + " null, Dept(30, Development)]"));
  }

  /** Employee record. */
  private static class Emp {
    final int deptno;
    final String name;

    Emp(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Emp(" + deptno + ", " + name + ")";
    }
  }

  /** Department record. */
  private static class Dept {
    final int deptno;
    final String name;

    Dept(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Dept(" + deptno + ", " + name + ")";
    }
  }
}

// End EnumerablesTest.java
