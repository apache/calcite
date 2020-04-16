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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static com.google.common.collect.Lists.newArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link org.apache.calcite.runtime.Enumerables}.
 */
class EnumerablesTest {
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

  private static final Predicate2<Emp, Dept> EMP_DEPT_EQUAL_DEPTNO =
      (e, d) -> e.deptno == d.deptno;
  private static final Predicate2<Dept, Emp> DEPT_EMP_EQUAL_DEPTNO =
      (d, e) -> d.deptno == e.deptno;

  @Test void testSemiJoinEmp() {
    assertThat(
        EnumerableDefaults.semiJoin(EMPS, DEPTS, e -> e.deptno, d -> d.deptno,
            Functions.identityComparer()).toList().toString(),
        equalTo("[Emp(20, Theodore), Emp(20, Sebastian)]"));
  }

  @Test void testSemiJoinDept() {
    assertThat(
        EnumerableDefaults.semiJoin(DEPTS, EMPS, d -> d.deptno, e -> e.deptno,
            Functions.identityComparer()).toList().toString(),
        equalTo("[Dept(20, Sales)]"));
  }

  @Test void testAntiJoinEmp() {
    assertThat(
        EnumerableDefaults.antiJoin(EMPS, DEPTS, e -> e.deptno, d -> d.deptno,
            Functions.identityComparer()).toList().toString(),
        equalTo("[Emp(10, Fred), Emp(30, Joe)]"));
  }

  @Test void testAntiJoinDept() {
    assertThat(
        EnumerableDefaults.antiJoin(DEPTS, EMPS, d -> d.deptno, e -> e.deptno,
            Functions.identityComparer()).toList().toString(),
        equalTo("[Dept(15, Marketing)]"));
  }

  @Test void testMergeJoin() {
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
            (v0, v1) -> v0 + ", " + v1, JoinType.INNER, null).toList().toString(),
        equalTo("[Emp(20, Theodore), Dept(20, Sales),"
            + " Emp(20, Sebastian), Dept(20, Sales),"
            + " Emp(30, Joe), Dept(30, Research),"
            + " Emp(30, Joe), Dept(30, Development),"
            + " Emp(30, Greg), Dept(30, Research),"
            + " Emp(30, Greg), Dept(30, Development)]"));
  }

  @Test void testMergeJoinWithNullKeys() {
    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(30, "Fred"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Theodore"),
                    new Emp(20, "Theodore"),
                    new Emp(40, null),
                    new Emp(30, null))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Theodore"),
                    new Dept(40, null))),
            e -> e.name,
            d -> d.name,
            (v0, v1) -> v0 + ", " + v1, JoinType.INNER, null).toList().toString(),
        equalTo("[Emp(30, Theodore), Dept(30, Theodore),"
            + " Emp(20, Theodore), Dept(30, Theodore)]"));
  }

  @Test void testMergeJoin2() {
    final JoinType[] joinTypes = {JoinType.INNER, JoinType.SEMI};
    for (JoinType joinType : joinTypes) {
      // Matching keys at start
      testIntersect(
          newArrayList(1, 3, 4),
          newArrayList(1, 4),
          equalTo("[1, 4]"),
          joinType);
      // Matching key at start and end of right, not of left
      testIntersect(
          newArrayList(0, 1, 3, 4, 5),
          newArrayList(1, 4),
          equalTo("[1, 4]"),
          joinType);
      // Matching key at start and end of left, not right
      testIntersect(
          newArrayList(1, 3, 4),
          newArrayList(0, 1, 4, 5),
          equalTo("[1, 4]"),
          joinType);
      // Matching key not at start or end of left or right
      testIntersect(
          newArrayList(0, 2, 3, 4, 5),
          newArrayList(1, 3, 4, 6),
          equalTo("[3, 4]"),
          joinType);
      // Matching duplicated keys
      testIntersect(
          newArrayList(1, 3, 4),
          newArrayList(1, 1, 4, 4),
          equalTo(joinType == JoinType.INNER ? "[1, 1, 4, 4]" : "[1, 4]"),
          joinType);
    }
  }

  @Test void testMergeJoin3() {
    final JoinType[] joinTypes = {JoinType.INNER, JoinType.SEMI};
    for (JoinType joinType : joinTypes) {
      // No overlap
      testIntersect(
          Lists.newArrayList(0, 2, 4),
          Lists.newArrayList(1, 3, 5),
          equalTo("[]"),
          joinType);
      // Left empty
      testIntersect(
          new ArrayList<>(),
          newArrayList(1, 3, 4, 6),
          equalTo("[]"),
          joinType);
      // Right empty
      testIntersect(
          newArrayList(3, 7),
          new ArrayList<>(),
          equalTo("[]"),
          joinType);
      // Both empty
      testIntersect(
          new ArrayList<Integer>(),
          new ArrayList<>(),
          equalTo("[]"),
          joinType);
    }
  }

  private static <T extends Comparable<T>> void testIntersect(
      List<T> list0, List<T> list1, org.hamcrest.Matcher<String> matcher, JoinType joinType) {
    assertThat(
        intersect(list0, list1, joinType).toList().toString(),
        matcher);

    // Repeat test with nulls at the end of left / right: result should not be impacted

    // Null at the end of left
    list0.add(null);
    assertThat(
        intersect(list0, list1, joinType).toList().toString(),
        matcher);

    // Null at the end of right
    list0.remove(list0.size() - 1);
    list1.add(null);
    assertThat(
        intersect(list0, list1, joinType).toList().toString(),
        matcher);

    // Null at the end of left and right
    list0.add(null);
    assertThat(
        intersect(list0, list1, joinType).toList().toString(),
        matcher);
  }

  private static <T extends Comparable<T>> Enumerable<T> intersect(
      List<T> list0, List<T> list1, JoinType joinType) {
    return EnumerableDefaults.mergeJoin(
        Linq4j.asEnumerable(list0),
        Linq4j.asEnumerable(list1),
        Functions.identitySelector(),
        Functions.identitySelector(),
        (v0, v1) -> v0,
        joinType,
        null);
  }

  @Test void testMergeJoinWithPredicate() {
    final List<Emp> listEmp1 = Arrays.asList(
        new Emp(1, "Fred"),
        new Emp(2, "Fred"),
        new Emp(3, "Joe"),
        new Emp(4, "Joe"),
        new Emp(5, "Peter"));
    final List<Emp> listEmp2 = Arrays.asList(
        new Emp(2, "Fred"),
        new Emp(3, "Fred"),
        new Emp(3, "Joe"),
        new Emp(5, "Joe"),
        new Emp(6, "Peter"));

    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(listEmp1),
            Linq4j.asEnumerable(listEmp2),
            e1 -> e1.name,
            e2 -> e2.name,
            (e1, e2) -> e1.deptno < e2.deptno,
            (v0, v1) -> v0 + "-" + v1, JoinType.INNER, null).toList().toString(),
        equalTo("["
            + "Emp(1, Fred)-Emp(2, Fred), "
            + "Emp(1, Fred)-Emp(3, Fred), "
            + "Emp(2, Fred)-Emp(3, Fred), "
            + "Emp(3, Joe)-Emp(5, Joe), "
            + "Emp(4, Joe)-Emp(5, Joe), "
            + "Emp(5, Peter)-Emp(6, Peter)]"));

    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(listEmp2),
            Linq4j.asEnumerable(listEmp1),
            e2 -> e2.name,
            e1 -> e1.name,
            (e2, e1) -> e2.deptno > e1.deptno,
            (v0, v1) -> v0 + "-" + v1, JoinType.INNER, null).toList().toString(),
        equalTo("["
            + "Emp(2, Fred)-Emp(1, Fred), "
            + "Emp(3, Fred)-Emp(1, Fred), "
            + "Emp(3, Fred)-Emp(2, Fred), "
            + "Emp(5, Joe)-Emp(3, Joe), "
            + "Emp(5, Joe)-Emp(4, Joe), "
            + "Emp(6, Peter)-Emp(5, Peter)]"));

    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(listEmp1),
            Linq4j.asEnumerable(listEmp2),
            e1 -> e1.name,
            e2 -> e2.name,
            (e1, e2) -> e1.deptno == e2.deptno * 2,
            (v0, v1) -> v0 + "-" + v1, JoinType.INNER, null).toList().toString(),
        equalTo("[]"));

    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(listEmp2),
            Linq4j.asEnumerable(listEmp1),
            e2 -> e2.name,
            e1 -> e1.name,
            (e2, e1) -> e2.deptno == e1.deptno * 2,
            (v0, v1) -> v0 + "-" + v1, JoinType.INNER, null).toList().toString(),
        equalTo("[Emp(2, Fred)-Emp(1, Fred)]"));

    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(listEmp2),
            Linq4j.asEnumerable(listEmp1),
            e2 -> e2.name,
            e1 -> e1.name,
            (e2, e1) -> e2.deptno == e1.deptno + 2,
            (v0, v1) -> v0 + "-" + v1, JoinType.INNER, null).toList().toString(),
        equalTo("[Emp(3, Fred)-Emp(1, Fred), Emp(5, Joe)-Emp(3, Joe)]"));
  }

  @Test void testMergeSemiJoin() {
    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(10, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(25, "HR"),
                    new Dept(30, "Research"),
                    new Dept(40, "Development"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"),
                    new Emp(30, "Greg"),
                    new Emp(50, "Mary"))),
            d -> d.deptno,
            e -> e.deptno,
            null,
            (v0, v1) -> v0,
            JoinType.SEMI,
            null).toList().toString(), equalTo("[Dept(10, Marketing),"
            + " Dept(20, Sales)," + " Dept(30, Research)]"));
  }

  @Test void testMergeSemiJoinWithPredicate() {
    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(10, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(25, "HR"),
                    new Dept(30, "Research"),
                    new Dept(40, "Development"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"),
                    new Emp(30, "Greg"),
                    new Emp(50, "Mary"))),
            d -> d.deptno,
            e -> e.deptno,
            (d, e) -> e.name.contains("a"),
            (v0, v1) -> v0,
            JoinType.SEMI,
            null).toList().toString(), equalTo("[Dept(20, Sales)]"));
  }

  @Test void testMergeSemiJoinWithNullKeys() {
    assertThat(
        EnumerableDefaults.mergeJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(30, "Fred"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Theodore"),
                    new Emp(20, "Zoey"),
                    new Emp(40, null),
                    new Emp(30, null))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(15, "Marketing"),
                    new Dept(20, "Sales"),
                    new Dept(30, "Theodore"),
                    new Dept(25, "Theodore"),
                    new Dept(33, "Zoey"),
                    new Dept(40, null))),
            e -> e.name,
            d -> d.name,
            (e, d) -> e.name.startsWith("T"),
            (v0, v1) -> v0,
            JoinType.SEMI,
            null).toList().toString(), equalTo("[Emp(30, Theodore)]"));
  }

  @Test void testNestedLoopJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.INNER).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}]"));
  }

  @Test void testNestedLoopLeftJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.LEFT).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}]"));
  }

  @Test void testNestedLoopRightJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.RIGHT).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test void testNestedLoopFullJoin() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test void testNestedLoopFullJoinLeftEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS.take(0), DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL)
            .orderBy(Functions.identitySelector()).toList().toString(),
        equalTo("[{null, null, 15, Marketing}, {null, null, 20, Sales}]"));
  }

  @Test void testNestedLoopFullJoinRightEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS.take(0), EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, null, null}, "
            + "{Sebastian, 20, null, null}, {Joe, 30, null, null}]"));
  }

  @Test void testNestedLoopFullJoinBothEmpty() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS.take(0), DEPTS.take(0), EMP_DEPT_EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, JoinType.FULL).toList().toString(),
        equalTo("[]"));
  }

  @Test void testNestedLoopSemiJoinEmp() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            (e, d) -> e.toString(), JoinType.SEMI).toList().toString(),
        equalTo("[Emp(20, Theodore), Emp(20, Sebastian)]"));
  }

  @Test void testNestedLoopSemiJoinDept() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(DEPTS, EMPS, DEPT_EMP_EQUAL_DEPTNO,
            (d, e) -> d.toString(), JoinType.SEMI).toList().toString(),
        equalTo("[Dept(20, Sales)]"));
  }

  @Test void testNestedLoopAntiJoinEmp() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(EMPS, DEPTS, EMP_DEPT_EQUAL_DEPTNO,
            (e, d) -> e.toString(), JoinType.ANTI).toList().toString(),
        equalTo("[Emp(10, Fred), Emp(30, Joe)]"));
  }

  @Test void testNestedLoopAntiJoinDept() {
    assertThat(
        EnumerableDefaults.nestedLoopJoin(DEPTS, EMPS, DEPT_EMP_EQUAL_DEPTNO,
            (d, e) -> d.toString(), JoinType.ANTI).toList().toString(),
        equalTo("[Dept(15, Marketing)]"));
  }

  @Test @Disabled // TODO fix this
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

  @Test void testInnerHashJoin() {
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

  @Test void testLeftHashJoinWithNonEquiConditions() {
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

  @Test void testRightHashJoinWithNonEquiConditions() {
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

  @Test void testFullHashJoinWithNonEquiConditions() {
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
