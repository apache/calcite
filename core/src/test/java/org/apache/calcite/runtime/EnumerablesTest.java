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
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate2;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
      new Function2<Emp, Dept, String>() {
        public String apply(Emp v0, Dept v1) {
          return "{" + (v0 == null ? null : v0.name)
              + ", " + (v0 == null ? null : v0.deptno)
              + ", " + (v1 == null ? null : v1.deptno)
              + ", " + (v1 == null ? null : v1.name)
              + "}";
        }
      };

  private static final Predicate2<Emp, Dept> EQUAL_DEPTNO =
      new Predicate2<Emp, Dept>() {
        public boolean apply(Emp v0, Dept v1) {
          return v0.deptno == v1.deptno;
        }
      };

  @Test public void testSemiJoin() {
    assertThat(
        EnumerableDefaults.semiJoin(EMPS, DEPTS,
            new Function1<Emp, Integer>() {
              public Integer apply(Emp a0) {
                return a0.deptno;
              }
            },
            new Function1<Dept, Integer>() {
              public Integer apply(Dept a0) {
                return a0.deptno;
              }
            },
            Functions.<Integer>identityComparer()).toList().toString(),
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
            new Function1<Emp, Integer>() {
              public Integer apply(Emp a0) {
                return a0.deptno;
              }
            },
            new Function1<Dept, Integer>() {
              public Integer apply(Dept a0) {
                return a0.deptno;
              }
            },
            new Function2<Emp, Dept, String>() {
              public String apply(Emp v0, Dept v1) {
                return v0 + ", " + v1;
              }
            }, false, false).toList().toString(),
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
        intersect(Lists.<Integer>newArrayList(),
            Lists.newArrayList(1, 3, 4, 6)).toList().toString(),
        equalTo("[]"));
    // Right empty
    assertThat(
        intersect(Lists.newArrayList(3, 7),
            Lists.<Integer>newArrayList()).toList().toString(),
        equalTo("[]"));
    // Both empty
    assertThat(
        intersect(Lists.<Integer>newArrayList(),
            Lists.<Integer>newArrayList()).toList().toString(),
        equalTo("[]"));
  }

  private static <T extends Comparable<T>> Enumerable<T> intersect(
      List<T> list0, List<T> list1) {
    return EnumerableDefaults.mergeJoin(
        Linq4j.asEnumerable(list0),
        Linq4j.asEnumerable(list1),
        Functions.<T>identitySelector(),
        Functions.<T>identitySelector(),
        new Function2<T, T, T>() {
          public T apply(T v0, T v1) {
            return v0;
          }
        }, false, false);
  }

  @Test public void testThetaJoin() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, false, false).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}]"));
  }

  @Test public void testThetaLeftJoin() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, false, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaRightJoin() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, false).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoin() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoinLeftEmpty() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS.take(0), DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true)
            .orderBy(Functions.<String>identitySelector()).toList().toString(),
        equalTo("[{null, null, 15, Marketing}, {null, null, 20, Sales}]"));
  }

  @Test public void testThetaFullJoinRightEmpty() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS, DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, null, null}, "
            + "{Sebastian, 20, null, null}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaFullJoinBothEmpty() {
    assertThat(
        EnumerableDefaults.thetaJoin(EMPS.take(0), DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[]"));
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
