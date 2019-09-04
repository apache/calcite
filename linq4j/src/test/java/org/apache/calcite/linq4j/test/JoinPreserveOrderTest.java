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
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

import static org.apache.calcite.linq4j.function.Functions.nullsComparator;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test validating the order preserving properties of join algorithms in
 * {@link org.apache.calcite.linq4j.ExtendedEnumerable}. The correctness of the
 * join algorithm is not examined by this set of tests.
 *
 * <p>To verify that the order of left/right/both input(s) is preserved they
 * must be all ordered by at least one column. The inputs are either sorted on
 * the join or some other column. For the tests to be meaningful the result of
 * the join must not be empty.
 *
 * <p>Interesting variants that may affect the join output and thus destroy the
 * order of one or both inputs is when the join column or the sorted column
 * (when join column != sort column) contain nulls or duplicate values.
 *
 * <p>In addition, the way that nulls are sorted before the join can also play
 * an important role regarding the order preserving semantics of the join.
 *
 * <p>Last but not least, the type of the join (left/right/full/inner/semi/anti)
 * has a major impact on the preservation of order for the various joins.
 */
@RunWith(Parameterized.class)
public final class JoinPreserveOrderTest {

  /**
   * A description holding which column must be sorted and how.
   * @param <T> the type of the input relation
   */
  private static class FieldCollationDescription<T> {
    private final String colName;
    private final Function1<T, Comparable> colSelector;
    private final boolean isAscending;
    private final boolean isNullsFirst;

    FieldCollationDescription(final String colName,
        final Function1<T, Comparable> colSelector,
        final boolean isAscending,
        final boolean isNullsFirst) {
      this.colName = colName;
      this.colSelector = colSelector;
      this.isAscending = isAscending;
      this.isNullsFirst = isNullsFirst;
    }

    @Override public String toString() {
      return "on='" + colName + "', asc=" + isAscending + ", nullsFirst=" + isNullsFirst + '}';
    }
  }

  /**
   * An abstraction for a join algorithm which performs an operation on two inputs and produces a
   * result.
   *
   * @param <L> the type of the left input
   * @param <R> the type of the right input
   * @param <Result> the type of the result
   */
  private interface JoinAlgorithm<L, R, Result> {
    Enumerable<Result> join(Enumerable<L> left, Enumerable<R> right);
  }

  private final FieldCollationDescription<Employee> leftColumn;
  private final FieldCollationDescription<Department> rightColumn;
  private static final Function2<Employee, Department, List<Integer>> RESULT_SELECTOR =
      (emp, dept) -> Arrays.asList(
          (emp != null) ? emp.eid : null,
          (dept != null) ? dept.did : null);

  public JoinPreserveOrderTest(
      final FieldCollationDescription<Employee> leftColumn,
      final FieldCollationDescription<Department> rightColumn) {
    this.leftColumn = leftColumn;
    this.rightColumn = rightColumn;
  }

  @Parameterized.Parameters(name = "{index}: columnLeft({0}), columnRight({1})")
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList<>();
    List<String> empOrderColNames = Arrays.asList("name", "deptno", "eid");
    List<Function1<Employee, Comparable>> empOrderColSelectors = Arrays.asList(
        Employee::getName,
        Employee::getDeptno,
        Employee::getEid);
    List<String> deptOrderColNames = Arrays.asList("name", "deptno", "did");
    List<Function1<Department, Comparable>> deptOrderColSelectors = Arrays.asList(
        Department::getName,
        Department::getDeptno,
        Department::getDid);
    List<Boolean> trueFalse = Arrays.asList(true, false);
    for (int i = 0; i < empOrderColNames.size(); i++) {
      for (Boolean ascendingL : trueFalse) {
        for (Boolean nullsFirstL : trueFalse) {
          for (int j = 0; j < deptOrderColNames.size(); j++) {
            for (Boolean nullsFirstR : trueFalse) {
              for (Boolean ascendingR : trueFalse) {
                Object[] params = new Object[2];
                params[0] = new FieldCollationDescription<>(
                    empOrderColNames.get(i),
                    empOrderColSelectors.get(i),
                    ascendingL,
                    nullsFirstL);
                params[1] = new FieldCollationDescription<>(
                    deptOrderColNames.get(j),
                    deptOrderColSelectors.get(j),
                    ascendingR,
                    nullsFirstR);
                data.add(params);
              }
            }
          }
        }
      }
    }
    return data;
  }

  @Test public void testLeftJoinPreservesOrderOfLeftInput() {
    testJoin(hashJoin(false, true), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testRightJoinPreservesOrderOfLeftInput() {
    Assume.assumeFalse(leftColumn.isNullsFirst);
    testJoin(hashJoin(true, false), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testFullJoinPreservesOrderOfLeftInput() {
    Assume.assumeFalse(leftColumn.isNullsFirst);
    testJoin(hashJoin(true, true), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testInnerJoinPreservesOrderOfLeftInput() {
    testJoin(hashJoin(false, false), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testLeftNestedLoopJoinPreservesOrderOfLeftInput() {
    testJoin(nestedLoopJoin(JoinType.LEFT), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testRightNestedLoopJoinPreservesOrderOfLeftInput() {
    Assume.assumeFalse(leftColumn.isNullsFirst);
    testJoin(nestedLoopJoin(JoinType.RIGHT), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testFullNestedLoopJoinPreservesOrderOfLeftInput() {
    Assume.assumeFalse(leftColumn.isNullsFirst);
    testJoin(nestedLoopJoin(JoinType.FULL), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testInnerNestedLoopJoinPreservesOrderOfLeftInput() {
    testJoin(nestedLoopJoin(JoinType.INNER), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }


  @Test public void testLeftCorrelateJoinPreservesOrderOfLeftInput() {
    testJoin(correlateJoin(JoinType.LEFT), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testInnerCorrelateJoinPreservesOrderOfLeftInput() {
    testJoin(correlateJoin(JoinType.INNER), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testAntiCorrelateJoinPreservesOrderOfLeftInput() {
    testJoin(correlateJoin(JoinType.ANTI), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testSemiCorrelateJoinPreservesOrderOfLeftInput() {
    testJoin(correlateJoin(JoinType.SEMI), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testSemiDefaultJoinPreservesOrderOfLeftInput() {
    testJoin(semiJoin(), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  @Test public void testCorrelateBatchJoin() {
    testJoin(
        correlateBatchJoin(JoinType.INNER),
        AssertOrder.PRESERVED,
        AssertOrder.IGNORED);
  }

  @Test public void testAntiDefaultJoinPreservesOrderOfLeftInput() {
    testJoin(antiJoin(), AssertOrder.PRESERVED, AssertOrder.IGNORED);
  }

  private void testJoin(
      JoinAlgorithm<Employee, Department, List<Integer>> joinAlgorithm,
      AssertOrder assertLeftInput, AssertOrder assertRightInput) {
    Enumerable<Employee> left =
        Linq4j.asEnumerable(EMPS)
            .orderBy(leftColumn.colSelector,
                nullsComparator(leftColumn.isNullsFirst, !leftColumn.isAscending));
    Enumerable<Department> right =
        Linq4j.asEnumerable(DEPTS)
            .orderBy(rightColumn.colSelector,
                nullsComparator(rightColumn.isNullsFirst, !rightColumn.isAscending));
    Enumerable<List<Integer>> joinResult = joinAlgorithm.join(left, right);

    List<Integer> actualIdOrderLeft = joinResult.select(joinTuple -> joinTuple.get(0)).toList();
    List<Integer> expectedIdOrderLeft = left.select(e -> e.eid).toList();
    assertLeftInput.check(expectedIdOrderLeft, actualIdOrderLeft, leftColumn.isNullsFirst);
    List<Integer> actualIdOrderRight = joinResult.select(joinTuple -> joinTuple.get(1)).toList();
    List<Integer> expectedIdOrderRight = right.select(d -> d.did).toList();
    assertRightInput.check(expectedIdOrderRight, actualIdOrderRight, rightColumn.isNullsFirst);
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> correlateJoin(
      JoinType joinType) {
    return (left, right) ->
        left.correlateJoin(
            joinType,
            emp -> right.where(dept ->
                emp.deptno != null
                    && dept.deptno != null
                    && emp.deptno.equals(dept.deptno)),
            RESULT_SELECTOR);
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> hashJoin(
      boolean generateNullsOnLeft,
      boolean generateNullsOnRight) {
    return (left, right) ->
        left.hashJoin(right,
            e -> e.deptno,
            d -> d.deptno,
            RESULT_SELECTOR,
            null,
            generateNullsOnLeft,
            generateNullsOnRight);
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> nestedLoopJoin(JoinType joinType) {
    return (left, right) ->
        EnumerableDefaults.nestedLoopJoin(
            left,
            right,
            (emp, dept) ->
                emp.deptno != null && dept.deptno != null && emp.deptno.equals(dept.deptno),
            RESULT_SELECTOR,
            joinType);
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> semiJoin() {
    return (left, right) ->
        EnumerableDefaults.semiJoin(
            left,
            right,
            emp -> emp.deptno,
            dept -> dept.deptno).select(emp -> Arrays.asList(emp.eid, null));
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> antiJoin() {
    return (left, right) ->
        EnumerableDefaults.antiJoin(
            left,
            right,
            emp -> emp.deptno,
            dept -> dept.deptno).select(emp -> Arrays.asList(emp.eid, null));
  }

  private JoinAlgorithm<Employee, Department, List<Integer>> correlateBatchJoin(
      JoinType joinType) {
    return (left, right) ->
        EnumerableDefaults.correlateBatchJoin(
            joinType,
            left,
            emp -> right.where(dept ->
                    dept.deptno != null
                        && (dept.deptno.equals(emp.get(0).deptno)
                        || dept.deptno.equals(emp.get(1).deptno)
                        || dept.deptno.equals(emp.get(2).deptno))),
            RESULT_SELECTOR,
            (emp, dept) -> dept.deptno.equals(emp.deptno),
             3);
  }

  /**
   * Different assertions for the result of the join.
   */
  private enum AssertOrder {
    PRESERVED {
      @Override <E> void check(final List<E> expected, final List<E> actual,
          final boolean nullsFirst) {
        assertTrue("Order is not preserved. Expected:<" + expected + "> but was:<" + actual + ">",
            isOrderPreserved(expected, actual, nullsFirst));
      }
    },
    DESTROYED {
      @Override <E> void check(final List<E> expected, final List<E> actual,
          final boolean nullsFirst) {
        assertFalse("Order is not destroyed. Expected:<" + expected + "> but was:<" + actual + ">",
            isOrderPreserved(expected, actual, nullsFirst));
      }
    },
    IGNORED {
      @Override <E> void check(final List<E> expected, final List<E> actual,
          final boolean nullsFirst) {
        // Do nothing
      }
    };

    abstract <E> void check(List<E> expected, List<E> actual, boolean nullsFirst);

    /**
     * Checks that the elements in the list are in the expected order.
     */
    <E> boolean isOrderPreserved(List<E> expected, List<E> actual, boolean nullsFirst) {
      boolean isPreserved = true;
      for (int i = 1; i < actual.size(); i++) {
        E prev = actual.get(i - 1);
        E next = actual.get(i);
        int posPrev = prev == null ? (nullsFirst ? -1 : actual.size()) : expected.indexOf(prev);
        int posNext = next == null ? (nullsFirst ? -1 : actual.size()) : expected.indexOf(next);
        isPreserved &= posPrev <= posNext;
      }
      return isPreserved;
    }
  }

  /** Department */
  private static class Department {
    private final int did;
    private final Integer deptno;
    private final String name;

    Department(final int did, final Integer deptno, final String name) {
      this.did = did;
      this.deptno = deptno;
      this.name = name;
    }

    int getDid() {
      return did;
    }

    Integer getDeptno() {
      return deptno;
    }

    String getName() {
      return name;
    }
  }

  /** Employee */
  private static class Employee {
    private final int eid;
    private final String name;
    private final Integer deptno;

    Employee(final int eid, final String name, final Integer deptno) {
      this.eid = eid;
      this.name = name;
      this.deptno = deptno;
    }

    int getEid() {
      return eid;
    }

    String getName() {
      return name;
    }

    Integer getDeptno() {
      return deptno;
    }

    @Override public String toString() {
      return "Employee{eid=" + eid + ", name='" + name + '\'' + ", deptno=" + deptno + '}';
    }
  }

  private static final Employee[] EMPS = new Employee[]{
      new Employee(100, "Stam", 10),
      new Employee(110, "Greg", 20),
      new Employee(120, "Ilias", 30),
      new Employee(130, "Ruben", 40),
      new Employee(140, "Tanguy", 50),
      new Employee(145, "Khawla", 40),
      new Employee(150, "Andrew", -10),
      // Nulls on name
      new Employee(160, null, 60),
      new Employee(170, null, -60),
      // Nulls on deptno
      new Employee(180, "Achille", null),
      // Duplicate values on name
      new Employee(190, "Greg", 70),
      new Employee(200, "Ilias", -70),
      // Duplicates values on deptno
      new Employee(210, "Sophia", 40),
      new Employee(220, "Alexia", -40),
      new Employee(230, "Loukia", -40)
  };

  private static final Department[] DEPTS = new Department[]{
      new Department(1, 10, "Sales"),
      new Department(2, 20, "Pre-sales"),
      new Department(4, 40, "Support"),
      new Department(5, 50, "Marketing"),
      new Department(6, 60, "Engineering"),
      new Department(7, 70, "Management"),
      new Department(8, 80, "HR"),
      new Department(9, 90, "Product design"),
      // Nulls on name
      new Department(3, 30, null),
      new Department(10, 100, null),
      // Nulls on deptno
      new Department(11, null, "Post-sales"),
      // Duplicate values on name
      new Department(12, 50, "Support"),
      new Department(13, 140, "Support"),
      // Duplicate values on deptno
      new Department(14, 20, "Board"),
      new Department(15, 40, "Promotions"),
  };

}

// End JoinPreserveOrderTest.java
