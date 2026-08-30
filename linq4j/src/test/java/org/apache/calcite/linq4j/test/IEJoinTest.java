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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.InequalityOperator;
import org.apache.calcite.linq4j.Linq4j;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for IEJoin. */
class IEJoinTest {
  private static final List<Point> LEFT =
      Arrays.asList(new Point("l0", 2, 8),
      new Point("l1", 5, 4),
      new Point("l2", 5, 4),
      new Point("lnx", null, 3),
      new Point("lny", 3, null));

  private static final List<Point> RIGHT =
      Arrays.asList(new Point("r0", 4, 3),
      new Point("r1", 7, 6),
      new Point("r2", 5, 4),
      new Point("rnx", null, 2),
      new Point("rny", 6, null));

  @Test void testAllOperatorCombinationsAgainstNestedLoop() {
    for (InequalityOperator operator1 : InequalityOperator.values()) {
      for (InequalityOperator operator2 : InequalityOperator.values()) {
        assertMatches("fixed input", LEFT, RIGHT, operator1, operator2);
      }
    }
  }

  @Test void testRandomInputsAgainstNestedLoop() {
    final Random random = new Random(0);
    for (int trial = 0; trial < 200; trial++) {
      final List<Point> left = new ArrayList<>();
      final List<Point> right = new ArrayList<>();
      final int leftCount = random.nextInt(9);
      final int rightCount = random.nextInt(9);
      for (int i = 0; i < leftCount; i++) {
        left.add(
            new Point("l" + i,
                random.nextInt(5) == 0 ? null : random.nextInt(7) - 3,
                random.nextInt(5) == 0 ? null : random.nextInt(7) - 3));
      }
      for (int i = 0; i < rightCount; i++) {
        right.add(
            new Point("r" + i,
                random.nextInt(5) == 0 ? null : random.nextInt(7) - 3,
                random.nextInt(5) == 0 ? null : random.nextInt(7) - 3));
      }
      for (InequalityOperator operator1 : InequalityOperator.values()) {
        for (InequalityOperator operator2 : InequalityOperator.values()) {
          assertMatches("trial " + trial, left, right, operator1, operator2);
        }
      }
    }
  }

  @Test void testEmptyAndSameInputs() {
    assertMatches("empty left", Collections.emptyList(), RIGHT,
        InequalityOperator.LESS_THAN, InequalityOperator.GREATER_THAN);
    assertMatches("empty right", LEFT, Collections.emptyList(),
        InequalityOperator.LESS_THAN, InequalityOperator.GREATER_THAN);
    assertMatches("same input", LEFT, LEFT,
        InequalityOperator.LESS_THAN_OR_EQUAL,
        InequalityOperator.GREATER_THAN_OR_EQUAL);
  }

  @Test void testReset() {
    final Enumerable<String> join =
        ieJoin(LEFT, RIGHT, InequalityOperator.LESS_THAN,
            InequalityOperator.GREATER_THAN);
    final List<String> first = new ArrayList<>();
    final List<String> second = new ArrayList<>();
    try (Enumerator<String> enumerator = join.enumerator()) {
      assertThrows(NoSuchElementException.class, enumerator::current);
      while (enumerator.moveNext()) {
        first.add(enumerator.current());
      }
      assertThrows(NoSuchElementException.class, enumerator::current);
      enumerator.reset();
      assertThrows(NoSuchElementException.class, enumerator::current);
      while (enumerator.moveNext()) {
        second.add(enumerator.current());
      }
    }
    assertThat(second, is(first));
    assertThat(join.toList(), is(first));
  }

  private static void assertMatches(String context, List<Point> left,
      List<Point> right, InequalityOperator operator1,
      InequalityOperator operator2) {
    final List<String> expected = nestedLoop(left, right, operator1, operator2);
    final List<String> actual = ieJoin(left, right, operator1, operator2).toList();
    Collections.sort(actual);
    assertThat(context + ", " + operator1 + "/" + operator2
            + ", left=" + left + ", right=" + right,
        actual, is(expected));
  }

  private static Enumerable<String> ieJoin(List<Point> left, List<Point> right,
      InequalityOperator operator1, InequalityOperator operator2) {
    return EnumerableDefaults.ieJoin(
        Linq4j.asEnumerable(left), Linq4j.asEnumerable(right),
        point -> point.x, point -> point.x,
        point -> point.y, point -> point.y,
        Comparator.naturalOrder(), Comparator.naturalOrder(),
        operator1, operator2,
        (leftPoint, rightPoint) -> leftPoint.name + ":" + rightPoint.name);
  }

  private static List<String> nestedLoop(List<Point> leftRows,
      List<Point> rightRows, InequalityOperator operator1,
      InequalityOperator operator2) {
    final List<String> result = new ArrayList<>();
    for (Point left : leftRows) {
      for (Point right : rightRows) {
        if (test(left.x, right.x, operator1)
            && test(left.y, right.y, operator2)) {
          result.add(left.name + ":" + right.name);
        }
      }
    }
    Collections.sort(result);
    return result;
  }

  private static boolean test(@Nullable Integer left, @Nullable Integer right,
      InequalityOperator operator) {
    if (left == null || right == null) {
      return false;
    }
    switch (operator) {
    case LESS_THAN:
      return left < right;
    case LESS_THAN_OR_EQUAL:
      return left <= right;
    case GREATER_THAN:
      return left > right;
    case GREATER_THAN_OR_EQUAL:
      return left >= right;
    default:
      throw new AssertionError(operator);
    }
  }

  /** Test row. */
  private static final class Point {
    final String name;
    final @Nullable Integer x;
    final @Nullable Integer y;

    private Point(String name, @Nullable Integer x, @Nullable Integer y) {
      this.name = name;
      this.x = x;
      this.y = y;
    }

    @Override public String toString() {
      return name + "(" + x + ", " + y + ")";
    }
  }
}
