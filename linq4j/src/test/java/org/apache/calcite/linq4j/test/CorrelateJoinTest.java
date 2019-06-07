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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests {@link ExtendedEnumerable#correlateJoin(JoinType, Function1, Function2)}
 */
public class CorrelateJoinTest {
  static final Function2<Integer, Integer, Integer[]> SELECT_BOTH =
      (v0, v1) -> new Integer[]{v0, v1};

  @Test public void testInner() {
    testJoin(JoinType.INNER, new Integer[][]{
        {2, 20},
        {3, -30},
        {3, -60},
        {20, 200},
        {30, -300},
        {30, -600}});
  }

  @Test public void testLeft() {
    testJoin(JoinType.LEFT, new Integer[][]{
        {1, null},
        {2, 20},
        {3, -30},
        {3, -60},
        {10, null},
        {20, 200},
        {30, -300},
        {30, -600}});
  }

  @Test public void testSemi() {
    testJoin(JoinType.SEMI, new Integer[][]{
        {2, null},
        {3, null},
        {20, null},
        {30, null}});
  }

  @Test public void testAnti() {
    testJoin(JoinType.ANTI, new Integer[][]{
        {1, null},
        {10, null}});
  }

  public void testJoin(JoinType joinType, Integer[][] expected) {
    Enumerable<Integer[]> join =
        Linq4j.asEnumerable(ImmutableList.of(1, 2, 3, 10, 20, 30))
            .correlateJoin(joinType, a0 -> {
              if (a0 == 1 || a0 == 10) {
                return Linq4j.emptyEnumerable();
              }
              if (a0 == 2 || a0 == 20) {
                return Linq4j.singletonEnumerable(a0 * 10);
              }
              if (a0 == 3 || a0 == 30) {
                return Linq4j.asEnumerable(
                    ImmutableList.of(-a0 * 10, -a0 * 20));
              }
              throw new IllegalArgumentException(
                  "Unexpected input " + a0);
            }, SELECT_BOTH);
    for (int i = 0; i < 2; i++) {
      Enumerator<Integer[]> e = join.enumerator();
      checkResults(e, expected);
      e.close();
    }
  }

  private void checkResults(Enumerator<Integer[]> e, Integer[][] expected) {
    List<Integer[]> res = new ArrayList<>();
    while (e.moveNext()) {
      res.add(e.current());
    }
    Integer[][] actual = res.toArray(new Integer[res.size()][]);
    assertArrayEquals(expected, actual);
  }
}

// End CorrelateJoinTest.java
