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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;

import org.junit.jupiter.api.Test;

/** Test for {@link org.apache.calcite.adapter.enumerable.EnumerableUncollect}. */
class EnumerableUncollectTest {

  @Test void simpleUnnestArray() {
    final String sql = "select * from UNNEST(array[3, 4]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=3",
            "y=4");
  }

  @Test void simpleUnnestNullArray() {
    final String sql = "SELECT * FROM UNNEST(CAST(null AS INTEGER ARRAY))";
    tester()
        .query(sql)
        .returnsCount(0);
  }

  @Test void simpleUnnestArrayOfArrays() {
    final String sql = "select * from UNNEST(array[array[3], array[4]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=[3]",
            "y=[4]");
  }

  @Test void simpleUnnestArrayOfArrays2() {
    final String sql = "select * from UNNEST(array[array[3, 4], array[4, 5]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=[3, 4]",
            "y=[4, 5]");
  }

  @Test void simpleUnnestArrayOfArrays3() {
    final String sql = "select * from UNNEST("
        + "array[array[array[3,4], array[4,5]], array[array[7,8], array[9,10]]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=[[3, 4], [4, 5]]",
            "y=[[7, 8], [9, 10]]");
  }

  @Test void simpleUnnestArrayOfRows() {
    final String sql = "select * from UNNEST(array[ROW(3), ROW(4)]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=3",
            "y=4");
  }

  @Test void simpleUnnestArrayOfRows2() {
    final String sql = "select * from UNNEST(array[ROW(3, 5), ROW(4, 6)]) as T2(y, z)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=3; z=5",
            "y=4; z=6");
  }

  @Test void simpleUnnestArrayOfRows3() {
    final String sql = "select * from UNNEST(array[ROW(3), ROW(4)]) WITH ORDINALITY as T2(y, o)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=3; o=1",
            "y=4; o=2");
  }

  @Test void simpleUnnestArrayOfRows4() {
    final String sql = "select * from UNNEST(array[ROW(1, ROW(5, 10)), ROW(2, ROW(6, 12))]) "
        + "as T2(y, z)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y=1; z={5, 10}",
            "y=2; z={6, 12}");
  }

  @Test void simpleUnnestArrayOfRows5() {
    final String sql = "select * from UNNEST(array[ROW(ROW(3)), ROW(ROW(4))]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "y={3}",
            "y={4}");
  }

  @Test void chainedUnnestArray() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[3, 4]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=3",
            "x=1; y=4",
            "x=2; y=3",
            "x=2; y=4");
  }

  @Test void chainedUnnestArrayOfArrays() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[array[3], array[4]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=[3]",
            "x=1; y=[4]",
            "x=2; y=[3]",
            "x=2; y=[4]");
  }

  @Test void chainedUnnestArrayOfArrays2() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[array[3, 4], array[4, 5]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=[3, 4]",
            "x=1; y=[4, 5]",
            "x=2; y=[3, 4]",
            "x=2; y=[4, 5]");
  }

  @Test void chainedUnnestArrayOfArrays3() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[array[array[3,4], array[4,5]], array[array[7,8], array[9,10]]]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=[[3, 4], [4, 5]]",
            "x=1; y=[[7, 8], [9, 10]]",
            "x=2; y=[[3, 4], [4, 5]]",
            "x=2; y=[[7, 8], [9, 10]]");
  }

  @Test void chainedUnnestArrayOfRows() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[ROW(3), ROW(4)]) as T2(y)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=3",
            "x=1; y=4",
            "x=2; y=3",
            "x=2; y=4");
  }

  @Test void chainedUnnestArrayOfRows2() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[ROW(3, 5), ROW(4, 6)]) as T2(y, z)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=3; z=5",
            "x=1; y=4; z=6",
            "x=2; y=3; z=5",
            "x=2; y=4; z=6");
  }

  @Test void chainedUnnestArrayOfRows3() {
    final String sql = "select * from (values (1), (2)) T1(x),"
        + "UNNEST(array[ROW(3), ROW(4)]) WITH ORDINALITY as T2(y, o)";
    tester()
        .query(sql)
        .returnsUnordered(
            "x=1; y=3; o=1",
            "x=1; y=4; o=2",
            "x=2; y=3; o=1",
            "x=2; y=4; o=2");
  }

  private CalciteAssert.AssertThat tester() {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false);
  }
}
