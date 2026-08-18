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

import org.junit.jupiter.api.Test;

/**
 * Tests for column aliases used as {@code PARTITION BY} keys of a window
 * aggregate.
 *
 * <p>SQL allows quoted identifiers to contain almost any character;
 * however, the enumerable convention materialises multi-column
 * {@code PARTITION BY} keys as fields of a synthetic Java class emitted
 * into generated source. Field names that are not valid Java identifiers
 * are transparently renamed to positional placeholders
 * ({@code f0}, {@code f1}, ...) inside the synthetic class, while the
 * original SQL name is preserved at the rowtype level for outer
 * references.
 */
class WindowPartitionAliasTest {

  /** A quoted alias that is a valid Java identifier plans and runs
   * normally when used as one of several {@code PARTITION BY} keys. The
   * projection wraps the aliased column in a computation so field-
   * trimming does not fold the sub-query away. */
  @Test void testValidIdentifierAlias() {
    final String sql = "select \"aliased_deptno\","
        + " count(*) over ("
        + "  partition by \"aliased_deptno\", \"empid\") as c"
        + " from ("
        + "    select \"deptno\" + 1 as \"aliased_deptno\", \"empid\""
        + "      from \"hr\".\"emps\")";
    CalciteAssert.hr()
        .query(sql)
        .runs();
  }

  /** A quoted alias containing a space (a legal SQL identifier character
   * that is not a legal Java identifier character) still runs: the
   * synthetic partition-key class carries a positional field name while
   * the outer projection continues to see the SQL alias. */
  @Test void testAliasWithSpaceRuns() {
    final String sql = "select \"has space\","
        + " count(*) over ("
        + "  partition by \"has space\", \"empid\") as c"
        + " from ("
        + "    select \"deptno\" + 1 as \"has space\", \"empid\""
        + "      from \"hr\".\"emps\")";
    CalciteAssert.hr()
        .query(sql)
        .runs();
  }

  /** Same shape as above, with a punctuation character. */
  @Test void testAliasWithPunctuationRuns() {
    final String sql = "select \"a.b\","
        + " count(*) over ("
        + "  partition by \"a.b\", \"empid\") as c"
        + " from ("
        + "    select \"deptno\" + 1 as \"a.b\", \"empid\""
        + "      from \"hr\".\"emps\")";
    CalciteAssert.hr()
        .query(sql)
        .runs();
  }
}
