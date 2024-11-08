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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.catchall.CatchallSchema;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableCalc}.
 */
class EnumerableCalcTest {

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3536">[CALCITE-3536]
   * NPE when executing plan with Coalesce due to wrong NullAs strategy</a>.
   */
  @Test void testCoalesceImplementation() {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .withRel(
            builder -> builder
                .scan("s", "emps")
                .project(
                  builder.call(
                    SqlStdOperatorTable.COALESCE,
                    builder.field("commission"),
                    builder.literal(0)))
                .build())
        .planContains("input_value != null ? input_value : 0")
        .returnsUnordered(
            "$f0=0",
            "$f0=250",
            "$f0=500",
            "$f0=1000");
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4419">[CALCITE-4419]
   * Posix regex operators cannot be used within RelBuilder</a>.
   */
  @Test void testPosixRegexCaseSensitive() {
    checkPosixRegex("E..c", SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE)
        .returnsUnordered("empid=200; name=Eric");
    checkPosixRegex("e..c", SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE)
        .returnsUnordered();
  }

  @Test void testPosixRegexCaseInsensitive() {
    checkPosixRegex("E..c", SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE)
        .returnsUnordered("empid=200; name=Eric");
    checkPosixRegex("e..c", SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE)
        .returnsUnordered("empid=200; name=Eric");
  }

  @Test void testNegatedPosixRegexCaseSensitive() {
    checkPosixRegex("E..c", SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE)
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
    checkPosixRegex("e..c", SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE)
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian",
            "empid=200; name=Eric");
  }

  @Test void testNegatedPosixRegexCaseInsensitive() {
    checkPosixRegex("E..c", SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE)
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
    checkPosixRegex("e..c", SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE)
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  private CalciteAssert.AssertQuery checkPosixRegex(
      String literalValue,
      SqlOperator operator) {
    return CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .withRel(
            builder -> builder
                .scan("s", "emps")
                .filter(
                    builder.call(
                        operator,
                        builder.field("name"),
                        builder.literal(literalValue)))
                .project(
                    builder.field("empid"),
                    builder.field("name"))
                .build());
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6680">[CALCITE-6680]
   * RexImpTable erroneously declares NullPolicy.NONE for IS_EMPTY</a>. */
  @Test public void testEmptyCheckOnArray() {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new CatchallSchema()))
        .withRel(builder -> builder
            .scan("s", "everyTypes")
            .project(builder.call(SqlStdOperatorTable.IS_EMPTY, builder.field("list")))
            .build())
        .planContains("input_value != null && input_value.isEmpty()")
        .returnsUnordered("$f0=false", "$f0=true");
  }
}
