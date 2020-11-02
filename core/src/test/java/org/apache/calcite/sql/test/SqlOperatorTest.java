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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.SqlValidatorTestCase;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.calcite.sql.fun.SqlLibrary.HIVE;
import static org.apache.calcite.sql.fun.SqlLibrary.ORACLE;
import static org.apache.calcite.sql.fun.SqlLibrary.SPARK;
import static org.apache.calcite.sql.fun.SqlLibrary.STANDARD;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Concrete subclass of {@link SqlOperatorBaseTest} which checks against
 * a {@link SqlValidator}. Tests that involve execution trivially succeed.
 */
class SqlOperatorTest extends SqlOperatorBaseTest {
  private static final SqlTester DEFAULT_TESTER =
      new SqlValidatorTestCase().getTester();

  /**
   * Creates a SqlOperatorTest.
   */
  SqlOperatorTest() {
    super(false, DEFAULT_TESTER);
  }

  @Test void testStandardSqlLibrary() {
    Set<SqlLibrary> librarySet =
        SqlStdOperatorTable.instance().getOperatorLibraries(SqlStdOperatorTable.PLUS);
    assertThat(librarySet, notNullValue());
    assertThat(librarySet.contains(STANDARD), is(true));

    librarySet = SqlStdOperatorTable.instance().getOperatorLibraries(
        SqlStdOperatorTable.GREATER_THAN);
    assertThat(librarySet, notNullValue());
    assertThat(librarySet.contains(STANDARD), is(true));
    assertThat(librarySet.contains(STANDARD), is(true));

    librarySet = SqlStdOperatorTable.instance().getOperatorLibraries(
        SqlStdOperatorTable.AVG);
    assertThat(librarySet, notNullValue());
    assertThat(librarySet.contains(STANDARD), is(true));
  }

  @Test void testOtherSqlLibrary() {
    Set<SqlLibrary> librarySet = SqlStdOperatorTable.instance().getOperatorLibraries(
        SqlLibraryOperators.IF);
    assertThat(librarySet, notNullValue());
    assertThat(librarySet.contains(HIVE), is(true));
    assertThat(librarySet.contains(SPARK), is(true));

    librarySet = SqlStdOperatorTable.instance().getOperatorLibraries(
        SqlLibraryOperators.DECODE);
    assertThat(librarySet, notNullValue());
    assertThat(librarySet.contains(ORACLE), is(true));
  }
}
