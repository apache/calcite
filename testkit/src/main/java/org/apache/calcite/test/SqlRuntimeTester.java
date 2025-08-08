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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.validate.SqlValidator;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tester of {@link SqlValidator} and runtime execution of the input SQL.
 */
class SqlRuntimeTester extends AbstractSqlTester {
  SqlRuntimeTester() {
  }

  @Override public void checkFails(SqlTestFactory factory, StringAndPos sap,
      String expectedError, boolean runtime) {
    String built = buildQuery(sap);
    final StringAndPos sap2 = StringAndPos.of(SqlParserUtil.escapeCarets(built));
    assertExceptionIsThrown(factory, sap2, expectedError, runtime);
  }

  @Override public void checkAggFails(SqlTestFactory factory,
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    final StringAndPos sap = StringAndPos.of(query);
    assertExceptionIsThrown(factory, sap, expectedError, runtime);
  }

  @Override public void assertExceptionIsThrown(SqlTestFactory factory,
      StringAndPos sap, @Nullable String expectedMsgPattern) {
    assertExceptionIsThrown(factory, sap, expectedMsgPattern, false);
  }

  public void assertExceptionIsThrown(SqlTestFactory factory,
      StringAndPos sap, @Nullable String expectedMsgPattern, boolean runtime) {
    final SqlNode sqlNode;
    try {
      sqlNode = parseQuery(factory, sap.sql);
    } catch (Throwable e) {
      checkParseEx(e, expectedMsgPattern, sap);
      return;
    }

    Throwable thrown = null;
    final SqlTests.Stage stage;
    final SqlValidator validator = factory.createValidator();
    if (runtime) {
      stage = SqlTests.Stage.RUNTIME;
      SqlNode validated = validator.validate(sqlNode);
      assertNotNull(validated);
      try {
        check(factory, sap.addCarets(), SqlTests.ANY_TYPE_CHECKER,
            SqlTests.ANY_PARAMETER_CHECKER, SqlTests.ANY_RESULT_CHECKER);
      } catch (Throwable ex) {
        // get the real exception in runtime check
        thrown = ex;
      }
    } else {
      stage = SqlTests.Stage.VALIDATE;
      try {
        validator.validate(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }
    }

    SqlTests.checkEx(thrown, expectedMsgPattern, sap, stage);
  }
}
