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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tester of {@link SqlValidator} and runtime execution of the input SQL.
 */
class SqlRuntimeTester extends AbstractSqlTester {
  SqlRuntimeTester(SqlTestFactory factory,
      UnaryOperator<SqlValidator> validatorTransform) {
    super(factory, validatorTransform);
  }

  @Override protected SqlTester with(SqlTestFactory factory) {
    return new SqlRuntimeTester(factory, validatorTransform);
  }

  public SqlTester withValidatorTransform(
      UnaryOperator<UnaryOperator<SqlValidator>> transform) {
    return new SqlRuntimeTester(factory,
        transform.apply(validatorTransform));
  }

  @Override public void checkFails(StringAndPos sap, String expectedError,
      boolean runtime) {
    final StringAndPos sap2 =
        StringAndPos.of(runtime ? buildQuery2(sap.addCarets())
            : buildQuery(sap.addCarets()));
    assertExceptionIsThrown(sap2, expectedError, runtime);
  }

  @Override public void checkAggFails(
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    final StringAndPos sap = StringAndPos.of(query);
    assertExceptionIsThrown(sap, expectedError, runtime);
  }

  public void assertExceptionIsThrown(
      StringAndPos sap,
      String expectedMsgPattern) {
    assertExceptionIsThrown(sap, expectedMsgPattern, false);
  }

  public void assertExceptionIsThrown(StringAndPos sap,
      String expectedMsgPattern, boolean runtime) {
    final SqlNode sqlNode;
    try {
      sqlNode = parseQuery(sap.sql);
    } catch (Throwable e) {
      checkParseEx(e, expectedMsgPattern, sap);
      return;
    }

    Throwable thrown = null;
    final SqlTests.Stage stage;
    final SqlValidator validator = getValidator();
    if (runtime) {
      stage = SqlTests.Stage.RUNTIME;
      SqlNode validated = validator.validate(sqlNode);
      assertNotNull(validated);
      try {
        check(sap.sql, SqlTests.ANY_TYPE_CHECKER,
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
