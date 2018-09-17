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
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.CheckUtil;

import static org.junit.Assert.assertNotNull;

/**
 * Tester of {@link SqlValidator} and runtime execution of the input SQL.
 */
public class SqlRuntimeTester extends AbstractSqlTester {
  public SqlRuntimeTester(SqlTestFactory factory) {
    super(factory);
  }

  @Override protected SqlTester with(SqlTestFactory factory) {
    return new SqlRuntimeTester(factory);
  }

  @Override public void checkFails(String expression, String expectedError, boolean runtime) {
    assertExceptionIsThrown(
        runtime ? buildQuery2(expression) : buildQuery(expression),
        expectedError,
        runtime
    );
  }

  public void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern) {
    assertExceptionIsThrown(sql, expectedMsgPattern, false);
  }

  public void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern,
      boolean runtime) {
    SqlValidator validator;
    SqlNode sqlNode;
    SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
    try {
      sqlNode = parseQuery(sap.sql);
    } catch (Throwable e) {
      checkParseEx(e, expectedMsgPattern, sap.sql);
      return;
    }

    Throwable thrown = null;
    CheckUtil.Stage stage;
    validator = getValidator();
    if (runtime) {
      stage = CheckUtil.Stage.RUNTIME;
      SqlNode validated = validator.validate(sqlNode);
      assertNotNull(validated);
      try {
        check(sap.sql, SqlTests.ANY_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
            SqlTests.ANY_RESULT_CHECKER);
      } catch (Throwable ex) {
        // get the real exception in runtime check
        thrown = ex;
      }
    } else {
      stage = CheckUtil.Stage.VALIDATE;
      try {
        validator.validate(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }
    }

    CheckUtil.checkEx(thrown, expectedMsgPattern, sap, stage);
  }
}

// End SqlRuntimeTester.java
