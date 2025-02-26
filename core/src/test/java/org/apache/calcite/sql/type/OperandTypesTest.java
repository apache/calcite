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
package org.apache.calcite.sql.type;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.test.SqlOperatorFixture;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.test.Fixtures;

import org.junit.jupiter.api.Test;

/**
 * Tests for operand checkers in {@link OperandTypes}.
 */
public class OperandTypesTest {

  @Test void testSequenceOfFamily() throws Exception {
    // Test for https://issues.apache.org/jira/browse/CALCITE-5479. The original checker that
    // inspired this test was doing "and(family(STRING), LITERAL)". We tweak this a bit here because
    // the test fixture does not preserve literals.
    final SqlOperandTypeChecker fChecker =
        OperandTypes.sequence("F(<STRING>, <TIMESTAMP or DATE>)",
            OperandTypes.STRING, OperandTypes.or(OperandTypes.TIMESTAMP, OperandTypes.DATE));
    final SqlOperator fOperator =
        new SqlFunction("F", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null, fChecker,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
    final SqlOperatorTable operatorTable = SqlOperatorTables.of(fOperator, SqlStdOperatorTable.ROW);
    try (SqlOperatorFixture f = Fixtures.forOperators(false).withOperatorTable(operatorTable)) {
      f.setFor(fOperator);
      f.checkType("F('foo', TIMESTAMP '2000-01-01 00:00:00')", "BIGINT NOT NULL");
      f.checkFails("^F('foo', 1)^",
          "Cannot apply 'F' to arguments of type 'F\\(<CHAR\\(3\\)>, <INTEGER>\\)'\\. Supported "
              + "form\\(s\\): F\\(<STRING>, <TIMESTAMP or DATE>\\)", false);
    }

  }

}
