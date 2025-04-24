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
package org.apache.calcite.sql;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for SqlCall operands' order and size match with set.
 * See <a href="https://issues.apache.org/jira/browse/CALCITE-6964">[CALCITE-6964]</a>
 *
 */
public class SqlCallOperandsTest {
  @Test void testSqlDeleteGetOperandsMatchWithSetOperand() {
    SqlDelete sqlDelete =
        new SqlDelete(SqlParserPos.ZERO, new SqlIdentifier("table1", SqlParserPos.ZERO),
            null,
            null,
            null);
    SqlNode targetTable = new SqlIdentifier("table2", SqlParserPos.ZERO);
    final SqlIdentifier field1 = new SqlIdentifier("field1", SqlParserPos.ZERO);
    SqlNode condition =
        SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, field1,
            SqlLiteral.createCharString("field1Value", SqlParserPos.ZERO));
    SqlSelect sourceSelect =
        new SqlSelect(SqlParserPos.ZERO, null,
            SqlNodeList.of(field1),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    SqlIdentifier alias = new SqlIdentifier("alias", SqlParserPos.ZERO);
    sqlDelete.setOperand(0, targetTable);
    sqlDelete.setOperand(1, condition);
    sqlDelete.setOperand(2, sourceSelect);
    sqlDelete.setOperand(3, alias);
    final List<SqlNode> operandList = sqlDelete.getOperandList();
    // Verify if the operands are in the correct position in the operandList
    assertThat(operandList, hasSize(4));
    assertThat(sqlDelete.getTargetTable(), equalTo(operandList.get(0)));
    assertThat(sqlDelete.getCondition(), equalTo(operandList.get(1)));
    assertThat(sqlDelete.getSourceSelect(), equalTo(operandList.get(2)));
    assertThat(sqlDelete.getAlias(), equalTo(operandList.get(3)));
  }
}
