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
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6968">[CALCITE-6968]
   * SqlUpdate#getOperandList return operands' missing 'sourceSelect'</a>. */
  @Test void testSqlUpdateGetOperandsMatchWithSetOperand() {
    SqlUpdate sqlUpdate =
        new SqlUpdate(SqlParserPos.ZERO, new SqlIdentifier("table1", SqlParserPos.ZERO),
            SqlNodeList.EMPTY,
            SqlNodeList.EMPTY,
            null,
            null,
            null);
    SqlNode targetTable = new SqlIdentifier("table2", SqlParserPos.ZERO);
    final SqlIdentifier field1 = new SqlIdentifier("field1", SqlParserPos.ZERO);
    final SqlIdentifier field2 = new SqlIdentifier("field2", SqlParserPos.ZERO);
    final SqlIdentifier field3 = new SqlIdentifier("field3", SqlParserPos.ZERO);
    final SqlNodeList targetColumnList = SqlNodeList.of(field2);
    final SqlNodeList sourceExpressionList = SqlNodeList.of(field3);
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
    sqlUpdate.setOperand(0, targetTable);
    sqlUpdate.setOperand(1, targetColumnList);
    sqlUpdate.setOperand(2, sourceExpressionList);
    sqlUpdate.setOperand(3, condition);
    sqlUpdate.setOperand(4, sourceSelect);
    sqlUpdate.setOperand(5, alias);
    final List<SqlNode> operandList = sqlUpdate.getOperandList();
    assertThat(operandList, hasSize(6));
    assertThat(sqlUpdate.getTargetTable(), equalTo(operandList.get(0)));
    assertThat(sqlUpdate.getTargetColumnList(), equalTo(operandList.get(1)));
    assertThat(sqlUpdate.getSourceExpressionList(), equalTo(operandList.get(2)));
    assertThat(sqlUpdate.getCondition(), equalTo(operandList.get(3)));
    assertThat(sqlUpdate.getSourceSelect(), equalTo(operandList.get(4)));
    assertThat(sqlUpdate.getAlias(), equalTo(operandList.get(5)));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6968">[CALCITE-6968]
   * SqlUpdate#getOperandList return operands' missing 'sourceSelect'</a>. */
  @Test void testSqlUpdateClonePreservesSourceSelect() {
    final SqlIdentifier field1 = new SqlIdentifier("field1", SqlParserPos.ZERO);
    final SqlSelect sourceSelect =
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
    final SqlIdentifier alias = new SqlIdentifier("alias", SqlParserPos.ZERO);
    final SqlUpdate sqlUpdate =
        new SqlUpdate(SqlParserPos.ZERO, new SqlIdentifier("table1", SqlParserPos.ZERO),
            SqlNodeList.of(field1),
            SqlNodeList.of(SqlLiteral.createCharString("field1Value", SqlParserPos.ZERO)),
            null,
            sourceSelect,
            alias);
    final SqlUpdate cloned = (SqlUpdate) sqlUpdate.clone(SqlParserPos.ZERO);
    assertThat(cloned.getOperandList(), hasSize(6));
    assertThat(cloned.getSourceSelect(), equalTo(sourceSelect));
    assertThat(cloned.getAlias(), equalTo(alias));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6968">[CALCITE-6968]
   * SqlUpdate#getOperandList return operands' missing 'sourceSelect'</a>. */
  @Test void testSqlUpdateUnparseIgnoresSourceSelect() {
    final SqlIdentifier targetColumn = new SqlIdentifier("field1", SqlParserPos.ZERO);
    final SqlSelect sourceSelect =
        new SqlSelect(SqlParserPos.ZERO, null,
            SqlNodeList.of(new SqlIdentifier("internalField", SqlParserPos.ZERO)),
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
    final SqlUpdate sqlUpdate =
        new SqlUpdate(SqlParserPos.ZERO, new SqlIdentifier("table1", SqlParserPos.ZERO),
            SqlNodeList.of(targetColumn),
            SqlNodeList.of(SqlLiteral.createCharString("field1Value", SqlParserPos.ZERO)),
            null,
            sourceSelect,
            new SqlIdentifier("alias", SqlParserPos.ZERO));
    final String sql =
        sqlUpdate.toSqlString(c -> c.withClauseStartsLine(false)).getSql();
    assertThat(sql, containsString("UPDATE"));
    assertThat(sql, containsString("field1Value"));
    assertThat(sql.contains("internalField"), equalTo(false));
    assertThat(sql.contains("SELECT"), equalTo(false));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6968">[CALCITE-6968]
   * SqlUpdate#getOperandList return operands' missing 'sourceSelect'</a>. */
  @Test void testSqlUpdateUnparseIgnoresSourceSelectAfterParsingSql()
      throws SqlParseException {
    final SqlUpdate sqlUpdate =
        (SqlUpdate) SqlParser.create("UPDATE table1 AS alias "
            + "SET field1 = 'field1Value' "
            + "WHERE field1 = 'field1Value'")
            .parseStmt();
    final SqlSelect sourceSelect =
        (SqlSelect) SqlParser.create("SELECT INTERNAL_MARKER FROM INTERNAL_SOURCE")
            .parseQuery();
    sqlUpdate.setSourceSelect(sourceSelect);
    final String sql =
        sqlUpdate.toSqlString(c -> c.withClauseStartsLine(false)).getSql();
    assertThat(sql, containsString("UPDATE"));
    assertThat(sql, containsString("field1Value"));
    assertThat(sql.contains("INTERNAL_MARKER"), equalTo(false));
    assertThat(sql.contains("INTERNAL_SOURCE"), equalTo(false));
    assertThat(sql.contains("SELECT"), equalTo(false));
  }
}
