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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for SqlCall operands' order and size match with set.
 * See <a href="https://issues.apache.org/jira/browse/CALCITE-6964">[CALCITE-6964]</a>
 *
 */
public class SqlCallOperandsTest {
  @Test void testSqlWithItemSearchAndCycleOperands() throws SqlParseException {
    final String sql =
        "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3)\n"
            + "SEARCH DEPTH FIRST BY n SET seq CYCLE n SET c USING p\n"
            + "SELECT * FROM t";
    final SqlWith with = (SqlWith) SqlParser.create(sql).parseQuery();
    final SqlWithItem item = (SqlWithItem) with.withList.get(0);
    assertThat(item.getOperandList(), hasSize(6));
    assertThat(item.operand(4), sameInstance(item.searchClause));
    assertThat(item.operand(5), sameInstance(item.cycleClause));

    // Cloning recreates the call from its operand list.
    final SqlWithItem copy = (SqlWithItem) item.clone(SqlParserPos.ZERO);
    assertThat(copy.searchClause, sameInstance(item.searchClause));
    assertThat(copy.cycleClause, sameInstance(item.cycleClause));
    copy.setOperand(4, null);
    assertThat(copy.searchClause, nullValue());
    assertThat(copy.cycleClause, sameInstance(item.cycleClause));
    copy.setOperand(5, null);
    assertThat(copy.cycleClause, nullValue());
    copy.setOperand(4, item.searchClause);
    copy.setOperand(5, item.cycleClause);
    assertThat(copy.getOperandList(), equalTo(item.getOperandList()));
  }

  @Test void testSqlWithItemCreateCall() throws SqlParseException {
    final String sql =
        "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3)\n"
            + "SEARCH DEPTH FIRST BY n SET seq CYCLE n SET c USING p\n"
            + "SELECT * FROM t";
    final SqlWith with = (SqlWith) SqlParser.create(sql).parseQuery();
    final SqlWithItem item = (SqlWithItem) with.withList.get(0);
    final SqlOperator operator = item.getOperator();
    final SqlWithItem withoutClauses =
        (SqlWithItem) operator.createCall(SqlParserPos.ZERO,
            item.name, item.columnList, item.query, item.recursive);
    assertThat(withoutClauses.searchClause, nullValue());
    assertThat(withoutClauses.cycleClause, nullValue());

    // Six operands preserve the clause positions even when either clause is absent.
    for (SqlSearchClause search : Arrays.asList(null, item.searchClause)) {
      for (SqlCycleClause cycle : Arrays.asList(null, item.cycleClause)) {
        final SqlWithItem copy =
            (SqlWithItem) operator.createCall(SqlParserPos.ZERO,
                item.name, item.columnList, item.query, item.recursive, search, cycle);
        assertThat(copy.searchClause, sameInstance(search));
        assertThat(copy.cycleClause, sameInstance(cycle));
      }
    }

    // Five operands are ambiguous: the final operand could be SEARCH or CYCLE.
    for (SqlNode clause : Arrays.asList(null, item.searchClause, item.cycleClause)) {
      final IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class,
              () -> operator.createCall(SqlParserPos.ZERO,
                  item.name, item.columnList, item.query, item.recursive, clause));
      assertThat(exception.getMessage(), equalTo("Expected 4 or 6 operands, got 5"));
    }
  }

  @Test void testSqlBasicCallSetOperandList() {
    final SqlLiteral one =
        SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    final SqlLiteral two =
        SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO);
    final SqlLiteral three =
        SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO);
    final SqlBasicCall call =
        (SqlBasicCall) SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
            SqlParserPos.ZERO, one, two);
    final List<SqlNode> originalOperands = call.getOperandList();

    call.setOperandList(Arrays.asList(two, three));

    assertThat(call.getOperandList(), equalTo(Arrays.asList(two, three)));
    // Bulk replacement preserves the snapshot behavior of setOperand.
    assertThat(originalOperands, equalTo(Arrays.asList(one, two)));
  }

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
