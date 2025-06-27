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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

/**
 * Class for expanding project expressions in SQL generation.
 */

class ProjectExpansionUtil {

  private SqlNode createAsSqlNode(String columnName, String identifierName) {
    return SqlStdOperatorTable.AS.createCall(POS,
        ImmutableList.of(
            new SqlIdentifier(
                ImmutableList.of(identifierName,
                    columnName.replaceAll("\\d+$", "")), SqlParserPos.ZERO),
            new SqlIdentifier(columnName, SqlParserPos.ZERO)));
  }

  private void updateResultSelectList(
      SqlImplementor.Result result, List<SqlNode> sqlIdentifierList) {
    if (sqlIdentifierList.size() >= 1 && result.node instanceof SqlSelect) {

      ((SqlSelect) result.node).setSelectList(new SqlNodeList(sqlIdentifierList, POS));
    }
  }

  private static boolean endsWithDigit(String str) {
    return !str.isEmpty() && Character.isDigit(str.charAt(str.length() - 1));
  }

  private List<String> getColumnsUsedInOnConditionWithSubQueryAlias(
      SqlNode sqlCondition, String neededAlias) {
    List<String> columnsUsed = new ArrayList<>();
    List<SqlIdentifier> sqlIdentifierList =
        collectSqlIdentifiers(((SqlBasicCall) sqlCondition).getOperandList());
    for (SqlIdentifier sqlIdentifier : sqlIdentifierList) {
      String alias = sqlIdentifier.names.get(0);
      String columnName = sqlIdentifier.names.get(1);
      if (alias.equals(neededAlias) && !columnsUsed.contains(columnName)) {
        columnsUsed.add(columnName);
      }
    }
    return columnsUsed;
  }

  private List<SqlIdentifier> collectSqlIdentifiers(List<SqlNode> sqlNodes) {
    List<SqlIdentifier> sqlIdentifiers = new ArrayList<>();

    for (SqlNode sqlNode : sqlNodes) {
      // Check if the SqlNode is a SqlBasicCall
      if (sqlNode instanceof SqlBasicCall) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        // Recursively process the operands of the SqlBasicCall
        collectSqlIdentifiersFromCall(sqlBasicCall, sqlIdentifiers);
      }
    }

    return sqlIdentifiers;
  }

  private void collectSqlIdentifiersFromCall(
      SqlBasicCall sqlBasicCall, List<SqlIdentifier> sqlIdentifiers) {
    for (SqlNode operand : sqlBasicCall.getOperandList()) {
      // If operand is SqlIdentifier, add it to the list
      if (operand instanceof SqlIdentifier) {
        sqlIdentifiers.add((SqlIdentifier) operand);
      } else if (operand instanceof SqlBasicCall) {
        collectSqlIdentifiersFromCall((SqlBasicCall) operand, sqlIdentifiers);
      }
    }
  }

  private boolean isJoinNodeBasicCall(SqlImplementor.Result result) {
    return result.node instanceof SqlSelect && ((SqlSelect) result.node).getFrom()
        instanceof SqlJoin && (((SqlJoin) ((SqlSelect) result.node).getFrom()).getLeft())
        instanceof SqlBasicCall;
  }

  private boolean isLeftOfJoinNodeSqlJoin(SqlImplementor.Result result) {
    return result.node instanceof SqlSelect && ((SqlSelect) result.node).getFrom()
        instanceof SqlJoin && (((SqlJoin) ((SqlSelect) result.node).getFrom()).getLeft())
        instanceof SqlJoin;
  }

  protected void handleResultAliasIfNeeded(SqlImplementor.Result result, SqlNode sqlCondition) {
    if (shouldHandleResultAlias(result, sqlCondition)) {
      List<String> fieldNames = result.neededType.getFieldNames();
      List<String> columnsUsed =
          getColumnsUsedInOnConditionWithSubQueryAlias(sqlCondition, result.neededAlias);

      List<SqlNode> sqlIdentifierList = new ArrayList<>();
      for (String columnName : columnsUsed) {
        if (fieldNames.contains(columnName)) {
          SqlNode sqlIdentifier = createSqlIdentifierForColumn(result, columnName);
          if (!sqlIdentifierList.contains(sqlIdentifier)) {
            sqlIdentifierList.add(createSqlIdentifierForColumn(result, columnName));
          }
        }
      }
      if (result.node instanceof SqlSelect && ((SqlSelect) result.node).getSelectList() == null
          && !hasAliasEndingWithDigit(sqlIdentifierList)) {
        return;
      }
      if (result.node instanceof SqlSelect && ((SqlSelect) result.node).getFrom()
          instanceof SqlJoin) {
        updateResultSelectList(result, sqlIdentifierList);
      }
    }
  }

  private boolean hasAliasEndingWithDigit(List<SqlNode> sqlIdentifierList) {
    return sqlIdentifierList.stream()
        .filter(obj -> obj instanceof SqlBasicCall)
        .map(obj -> (SqlBasicCall) obj)
        .filter(call -> call.getOperator().getName().equalsIgnoreCase("AS"))
        .map(call -> call.operand(1))
        .filter(op -> op instanceof SqlIdentifier)
        .map(op -> (SqlIdentifier) op)
        .map(id -> id.names.get(0))
        .anyMatch(ProjectExpansionUtil::endsWithDigit);
  }

  private boolean shouldHandleResultAlias(SqlImplementor.Result result, SqlNode sqlCondition) {
    String backTick = "`";
    return result.neededAlias != null
        && sqlCondition.toString().contains(backTick + result.neededAlias + backTick)
        && result.asSelect().getSelectList() == null && !hasAmbiguousAlias(result);
  }

  private boolean hasAmbiguousAlias(SqlImplementor.Result result) {
    if (result.node instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) result.node;
      List<SqlNode> fromElements = getFromElements(sqlSelect);
      return doesAliasMatchNeededAlias(fromElements, result.neededAlias);
    }
    return false;
  }

  private List<SqlNode> getFromElements(SqlSelect sqlSelect) {
    List<SqlNode> fromElements = new ArrayList<>();
    if (sqlSelect.getFrom() instanceof SqlJoin) {
      SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
      fromElements.add(sqlJoin.getLeft());
      fromElements.add(sqlJoin.getRight());
    } else {
      fromElements.add(sqlSelect.getFrom());
    }
    return fromElements;
  }

  private boolean doesAliasMatchNeededAlias(List<SqlNode> fromElements, String neededAlias) {
    for (SqlNode fromElement : fromElements) {
      if (fromElement instanceof SqlBasicCall) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) fromElement;
        if (sqlBasicCall.getOperator() == SqlStdOperatorTable.AS) {
          if (sqlBasicCall.operand(1) instanceof SqlIdentifier) {
            SqlIdentifier alias = sqlBasicCall.operand(1);
            if (alias.names.get(0).equalsIgnoreCase(neededAlias)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private SqlNode createSqlIdentifierForColumn(SqlImplementor.Result result, String columnName) {
    if (endsWithDigit(columnName) && result.node instanceof SqlSelect
        && !(((SqlSelect) result.node).getFrom() instanceof SqlIdentifier)) {
      return createAsSqlIdentifierForColumn(result, columnName);
    } else {
      if (isJoinNodeBasicCall(result)) {
        SqlBasicCall sqlBasicCall = getLeftMostOperand((SqlSelect) result.node);
        SqlIdentifier sqlIdentifier = findSingleIdentifierInOperands(sqlBasicCall);
        if (sqlIdentifier != null) {
          return new SqlIdentifier(ImmutableList.of(sqlIdentifier.names.get(0), columnName), POS);
        }
      } else if (isLeftOfJoinNodeSqlJoin(result)) {
        SqlJoin sqlJoin = getLeftMostSqlJoin((SqlSelect) result.node);
        List<String> tableName = getQualifiedTableName(result, columnName);
        return sqlJoin.getLeft() instanceof SqlIdentifier &&  tableName.size() > 0
            ? new SqlIdentifier(
                ImmutableList.of(tableName.get(tableName.size() - 1),
            columnName), POS)
            : new SqlIdentifier(ImmutableList.of(columnName), POS);
      }
      return new SqlIdentifier(ImmutableList.of(columnName), POS);
    }
  }

  private static List<String> getQualifiedTableName(SqlImplementor.Result result,
      String columnName) {
    List<TableInfo> tableInfoList = new ArrayList<>();
    populateTableInfo(result.expectedRel, tableInfoList);
    return getFirstTableNameWithColumn(tableInfoList, columnName);
  }

  private static List<String> getFirstTableNameWithColumn(List<TableInfo> tableInfoList,
      String columnName) {
    for (TableInfo tableInfo : tableInfoList) {
      if (tableInfo.columnExists(columnName)) {
        return tableInfo.tableName;
      }
    }
    return new ArrayList<>();
  }

  private SqlBasicCall getLeftMostOperand(SqlSelect sqlSelect) {
    return (SqlBasicCall) ((SqlJoin) sqlSelect.getFrom()).getLeft();
  }

  private SqlJoin getLeftMostSqlJoin(SqlSelect sqlSelect) {
    return (SqlJoin) ((SqlJoin) sqlSelect.getFrom()).getLeft();
  }
  private SqlNode createAsSqlIdentifierForColumn(
      SqlImplementor.Result leftResult, String columnName) {
    SqlBasicCall sqlBasicCall = extractSqlBasicCallFromResult(leftResult);
    SqlIdentifier sqlIdentifier = findSingleIdentifierInOperands(sqlBasicCall);
    return createAsSqlNode(columnName, sqlIdentifier.names.get(0));
  }

  private SqlBasicCall extractSqlBasicCallFromJoin(SqlJoin sqlJoin) {
    return (SqlBasicCall) ((SqlJoin) sqlJoin.getLeft()).getLeft();
  }

  private SqlBasicCall extractSqlBasicCallFromResult(SqlImplementor.Result result) {
    SqlSelect sqlSelect = (SqlSelect) result.node;
    if (sqlSelect.getFrom() instanceof SqlJoin) {
      SqlJoin sqlJoin = (SqlJoin) sqlSelect.getFrom();
      return extractSqlBasicCallFromJoin(sqlJoin);
    } else {
      return (SqlBasicCall) sqlSelect.getFrom();
    }
  }

  private SqlIdentifier findSingleIdentifierInOperands(SqlBasicCall sqlBasicCall) {
    List<SqlNode> sqlNodes = sqlBasicCall.getOperandList();
    SqlIdentifier sqlIdentifier = null;
    for (SqlNode sqlNode : sqlNodes) {
      if (sqlNode instanceof SqlIdentifier) {
        if (((SqlIdentifier) sqlNode).names.size() == 1) {
          sqlIdentifier = (SqlIdentifier) sqlNode;
        }
      }
    }
    return sqlIdentifier;
  }

  /**
   * Updates the SELECT list in the subQuery if the columns used in the GROUP BY clause
   * are identified in the subQuery.
   * <p>
   * This method extracts the alias of the subQuery, identifies the column names used
   * from the subQuery in the GROUP BY clause, creates a list of SqlIdentifier for the
   * identified column names, and then updates the SELECT list in the subQuery accordingly.
   *
   * @param builder     The builder instance containing the current SqlNode of the query.
   * @param groupByList The list of SqlNode representing the GROUP BY clause.
   * @see #getLeftBasicCall(SqlImplementor.Builder)
   * @see #extractSubQueryAlias(SqlBasicCall)
   * @see #extractColumnNamesUsedFromSubQuery(List, String)
   * @see #createSqlIdentifiers(List)
   * @see #updateSelectListInSubQuery(SqlBasicCall, List)
   */
  protected void updateSelectIfColumnIsUsedInGroupBy(
      SqlImplementor.Builder builder, List<SqlNode> groupByList) {
    SqlBasicCall sqlBasicCall = getLeftBasicCall(builder);

    // Extract the alias of the subQuery
    String subQueryAlias = extractSubQueryAlias(sqlBasicCall);

    // Identify column names used from the subQuery in the GROUP BY clause
    List<String> columnNamesUsedFromSubQuery =
        extractColumnNamesUsedFromSubQuery(groupByList, subQueryAlias);

    // Create a list of SqlIdentifier for the identified column names
    List<SqlIdentifier> sqlIdentifiersNew = createSqlIdentifiers(columnNamesUsedFromSubQuery);

    // Update the SELECT list in the subQuery
    updateSelectListInSubQuery(sqlBasicCall, sqlIdentifiersNew);
  }

  // Function to check if the FROM clause is a join with a basic call
  protected boolean isJoinWithBasicCall(SqlImplementor.Builder builder) {
    return builder.select.getFrom() instanceof SqlJoin
        && ((SqlJoin) builder.select.getFrom()).getLeft() instanceof SqlBasicCall;
  }

  // Function to get the left operand if it is a basic call
  private SqlBasicCall getLeftBasicCall(SqlImplementor.Builder builder) {
    return (SqlBasicCall) ((SqlJoin) builder.select.getFrom()).getLeft();
  }

  // Function to extract the alias of the subQuery
  private String extractSubQueryAlias(SqlBasicCall sqlBasicCall) {
    String subQueryAlias = null;
    for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
      if (sqlNode instanceof SqlIdentifier) {
        subQueryAlias = ((SqlIdentifier) sqlNode).names.get(0);
      }
    }
    return subQueryAlias;
  }

  // Function to extract column names used from the subQuery in the GROUP BY clause
  private List<String> extractColumnNamesUsedFromSubQuery(
      List<SqlNode> groupByList, String subQueryAlias) {
    List<String> columnNamesUsedFromSubQuery = new ArrayList<>();
    for (SqlNode groupByNode : groupByList) {
      if (groupByNode instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) groupByNode;
        if (identifier.names.get(0).equals(subQueryAlias) && identifier.names.size() == 2) {
          columnNamesUsedFromSubQuery.add(((SqlIdentifier) groupByNode).names.get(1));
        }
      }
    }
    return columnNamesUsedFromSubQuery;
  }

  // Function to create a list of SqlIdentifier for the identified column names
  private List<SqlIdentifier> createSqlIdentifiers(List<String> columnNamesUsedFromSubQuery) {
    List<SqlIdentifier> sqlIdentifierList = new ArrayList<>();
    for (String col : columnNamesUsedFromSubQuery) {
      sqlIdentifierList.add(new SqlIdentifier(ImmutableList.of(col), POS));
    }
    return sqlIdentifierList;
  }

  // Function to update the SELECT list in the subQuery
  private void updateSelectListInSubQuery(
      SqlBasicCall sqlBasicCall, List<SqlIdentifier> sqlIdentifiersNew) {
    for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
      if (sqlNode instanceof SqlSelect) {
        updateSelectListInSelectNode((SqlSelect) sqlNode, sqlIdentifiersNew);
      }
    }
  }

  private void updateSelectListInSelectNode(
      SqlSelect sqlSelect, List<SqlIdentifier> sqlIdentifiersNew) {
    SqlNodeList sqlNodeList = sqlSelect.getSelectList();
    if (sqlNodeList != null) {
      List<String> columnNamesInSelect = getColumnNamesInSelect(sqlNodeList);

      if (sqlIdentifiersNew.size() > sqlNodeList.size()) {
        for (SqlIdentifier sqlIdentifier : sqlIdentifiersNew) {
          if (!columnNamesInSelect.contains(sqlIdentifier.names.get(0))) {
            sqlNodeList.add(createNewSelectItem(sqlIdentifier, sqlSelect));
          }
        }

        sqlSelect.setSelectList(new SqlNodeList(sqlNodeList, POS));
      }
    } else if (sqlIdentifiersNew.size() > 0) {
      updateSelectListForJoin(sqlSelect, sqlIdentifiersNew);
    }
  }

  private List<String> getColumnNamesInSelect(SqlNodeList sqlNodeList) {
    List<String> columnNamesInSelect = new ArrayList<>();

    for (SqlNode selectNode : sqlNodeList) {
      if (selectNode instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) selectNode;
        columnNamesInSelect.add(identifier.names.size() > 1 ? identifier.names.reverse().get(0)
            : identifier.names.get(0));
      }
    }
    return columnNamesInSelect;
  }

  private void updateSelectListForJoin(SqlSelect sqlSelect, List<SqlIdentifier> sqlIdentifiersNew) {
    if (sqlSelect.getFrom() instanceof SqlJoin) {
      SqlNodeList sqlNodes = new SqlNodeList(POS);

      for (SqlIdentifier sqlIdentifier : sqlIdentifiersNew) {
        sqlNodes.add(createNewSelectItem(sqlIdentifier, sqlSelect));
      }

      sqlSelect.setSelectList(new SqlNodeList(sqlNodes, POS));
    }
  }

  // Function to create a new SqlSelectItem with an alias if it ends with a digit
  private SqlNode createNewSelectItem(SqlIdentifier sqlIdentifier, SqlNode sqlNode) {
    boolean endsWithDigit = endsWithDigit(sqlIdentifier.names.get(0));
    ImmutableList<String> operands = getOperandNames(sqlNode, endsWithDigit);
    if (endsWithDigit) {
      return createAsSqlNodeWithAlias(operands.get(0), sqlIdentifier.names.get(0));
    } else {
      return new SqlIdentifier(ImmutableList.of(operands.get(0), sqlIdentifier.names.get(0)),
          SqlParserPos.ZERO);
    }
  }

  private ImmutableList<String> getOperandNames(SqlNode sqlNode, boolean endsWithDigit) {
    List<SqlNode> operands = getOperandsFromJoin(sqlNode, endsWithDigit);
    return ((SqlIdentifier) operands.get(1)).names; // Need to check index
  }

  private List<SqlNode> getOperandsFromJoin(SqlNode sqlNode, boolean endsWithDigit) {
    SqlJoin joinNode = (SqlJoin) ((SqlSelect) sqlNode).getFrom();
    SqlBasicCall basicCall =
        (SqlBasicCall) (endsWithDigit ? joinNode.getRight()
            : joinNode.getLeft());
    return basicCall.getOperandList();
  }

  private SqlNode createAsSqlNodeWithAlias(String alias, String columnName) {
    return SqlStdOperatorTable.AS.createCall(POS,
        ImmutableList.of(
            new SqlIdentifier(
                ImmutableList.of(
                    alias, columnName.replaceAll("\\d+$",
                        "")), SqlParserPos.ZERO),
            new SqlIdentifier(columnName, SqlParserPos.ZERO)));
  }


  private static void populateTableInfo(RelNode relNode, List<TableInfo> tableInfoList) {
    if (relNode instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) relNode;
      populateTableInfo(join.getLeft(), tableInfoList);
      populateTableInfo(join.getRight(), tableInfoList);
    } else if (relNode instanceof TableScan) {
      TableScan tableScan = (TableScan) relNode;
      List<String> tableName = tableScan.getTable().getQualifiedName();
      RelDataType rowType = tableScan.getRowType();
      tableInfoList.add(new TableInfo(tableName, rowType));
    } else {
      relNode.getInputs().forEach(input -> populateTableInfo(input, tableInfoList));
    }
  }

  /**
   * TableInfo class holds the qualified tableName and its rowtype information.
   */
  private static class TableInfo {
    List<String> tableName;
    RelDataType rowType;

    TableInfo(List<String> tableName, RelDataType rowType) {
      this.tableName = tableName;
      this.rowType = rowType;
    }

    boolean columnExists(String columnName) {
      List<RelDataTypeField> fields = rowType.getFieldList();
      for (RelDataTypeField field : fields) {
        if (field.getName().equalsIgnoreCase(columnName)) {
          return true;
        }
      }
      return false;
    }
  }
}
