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

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class to identify Rel structure which is of UNPIVOT Type.
 */

public class PivotRelToSqlUtil {
  SqlParserPos pos;
  String pivotTableAlias = "";

  PivotRelToSqlUtil(SqlParserPos pos) {
    this.pos = pos;
  }
  /**
   *  Builds SqlPivotNode for Aggregate RelNode.
   *
   * @param e The aggregate node with pivot relTrait flag
   * @param builder The SQL builder
   * @param selectColumnList  selectNodeList from Project node
   * @return  Result with sqlPivotNode wrap in it.
   */
  public SqlNode buildSqlPivotNode(
      Aggregate e, SqlImplementor.Builder builder, List<SqlNode> selectColumnList) {
    //create query parameter
    SqlNode query = ((SqlSelect) builder.select).getFrom();

    //create aggList parameter
    SqlNodeList aggList = getAggSqlNodes(e, selectColumnList);


    //create axisList parameter
    SqlNodeList axesNodeList = getAxisSqlNodes(e);

    //create inValues List parameter
    SqlNodeList inColumnList = getInValueNodes(e);

    //create Pivot Node
    return wrapSqlPivotInSqlSelectSqlNode(
        builder, query, aggList, axesNodeList, inColumnList);
  }

  private SqlNode wrapSqlPivotInSqlSelectSqlNode(
      SqlImplementor.Builder builder, SqlNode query, SqlNodeList aggList,
      SqlNodeList axesNodeList, SqlNodeList inColumnList) {
    SqlPivot sqlPivot = new SqlPivot(pos, query, axesNodeList, aggList, inColumnList);
    SqlNode sqlTableAlias = sqlPivot;
    if (pivotTableAlias.length() > 0) {
      sqlTableAlias = SqlStdOperatorTable.AS.createCall(
          pos, sqlPivot,
          new SqlIdentifier(pivotTableAlias, pos));
    }
    SqlNode select = new SqlSelect(
        SqlParserPos.ZERO, null, null, sqlTableAlias,
        builder.select.getWhere(), null,
        builder.select.getHaving(), null, builder.select.getOrderList(),
        null, null, SqlNodeList.EMPTY
    );
    return select;
  }

  private SqlNodeList getInValueNodes(Aggregate e) {
    SqlNodeList inColumnList = new SqlNodeList(pos);
    for (AggregateCall aggCall : e.getAggCallList()) {
      String columnName1 = e.getRowType().getFieldList().get(aggCall.filterArg).getKey();
      String[] inValues = columnName1.split("'");
      String tableInValueAliases = inValues[2];
      if (tableInValueAliases.contains("null")) {
        tableInValueAliases = tableInValueAliases.replace("_null", "")
            .replace("-null", "")
            .replace("'", "");
      }
      String[] columnNameAndAlias = tableInValueAliases.split("-");
      SqlNode inListColumnNode;
      if (columnNameAndAlias.length == 1) {
        inListColumnNode = SqlLiteral.createCharString(inValues[1], pos);
      } else {
        pivotTableAlias = columnNameAndAlias[1];
        if (columnNameAndAlias.length == 2) {
          inListColumnNode = SqlLiteral.createCharString(inValues[1], pos);
        } else {
          inListColumnNode = SqlStdOperatorTable.AS.createCall(
              pos, SqlLiteral.createCharString(
                  inValues[1], pos),
              new SqlIdentifier(columnNameAndAlias[2], pos));
        }
      }
      inColumnList.add(inListColumnNode);
    }
    return inColumnList;
  }

  private SqlNodeList getAxisSqlNodes(Aggregate e) {
    Set<SqlNode> aggArgList = new HashSet<>();
    Set<String> columnName = new HashSet<>();
    for (int i = 0; i < e.getAggCallList().size(); i++) {
      columnName.add(
          e.getRowType().getFieldList().get(
              e.getAggCallList().get(i).getArgList().get(0)
          ).getKey());
    }
    SqlNode tempNode = new SqlIdentifier(new ArrayList<>(columnName).get(0), pos);
    SqlNode aggFunctionNode =
        e.getAggCallList().get(0).getAggregation().createCall(pos, tempNode);
    aggArgList.add(aggFunctionNode);
    SqlNodeList axesNodeList = new SqlNodeList(aggArgList, pos);
    return axesNodeList;
  }

  private SqlNodeList getAggSqlNodes(Aggregate e, List<SqlNode> selectColumnList) {
    final Set<SqlNode> selectList = new HashSet<>();
    for (int i = 0; i < e.getAggCallList().size(); i++) {
      int fieldIndex = e.getAggCallList().get(i).filterArg - (i + 2);
      if (fieldIndex < 0) {
        continue;
      }
      SqlNode aggCallSqlNode = selectColumnList.get(fieldIndex);
      selectList.add(aggCallSqlNode);

    }
    SqlNodeList aggList = new SqlNodeList(selectList, pos);
    return aggList;
  }
}
