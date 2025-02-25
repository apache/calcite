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

import org.apache.calcite.plan.PivotRelTrait;
import org.apache.calcite.plan.PivotRelTraitDef;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
   * Builds SqlPivotNode for Aggregate RelNode.
   *
   * @param e                The aggregate node with pivot relTrait flag
   * @param builder          The SQL builder
   * @param selectColumnList selectNodeList from Project node
   * @return Result with sqlPivotNode wrap in it.
   */
  public SqlNode buildSqlPivotNode(
      Aggregate e, SqlImplementor.Builder builder, List<SqlNode> selectColumnList,
      List<SqlNode> aggregateInClauseFieldList) {
    //create query parameter
    Optional<RelTrait> pivotRelTrait =
        Optional.ofNullable(e.getTraitSet().getTrait(PivotRelTraitDef.instance));
    boolean hasSubquery = false;
    if (pivotRelTrait.isPresent()) {
      hasSubquery = ((PivotRelTrait) pivotRelTrait.get()).hasSubquery();
      pivotTableAlias = ((PivotRelTrait) pivotRelTrait.get()).getPivotAlias();
    }
    SqlNode query;
    if (hasSubquery) {
      query = builder.select;
    } else {
      query = builder.select.getFrom();
    }

    //create axisList parameter
    SqlNodeList axisNodeList = getAxisNodeList(selectColumnList, hasSubquery);

    //create pivotAggregateColumnList parameter
    SqlNodeList pivotAggregateColumnList = getAggregateColumnNode(e);

    //create inValues List parameter
    SqlNodeList pivotInClauseValueNodes =
        getPivotInClauseValueNodes(selectColumnList, aggregateInClauseFieldList);

    //create Pivot Node
    return wrapSqlPivotInSqlSelectSqlNode(
        builder, query, pivotAggregateColumnList, axisNodeList, pivotInClauseValueNodes);
  }

  private SqlNode wrapSqlPivotInSqlSelectSqlNode(
      SqlImplementor.Builder builder, SqlNode query, SqlNodeList pivotAggregateColumnList,
      SqlNodeList axisNodeList, SqlNodeList pivotInClauseValueNodes) {
    SqlPivot sqlPivot =
        new SqlPivot(pos, query, pivotAggregateColumnList, axisNodeList, pivotInClauseValueNodes);
    SqlNode sqlTableAlias = sqlPivot;
    if (pivotTableAlias != null && !(builder.context.dialect instanceof SparkSqlDialect)) {
      sqlTableAlias =
          SqlStdOperatorTable.AS.createCall(pos, sqlPivot,
              new SqlIdentifier(pivotTableAlias, pos));
    }
    SqlNode select =
        new SqlSelect(SqlParserPos.ZERO, null, null, sqlTableAlias,
            builder.select.getWhere(), null,
            builder.select.getHaving(), null, builder.select.getOrderList(),
            null, null, SqlNodeList.EMPTY);
    return select;
  }

  private SqlNodeList getPivotInClauseValueNodes(List<SqlNode> selectNodeList,
      List<SqlNode> aggregateInClauseFieldList) {
    SqlNodeList inColumnList = new SqlNodeList(pos);

    if (aggregateInClauseFieldList.isEmpty()) {
      selectNodeList.stream()
          .filter(x -> !(x instanceof SqlIdentifier))
          .map(node -> {
            // Extract the specific node as per your expression
            SqlNode secondOperand = ((SqlBasicCall)
                ((SqlCase)
                    ((SqlBasicCall)
                        ((SqlBasicCall) node)
                            .getOperandList().get(0))
                        .operand(0))
                    .getWhenOperands().get(0))
                .operand(1);

            if (secondOperand.getKind() == SqlKind.AS
                && ((SqlBasicCall) secondOperand).operand(1) instanceof SqlCharStringLiteral) {
              return SqlStdOperatorTable.AS.createCall(pos,
                  ((SqlBasicCall) secondOperand).operand(0),
                  new SqlIdentifier(((SqlBasicCall) secondOperand).operand(1).toString().replaceAll("'", ""), pos));
            }

            return secondOperand;
          })
          .forEach(inColumnList::add);
    }

    inColumnList.addAll(modifyInClauseAliases(aggregateInClauseFieldList));

    return inColumnList;
  }

  private List<SqlNode> modifyInClauseAliases(List<SqlNode> aggregateInClauseFieldList) {
    return aggregateInClauseFieldList.stream().map(this::modifyAlias).collect(Collectors.toList());
  }

  private SqlNode modifyAlias(SqlNode sqlNode) {
    if (sqlNode instanceof SqlBasicCall) {
      SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
      if (sqlBasicCall.getOperator() == SqlStdOperatorTable.AS) {
        return SqlStdOperatorTable.AS.createCall(pos, sqlBasicCall.operand(0),
            new SqlIdentifier(sqlBasicCall.operand(1).toString().replaceAll("'", ""), pos));
      }
    }
    return sqlNode;
  }

  private SqlNodeList getAggregateColumnNode(Aggregate e) {
    Set<SqlNode> aggArgList = new HashSet<>();
    Set<String> columnName = new HashSet<>();
    for (int i = 0; i < e.getAggCallList().size(); i++) {
      columnName.add(
          e.getInput().getRowType().getFieldList().get(
                  e.getAggCallList().get(i).getArgList().get(0))
              .getKey());
    }
    SqlNode tempNode = new SqlIdentifier(new ArrayList<>(columnName).get(0), pos);
    SqlNode aggFunctionNode =
        e.getAggCallList().get(0).getAggregation().createCall(pos, tempNode);
    aggArgList.add(aggFunctionNode);
    return new SqlNodeList(aggArgList, pos);
  }

  private SqlNodeList getAxisNodeList(List<SqlNode> selectColumnList, boolean hasSubquery) {

    final Set<SqlNode> modifiedAxisNodeList = new HashSet<>();

    SqlBasicCall pivotColumnAggregation =
        (SqlBasicCall) selectColumnList.get(selectColumnList.size() - 1);

    if (!hasSubquery) {
      SqlCase pivotColumnAggregationCaseCall =
          (SqlCase) (
              (SqlBasicCall) ((SqlBasicCall) selectColumnList
                  .get(selectColumnList.size() - 1))
                  .getOperandList().get(0)).getOperandList().get(0);
      SqlBasicCall caseConditionCall =
          (SqlBasicCall) pivotColumnAggregationCaseCall.getWhenOperands().get(0);
      SqlIdentifier aggregateCol = caseConditionCall.operand(0);
      modifiedAxisNodeList.add(aggregateCol);
      return new SqlNodeList(modifiedAxisNodeList, pos);
    }

    SqlBasicCall axisNodeList =
        ((SqlBasicCall) pivotColumnAggregation.getOperandList().get(0)).operand(0);

    axisNodeList = axisNodeList.getOperator().kind == SqlKind.EQUALS
        ? (SqlBasicCall) axisNodeList.getOperandList().get(0)
        : axisNodeList;

    if (axisNodeList.getOperator().kind == SqlKind.AS) {
      if (!(axisNodeList.operand(1) instanceof SqlIdentifier)) {
        modifiedAxisNodeList.add(
            new SqlIdentifier(
                axisNodeList.operand(1).toString().replaceAll("'", ""),
                SqlParserPos.QUOTED_ZERO));
      } else {
        modifiedAxisNodeList.add(axisNodeList);
      }
    } else {
      modifiedAxisNodeList.add(new SqlIdentifier(axisNodeList.toString(), SqlParserPos.QUOTED_ZERO));
    }
    return new SqlNodeList(modifiedAxisNodeList, pos);
  }
}
