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
package org.apache.calcite.piglet;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

/**
 * An extension of {@link RelToSqlConverter} to convert a relation algebra tree,
 * translated from a Pig script, into a SQL statement.
 *
 * <p>The input relational algebra tree can be optimized by the planner for Pig
 * to {@link RelNode}.
 */
public class PigRelToSqlConverter extends RelToSqlConverter {

  /** Creates a RelToSqlConverter.
   *
   * @param dialect SQL dialect
   */
  PigRelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  @Override public Result visit(Aggregate e) {
    final Result x = visitChild(0, e.getInput());
    final boolean isProjectOutput = e.getInput() instanceof Project
        || (e.getInput() instanceof EnumerableInterpreter
            && ((EnumerableInterpreter) e.getInput()).getInput()
                instanceof Project);
    final Builder builder = getAggregateBuilder(e, x, isProjectOutput);

    final List<SqlNode> groupByList = Expressions.list();
    final List<SqlNode> selectList = new ArrayList<>();
    buildAggGroupList(e, builder, groupByList, selectList);

    final int groupSetSize = e.getGroupSets().size();
    SqlNodeList groupBy = new SqlNodeList(groupByList, POS);
    if (groupSetSize > 1) {
      // If there are multiple group sets, this should be a result of converting a
      // Pig CUBE/cube or Pig CUBE/rollup
      final List<SqlNode> cubeRollupList = Expressions.list();
      if (groupSetSize == groupByList.size() + 1) {
        cubeRollupList.add(SqlStdOperatorTable.ROLLUP.createCall(groupBy));
      } else {
        assert groupSetSize == Math.round(Math.pow(2, groupByList.size()));
        cubeRollupList.add(SqlStdOperatorTable.CUBE.createCall(groupBy));
      }
      groupBy = new SqlNodeList(cubeRollupList, POS);
    }

    return buildAggregate(e, builder, selectList, groupBy.getList());
  }

  /** @see #dispatch */
  public Result visit(Window e) {
    final Result x = visitChild(0, e.getInput());
    final Builder builder = x.builder(e, Clause.SELECT);
    final List<SqlNode> selectList =
        new ArrayList<>(builder.context.fieldList());

    for (Window.Group winGroup : e.groups) {
      final List<SqlNode> partitionList = Expressions.list();
      for (int i : winGroup.keys) {
        partitionList.add(builder.context.field(i));
      }

      final List<SqlNode> orderList = Expressions.list();
      for (RelFieldCollation orderKey : winGroup.collation().getFieldCollations()) {
        orderList.add(builder.context.toSql(orderKey));
      }

      final SqlNode sqlWindow =  SqlWindow.create(
          null, // Window declaration name
          null, // Window reference name
          new SqlNodeList(partitionList, POS),
          new SqlNodeList(orderList, POS),
          SqlLiteral.createBoolean(winGroup.isRows, POS),
          builder.context.toSql(winGroup.lowerBound),
          builder.context.toSql(winGroup.upperBound),
          null, // allowPartial
          POS);

      for (Window.RexWinAggCall winFunc : winGroup.aggCalls) {
        final List<SqlNode> winFuncOperands = Expressions.list();
        for (RexNode operand : winFunc.getOperands()) {
          winFuncOperands.add(builder.context.toSql(null, operand));
        }
        SqlNode aggFunc = winFunc.getOperator().createCall(new SqlNodeList(winFuncOperands, POS));
        selectList.add(SqlStdOperatorTable.OVER.createCall(POS, aggFunc, sqlWindow));
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }
    return builder.result();
  }
}

// End PigRelToSqlConverter.java
