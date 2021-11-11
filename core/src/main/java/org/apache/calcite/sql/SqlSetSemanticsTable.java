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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.List;

/**
 * Parse tree node that represents input table with set semantics of Table Function.
 */
public class SqlSetSemanticsTable extends SqlBasicCall {
  static final SqlSpecialOperator OPERATOR = new Operator(SqlKind.SET_SEMANTICS_TABLE);


  //~ Constructors -----------------------------------------------------------

  public SqlSetSemanticsTable(
      SqlParserPos pos,
      SqlNode queryOrTableRef,
      SqlNodeList partitionList,
      SqlNodeList orderList) {
    super(
        OPERATOR,
        Arrays.asList(queryOrTableRef, partitionList, orderList),
        pos);
  }

  /**
   * Sql partition-by operator.
   */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlSetSemanticsTable sqlTableFunctionTableParam = (SqlSetSemanticsTable) call;
      sqlTableFunctionTableParam.operand(0).unparse(writer, 0, 0);
      final SqlWriter.Frame mrFrame = writer.startFunCall("TABLE");

      SqlNodeList partitionList = sqlTableFunctionTableParam.operand(1);
      if (partitionList.size() > 0) {
        writer.newlineAndIndent();
        writer.sep("PARTITION BY");
        final SqlWriter.Frame partitionFrame = writer.startList("", "");
        partitionList.unparse(writer, 0, 0);
        writer.endList(partitionFrame);
      }
      SqlNodeList orderList = sqlTableFunctionTableParam.operand(2);
      if (orderList.size() > 0) {
        writer.newlineAndIndent();
        writer.sep("ORDER BY");
        writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA, orderList);
      }
      writer.endList(mrFrame);
    }

    @Override public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
      final List<SqlNode> operands = call.getOperandList();
      assert operands.size() == 3;
      RelDataType tableType = validator.deriveType(scope, operands.get(0));
      assert tableType != null;
      return tableType;
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return ordinal != 0;
    }
  }
}
