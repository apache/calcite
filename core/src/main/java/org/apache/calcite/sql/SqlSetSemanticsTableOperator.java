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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * SetSemanticsTable appears as an parameter in a table function.
 * It represents as an input table with set semantics.
 * Set semantics means that the outcome of the function depends on how
 * the data is partitioned.
 * When the table function is called from a query, the table parameter can
 * optionally be extended with either a PARTITION BY clause or
 * an ORDER BY clause or both.
 *
 */
public class SqlSetSemanticsTableOperator extends SqlInternalOperator {

  //~ Constructors -----------------------------------------------------------

  public SqlSetSemanticsTableOperator() {
    super("SET_SEMANTICS_TABLE", SqlKind.SET_SEMANTICS_TABLE);
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    assert operands.length == 3;
    SqlNode partitionList = operands[1];
    SqlNode orderList = operands[2];
    assert (partitionList != null && !SqlNodeList.isEmptyList(partitionList))
        || (orderList != null && !SqlNodeList.isEmptyList(orderList));
    return super.createCall(functionQualifier, pos, operands);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    call.operand(0).unparse(writer, 0, 0);

    SqlNodeList partitionList = call.operand(1);
    if (partitionList.size() > 0) {
      writer.sep("PARTITION BY");
      final SqlWriter.Frame partitionFrame = writer.startList("", "");
      partitionList.unparse(writer, 0, 0);
      writer.endList(partitionFrame);
    }
    SqlNodeList orderList = call.operand(2);
    if (orderList.size() > 0) {
      writer.sep("ORDER BY");
      writer.list(
          SqlWriter.FrameTypeEnum.ORDER_BY_LIST,
          SqlWriter.COMMA,
          orderList);
    }
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    RelDataType tableType = validator.deriveType(scope, operands.get(0));
    assert tableType != null;
    return tableType;
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }
}
