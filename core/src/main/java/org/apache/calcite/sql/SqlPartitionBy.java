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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Static;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Parse tree node that represents an {@code PARTITION BY} clause on input of Window Table Function.
 */
public class SqlPartitionBy extends SqlCall {
  static final SqlSpecialOperator OPERATOR = new Operator(SqlKind.OTHER_FUNCTION);

  public SqlNode tableRef;
  public final SqlNodeList partitionList;

  //~ Constructors -----------------------------------------------------------

  public SqlPartitionBy(
      SqlParserPos pos,
      SqlNode tableRef,
      SqlNodeList partitionList) {
    super(pos);
    this.tableRef = tableRef;
    this.partitionList = partitionList;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.OTHER_FUNCTION;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableRef, partitionList);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      tableRef = operand;
      break;
    default:
      super.setOperand(i, operand);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    tableRef.unparse(writer, leftPrec, 0);
    writer.keyword("PARTITION BY");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    // force parentheses if there is more than one axis
    final int leftPrec1 = partitionList.size() > 1 ? 1 : 0;
    partitionList.unparse(writer, leftPrec1, 0);
    writer.endList(frame);
  }

  /** Sql partition-by operator. */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }

    @Override public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
      final List<SqlNode> operands = call.getOperandList();
      assert operands.size() == 2;
      final SqlNode table = operands.get(0);
      final SqlNode partitionKeys = operands.get(1);
      RelDataType tableType = validator.deriveType(scope, table);
      assert tableType != null;
      SqlNodeList nodeList = (SqlNodeList) partitionKeys;
      for (int i = 0; i < nodeList.size(); i++) {
        SqlNode node = nodeList.get(i);
        final String partitionKeyName = node.toString();
        final RelDataTypeField partitionKeyType =
            tableType.getField(partitionKeyName, false, false);
        if (partitionKeyType == null) {
          throw SqlUtil.newContextException(node.getParserPosition(),
              Static.RESOURCE.unknownField(partitionKeyName));
        }
      }
      return tableType;
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return false;
    }
  }
}
