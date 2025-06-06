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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.Map;

/**
 * Teradata s STRTOK_SPLIT_TO_TABLE function.
 */
public class TeradataStrtokSplitToTableFunction extends SqlFunction
    implements SqlTableFunction {

  private final Map<String, RelDataType> columnDefinitions;

  public TeradataStrtokSplitToTableFunction(Map<String, RelDataType> columnDefinitions) {
    super("STRTOK_SPLIT_TO_TABLE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.sequence(
            "'STRTOK_SPLIT_TO_TABLE(<INTEGER_OR_STRING>, <STRING>, <STRING>)'",
            OperandTypes.INTEGER_OR_STRING, OperandTypes.STRING, OperandTypes.STRING),
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    this.columnDefinitions = columnDefinitions;
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::getRowType;
  }

  public RelDataType getRowType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory.Builder builder =
        opBinding.getTypeFactory().builder();
    for (Map.Entry<String, RelDataType> entry : columnDefinitions.entrySet()) {
      builder.add(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.getOperandList().size() == 3;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (SqlNode sqlNode : call.getOperandList()) {
      writer.sep(",");
      sqlNode.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
    final SqlWriter.Frame returnFrame = writer.startFunCall("RETURNS");
    for (Map.Entry<String, RelDataType> entry : columnDefinitions.entrySet()) {
      writer.sep(",");
      writer.keyword(entry.getKey());
      writer.keyword(entry.getValue().getSqlTypeName().getName());
    }
    writer.endFunCall(returnFrame);
  }
}
