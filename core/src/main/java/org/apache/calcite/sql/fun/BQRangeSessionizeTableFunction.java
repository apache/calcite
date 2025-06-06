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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Bigquery's RANGE_SESSIONIZE Table function.
 */
public class BQRangeSessionizeTableFunction extends SqlFunction
    implements SqlTableFunction {

  private final Map<String, RelDataType> columnDefinitions;

  public BQRangeSessionizeTableFunction(Map<String, RelDataType> columnDefinitions) {
    super("RANGE_SESSIONIZE",
        SqlKind.RANGE_SESSIONIZE,
        ReturnTypes.CURSOR,
        null,
        OperandTypes.VARIADIC,
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

  @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
      SqlParserPos pos, @Nullable SqlNode... operands) {
    // As super.createCall, but don't union the positions
    return new SqlBasicCall(this, ImmutableNullableList.copyOf(operands), pos,
        functionQualifier);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (SqlNode sqlNode : call.getOperandList()) {
      writer.sep(",");
      sqlNode.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }
}
