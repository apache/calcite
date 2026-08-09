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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class GenericTableFunction extends SqlFunction implements SqlTableFunction {
  private final RelDataType rowType;

  public GenericTableFunction(String name, RelDataType rowType) {
    super(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(rowType),
        null,
        OperandTypes.SAME_VARIADIC,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);

    this.rowType = rowType;
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return new SqlReturnTypeInference() {
      @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return rowType;
      }
    };
  }

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
