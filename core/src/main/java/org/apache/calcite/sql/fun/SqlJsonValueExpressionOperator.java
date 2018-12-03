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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The JSON value expression operator that indicates that the value expression
 * should be parsed as JSON.
 */
public class SqlJsonValueExpressionOperator extends SqlSpecialOperator {
  private final boolean structured;

  public SqlJsonValueExpressionOperator(String name, boolean structured) {
    super(name, SqlKind.JSON_VALUE_EXPRESSION, 100, true,
        opBinding -> {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          return typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.ANY), true);
        },
        (callBinding, returnType, operandTypes) -> {
          if (callBinding.isOperandNull(0, false)) {
            final RelDataTypeFactory typeFactory =
                callBinding.getTypeFactory();
            operandTypes[0] = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true);
          }
        },
        structured ? OperandTypes.ANY : OperandTypes.STRING);
    this.structured = structured;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    call.operand(0).unparse(writer, 0, 0);
    if (!structured) {
      writer.keyword("FORMAT JSON");
    }
  }
}

// End SqlJsonValueExpressionOperator.java
