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
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.List;

/**
 * class for MSSQL_STRING_SPLIT tableFunction".
 */
public class SqlServerStringSplitFunction extends SqlFunction implements SqlTableFunction {
  public SqlServerStringSplitFunction() {
    super("MSSQL_STRING_SPLIT", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, null,
        OperandTypes.or(OperandTypes.STRING_STRING, OperandTypes.STRING_STRING_INTEGER),
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::getRowType;
  }

  private RelDataType getRowType(SqlOperatorBinding opBinding) {
    RelDataType componentType = inferReturnType(opBinding);
    RelDataTypeFactory.Builder builder = opBinding.getTypeFactory().builder();
    List<RexNode> operands = ((RexCallBinding) opBinding).operands();

    if (operands.size() == 2) {
      builder.add("VALUE", componentType);
    } else if (operands.size() == 3 && BigDecimal.ONE.equals(
        ((RexLiteral) operands.get(2)).getValue())) {
      builder.add("VALUE", componentType).add("ORDINAL", SqlTypeName.BIGINT);
    }
    return builder.build();
  }
}
