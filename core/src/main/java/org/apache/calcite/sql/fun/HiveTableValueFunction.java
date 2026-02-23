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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * class For Hive Table Value Function.
 */
public class HiveTableValueFunction extends SqlFunction implements SqlTableFunction {
  private final Map<String, RelDataType> columnDefinitions;
  private static final List<String> FUNCTION_NAME =
      ImmutableList.of("INLINE",
      "EXPLODE",
      "POSEXPLODE");

  private HiveTableValueFunction(String functionName, Map<String, RelDataType> columnDefinitions) {
    super(functionName,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.COLUMN_LIST,
        null,
        OperandTypes.ARRAY,
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

  public Map<String, RelDataType> getColumnDefinitions() {
    return columnDefinitions;
  }

  public static HiveTableValueFunction of(
      String functionName,
      Map<String, RelDataType> columnDefinitions) {

    if (functionName == null || functionName.trim().isEmpty()) {
      throw new IllegalArgumentException("functionName must not be null or empty");
    }
    if (!FUNCTION_NAME.contains(functionName.toUpperCase())) {
      throw new IllegalArgumentException(
          "Table Value Function " + functionName + " is not supported");
    }
    return new HiveTableValueFunction(functionName, columnDefinitions);
  }
}
