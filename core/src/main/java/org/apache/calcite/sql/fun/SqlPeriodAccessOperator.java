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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;

import java.util.Objects;

/**
 * SqlPeriodAccessOperator is used to access BEGIN/END types from a PERIOD type.
 */
public class SqlPeriodAccessOperator extends SqlFunction {

  private final boolean begin;

  public SqlPeriodAccessOperator(String name, boolean begin) {
    super(name,
        begin ? SqlKind.PERIOD_BEGIN : SqlKind.PERIOD_END,
        null,
        null,
        OperandTypes.PERIOD, SqlFunctionCategory.SYSTEM);
    this.begin = begin;
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    RelDataType periodType = opBinding.getOperandType(0);
    String fieldName = begin ? "_begin" : "_end";
    RelDataTypeField accessedField =
        periodType.getField(fieldName, true, false);
    RelDataType componentType = Objects.requireNonNull(accessedField, fieldName).getType();
    return opBinding.getTypeFactory().createTypeWithNullability(componentType, true);
  }
}
