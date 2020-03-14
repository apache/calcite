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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameter type-checking strategy for Explicit Type.
 */
public class ExplicitOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Methods ----------------------------------------------------------------

  private RelDataType type;

  public ExplicitOperandTypeChecker(RelDataType type) {
    this.type = type;
  }

  public boolean isOptional(int i) {
    return false;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    List<SqlTypeFamily> families = new ArrayList<>();

    List<RelDataTypeField> fieldList = type.getFieldList();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
      if (sqlTypeName == SqlTypeName.ROW) {
        if (field.getType().equals(callBinding.getOperandType(i))) {
          families.add(SqlTypeFamily.ANY);
        }
      } else {
        families.add(field.getType().getSqlTypeName().getFamily());
      }
    }
    return OperandTypes.family(families).checkOperandTypes(callBinding, throwOnFailure);
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(type.getFieldCount());
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<TYPE> " + opName + " <TYPE>";
  }

  public Consistency getConsistency() {
    return Consistency.NONE;
  }
}
