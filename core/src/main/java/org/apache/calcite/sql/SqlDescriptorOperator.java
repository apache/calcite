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
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * DESCRIPTOR appears as an argument in a function. DESCRIPTOR accepts a list of
 * identifiers that represent a list of names. The interpretation of names is left
 * to the function.
 *
 * <p>A typical syntax is DESCRIPTOR(col_name, ...).
 *
 * <p>An example is a table-valued function that takes names of columns to filter on.
 */
public class SqlDescriptorOperator extends SqlOperator {
  public SqlDescriptorOperator() {
    super("DESCRIPTOR",
        SqlKind.DESCRIPTOR,
        100,
        100,
        opBinding -> opBinding.typeFactory.createSqlType(SqlTypeName.COLUMN_LIST),
        null,
        null);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // A DESCRIPTOR is a list of column names, so every operand must be an
    // identifier. deriveType does not call validateOperands (the operands are
    // names interpreted by the enclosing function, not expressions to be typed),
    // so the operand check is done here. Functions such as TUMBLE, HOP and
    // SESSION apply the stricter rule that each name is a simple identifier that
    // resolves to a column, before the names are qualified by the validator.
    checkOperandTypes(new SqlCallBinding(validator, scope, call), true);
    return validator.getTypeFactory().createSqlType(SqlTypeName.COLUMN_LIST);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    for (SqlNode operand : callBinding.getCall().getOperandList()) {
      if (!(operand instanceof SqlIdentifier)) {
        if (throwOnFailure) {
          throw SqlUtil.newContextException(operand.getParserPosition(),
              RESOURCE.descriptorMustBeIdentifier());
        }
        return false;
      }
    }
    return true;
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.from(1);
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }
}
