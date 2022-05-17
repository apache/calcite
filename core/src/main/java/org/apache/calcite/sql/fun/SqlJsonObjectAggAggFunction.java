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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

import java.util.Objects;

/**
 * The <code>JSON_OBJECTAGG</code> aggregate function.
 */
public class SqlJsonObjectAggAggFunction extends SqlAggFunction {
  private final SqlJsonConstructorNullClause nullClause;

  /** Creates a SqlJsonObjectAggAggFunction. */
  public SqlJsonObjectAggAggFunction(SqlKind kind,
      SqlJsonConstructorNullClause nullClause) {
    super(kind + "_" + nullClause.name(), null, kind, ReturnTypes.VARCHAR_2000,
        (callBinding, returnType, operandTypes) -> {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          operandTypes[0] = typeFactory.createSqlType(SqlTypeName.VARCHAR);
          operandTypes[1] = typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.ANY), true);
        }, OperandTypes.family(SqlTypeFamily.CHARACTER,
            SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM, false, false, Optionality.FORBIDDEN);
    this.nullClause = Objects.requireNonNull(nullClause, "nullClause");
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame = writer.startFunCall("JSON_OBJECTAGG");
    writer.keyword("KEY");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.keyword("VALUE");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.keyword(nullClause.sql);
    writer.endFunCall(frame);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // To prevent operator rewriting by SqlFunction#deriveType.
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      validator.setValidatedNodeType(operand, nodeType);
    }
    return validateOperands(validator, scope, call);
  }

  public SqlJsonObjectAggAggFunction with(SqlJsonConstructorNullClause nullClause) {
    return this.nullClause == nullClause ? this
        : new SqlJsonObjectAggAggFunction(getKind(), nullClause);
  }

  public SqlJsonConstructorNullClause getNullClause() {
    return nullClause;
  }
}
