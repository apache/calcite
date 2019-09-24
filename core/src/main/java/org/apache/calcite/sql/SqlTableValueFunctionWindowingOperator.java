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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Base class for table-value function windowing operator (TUMBLE, HOP and SESSION).
 * With this class, we are able to re-write argumentMustBeScalar(int) because the first
 * parameter of table-value function windowing is TABLE parameter.
 */
public class SqlTableValueFunctionWindowingOperator extends SqlFunction {

  public SqlTableValueFunctionWindowingOperator(String name, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      assert nodeType != null;
    }

    RelDataType type = call.getOperator().validateOperands(validator, scope, call);

    // Validate and determine coercibility and resulting collation
    // name of binary operator if needed.
    type = adjustType(validator, call, type);
    SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
    return type;
  }

  /**
   * The first parameter of table-value function windowing is a TABLE parameter,
   * which is not scalar.
   */
  public boolean argumentMustBeScalar(int ordinal) {
    if (ordinal == 0) {
      return false;
    }
    return true;
  }
}

// End SqlTableValueFunctionWindowingOperator.java
