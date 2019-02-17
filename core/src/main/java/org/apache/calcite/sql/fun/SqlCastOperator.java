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
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Infix cast operator, "::", as used in PostgreSQL.
 *
 * <p>This operator is not enabled in the default operator table; it is
 * registered for various dialects via {@link SqlLibraryOperators#INFIX_CAST}.
 *
 * <p>The {@link #kind} is {@link SqlKind#CAST}, the same as the built-in
 * {@code CAST} function, {@link SqlCastFunction}. Be sure to use {@code kind}
 * rather than {@code instanceof} if you would like your code to apply to both
 * operators.
 *
 * <p>Unlike {@code SqlCastFunction}, it can only be used for
 * {@link org.apache.calcite.sql.SqlCall SqlCall};
 * {@link org.apache.calcite.rex.RexCall RexCall} must use
 * {@code SqlCastFunction}.
 */
class SqlCastOperator extends SqlBinaryOperator {
  SqlCastOperator() {
    super("::", SqlKind.CAST, 94, true, null, InferTypes.FIRST_KNOWN, null);
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    return SqlStdOperatorTable.CAST.inferReturnType(opBinding);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return SqlStdOperatorTable.CAST.checkOperandTypes(callBinding,
        throwOnFailure);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlStdOperatorTable.CAST.getOperandCountRange();
  }

  @Override public SqlMonotonicity getMonotonicity(
      SqlOperatorBinding call) {
    return SqlStdOperatorTable.CAST.getMonotonicity(call);
  }
}

// End SqlCastOperator.java
