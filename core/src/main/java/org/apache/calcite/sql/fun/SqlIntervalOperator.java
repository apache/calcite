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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;

/** Interval expression.
 *
 * <p>Syntax:
 *
 * <blockquote><pre>INTERVAL numericExpression timeUnit
 *
 * timeUnit: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND</pre></blockquote>
 *
 * <p>Compare with interval literal, whose syntax is
 * {@code INTERVAL characterLiteral timeUnit [ TO timeUnit ]}.
 */
public class SqlIntervalOperator extends SqlInternalOperator {
  private static final SqlReturnTypeInference RETURN_TYPE =
      ((SqlReturnTypeInference) SqlIntervalOperator::returnType)
          .andThen(SqlTypeTransforms.TO_NULLABLE);

  SqlIntervalOperator() {
    super("INTERVAL", SqlKind.INTERVAL, 0, true, RETURN_TYPE,
        InferTypes.ANY_NULLABLE, OperandTypes.NUMERIC_INTERVAL);
  }

  private static RelDataType returnType(SqlOperatorBinding opBinding) {
    final SqlIntervalQualifier intervalQualifier =
        opBinding.getOperandLiteralValue(1, SqlIntervalQualifier.class);
    return opBinding.getTypeFactory().createSqlIntervalType(intervalQualifier);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    writer.keyword("INTERVAL");
    final SqlNode expression = call.operand(0);
    final SqlIntervalQualifier intervalQualifier = call.operand(1);
    expression.unparseWithParentheses(writer, leftPrec, rightPrec,
        !(expression instanceof SqlLiteral
            || expression instanceof SqlIdentifier
            || expression.getKind() == SqlKind.MINUS_PREFIX
            || writer.isAlwaysUseParentheses()));
    assert intervalQualifier.timeUnitRange.endUnit == null;
    intervalQualifier.unparse(writer, 0, 0);
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0} {1} {2}"; // e.g. "INTERVAL <INTEGER> <INTERVAL HOUR>"
    default:
      throw new AssertionError();
    }
  }
}
