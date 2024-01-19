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
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Static;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities to register NAMED_STRUCT function.
 */
public class NamedStructUtils {

  private NamedStructUtils() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static SqlBasicFunction create() {
    return SqlBasicFunction.create(
      "NAMED_STRUCT",
      NamedStructUtils::returnType,
      OPERAND_TYPE_CHECKER);
  }

  private static RelDataType returnType(SqlOperatorBinding operatorBinding) {
    final List<String> keys = new ArrayList<String>();
    final List<RelDataType> values = new ArrayList<RelDataType>();
    int i = 0;
    for (RelDataType type : operatorBinding.collectOperandTypes()) {
      if (i % 2 == 0) {
        Object key = operatorBinding.getOperandLiteralValue(i, Object.class);
        if (key == null) {
          throw operatorBinding.newError(
            Static.RESOURCE.namedStructRequiresLiteralStringKeysGotExpression());
        }
        if (key instanceof NlsString) {
          keys.add(((NlsString) key).getValue());
        } else {
          String tpe = key.getClass().getSimpleName();
          throw operatorBinding.newError(
            Static.RESOURCE.namedStructRequiresLiteralStringKeysGotOtherType(tpe));
        }

      } else {
        values.add(type);
      }
      i++;
    }
    final RelDataTypeFactory typeFactory = operatorBinding.getTypeFactory();
    return typeFactory.createStructType(values, keys);
  }

  private static final SqlOperandTypeChecker TWO_OR_MORE =
      OperandTypes.variadic(SqlOperandCountRanges.from(2));

  private static final SqlOperandTypeChecker OPERAND_TYPE_CHECKER =
      new SqlOperandTypeChecker() {
        @Override public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          final boolean isRangeOk = TWO_OR_MORE.checkOperandTypes(callBinding, throwOnFailure);
          final int operandCount = callBinding.operands().size();
          final boolean isEvenNumberOfArgs = operandCount % 2 != 0;

          if (throwOnFailure && isEvenNumberOfArgs) {
            throw callBinding.newError(Static.RESOURCE.namedStructRequiresEvenNumberOfArgs());
          }

          return isRangeOk && isEvenNumberOfArgs;
        }

        @Override public SqlOperandCountRange getOperandCountRange() {
          return TWO_OR_MORE.getOperandCountRange();
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(...)";
        }
      };
}
