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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.LambdaScope;

import java.util.List;

/**
 * Lambda type-checking strategy where lambda return type is checked.
 */
public class LambdaOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private List<SqlTypeFamily> argFamilies;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Create using {@link OperandTypes#family}.
   */
  LambdaOperandTypeChecker(List<SqlTypeFamily> argFamilies) {
    this.argFamilies = argFamilies;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return checkSingleOperandType(callBinding, callBinding.getCall(), 0, throwOnFailure);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, argFamilies);
  }

  @Override public Consistency getConsistency() {
    return Consistency.NONE;
  }

  @Override public boolean isOptional(int i) {
    return false;
  }

  @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
      SqlNode node, int iFormalOperand, boolean throwOnFailure) {

    assert node instanceof SqlLambda;
    SqlLambda sqlLambda = (SqlLambda) node;
    LambdaScope lambdaScope = (LambdaScope) callBinding.getValidator().getLambdaScope(node);

    if (sqlLambda.getParameters().size() != argFamilies.size() - 1) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }

    for (int i = 1; i < argFamilies.size(); i++) {
      lambdaScope.setParameterType(i - 1, argFamilies.get(i));
      RelDataType type = callBinding.getValidator().getTypeFactory().createSqlType(SqlTypeName.ANY);
      if (argFamilies.get(i) != SqlTypeFamily.ANY) {
        type = argFamilies.get(i)
            .getDefaultConcreteType(callBinding.getTypeFactory());
      }
      callBinding.getValidator().setValidatedNodeType(sqlLambda.getParameters().get(i - 1), type);
    }

    RelDataType relDataType = callBinding.getValidator()
        .deriveType(lambdaScope, sqlLambda.getExpression());
    if (relDataType.getSqlTypeName().getFamily() != argFamilies.get(0)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }
}
