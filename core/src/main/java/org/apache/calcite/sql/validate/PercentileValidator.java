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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Visitor that finds if there are any percentile functions present and
 * validates that its operands are valid.
 */
public class PercentileValidator extends SqlBasicVisitor<Void> {
  private final SqlValidator validator;
  private final SqlValidatorScope scope;

  private boolean isPercentile;
  private String functionName;
  private SqlNodeList orderByList;

  public PercentileValidator(SqlValidator validator,
      SqlValidatorScope scope) {
    this.validator = validator;
    this.scope = scope;
    this.isPercentile = false;
    this.functionName = "";
    this.orderByList = SqlNodeList.EMPTY;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Void visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall sqlBasicCall = (SqlBasicCall) call;
      if (sqlBasicCall.getOperator() instanceof SqlBasicAggFunction) {
        SqlBasicAggFunction agg = (SqlBasicAggFunction) sqlBasicCall.getOperator();
        if (isPercentile(agg)) {
          isPercentile = true;
          functionName = agg.getName();
          return null;
        }
      }
    }
    return super.visit(call);
  }

  @Override public Void visit(SqlNodeList nodeList) {
    if (isPercentile) {
      orderByList = nodeList;
      return null;
    }
    return super.visit(nodeList);
  }

  public void validate(SqlCall call) {
    if (isPercentile) {
      // Validate that percentile function have a single ORDER BY expression
      if (orderByList.getList().size() != 1) {
        throw validator.newValidationError(call,
            RESOURCE.invalidArgCount(functionName, 1));
      }

      // Validate that the ORDER BY field is of NUMERIC type
      SqlNode node = orderByList.get(0);
      assert node != null;

      RelDataType type = validator.deriveType(scope, node);
      if (!isSupportedType(type)) {
        throw validator.newValidationError(call,
            RESOURCE.typeMustBeNumeric(type.getSqlTypeName().getName()));
      }
    }
  }

  private boolean isSupportedType(RelDataType type) {
    // TODO: Add support for interval types, like DATE/TIME.
    return type.getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
  }

  public static boolean isPercentile(SqlBasicAggFunction aggFunc) {
    return aggFunc == SqlStdOperatorTable.PERCENTILE_CONT
        || aggFunc == SqlStdOperatorTable.PERCENTILE_DISC;
  }
}
