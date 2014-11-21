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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.OrderByScope;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Static;

/**
 * The {@code GROUPING} function.
 */
class SqlGroupingFunction extends SqlFunction {
  public SqlGroupingFunction() {
    super("GROUPING", SqlKind.GROUPING, ReturnTypes.INTEGER, null,
        OperandTypes.ANY, SqlFunctionCategory.SYSTEM);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);
    final SelectScope selectScope =
        SqlValidatorUtil.getEnclosingSelectScope(scope);
    final SqlSelect select = selectScope.getNode();
    if (!validator.isAggregate(select)) {
      throw validator.newValidationError(call,
          Static.RESOURCE.groupingInAggregate());
    }
    final AggregatingSelectScope aggregatingSelectScope =
        SqlValidatorUtil.getEnclosingAggregateSelectScope(scope);
    if (aggregatingSelectScope == null) {
      // We're probably in the GROUP BY clause
      throw validator.newValidationError(call,
          Static.RESOURCE.groupingInWrongClause());
    }
    for (SqlNode operand : call.getOperandList()) {
      if (scope instanceof OrderByScope) {
        operand = validator.expandOrderExpr(select, operand);
      } else {
        operand = validator.expand(operand, scope);
      }
      if (!aggregatingSelectScope.isGroupingExpr(operand)) {
        throw validator.newValidationError(operand,
            Static.RESOURCE.groupingArgument());
      }
    }
  }
}

// End SqlGroupingFunction.java
