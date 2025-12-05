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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * An operator that applies a distinct operation before rows are included in an
 * aggregate function.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: a call to an aggregate function ({@link SqlCall})
 * <li>1: expressions to make distinct
 * </ul>
 */
public class SqlWithinDistinctOperator extends SqlBinaryOperator {
  public SqlWithinDistinctOperator() {
    super("WITHIN DISTINCT", SqlKind.WITHIN_DISTINCT, 100, true,
        ReturnTypes.ARG0, null, OperandTypes.ANY_IGNORE);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    call.operand(0).unparse(writer, 0, 0);
    writer.keyword("WITHIN DISTINCT");
    final SqlWriter.Frame orderFrame =
        writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, "(", ")");
    call.operand(1).unparse(writer, 0, 0);
    writer.endList(orderFrame);
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    assert call.operandCount() == 2;
    final SqlValidatorUtil.FlatAggregate flat =
        SqlValidatorUtil.flatten(call);
    if (!flat.aggregateCall.getOperator().isAggregator()) {
      throw validator.newValidationError(call,
          RESOURCE.withinDistinctNotAllowed(
              flat.aggregateCall.getOperator().getName()));
    }
    for (SqlNode order : requireNonNull(flat.distinctList)) {
      requireNonNull(validator.deriveType(scope, order));
    }
    validator.validateAggregateParams(flat.aggregateCall, flat.filter,
        flat.distinctList, flat.orderList, scope);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Validate type of the inner aggregate call
    return validateOperands(validator, scope, call);
  }
}
