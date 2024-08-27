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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * An operator that applies a filter before rows are included in an aggregate
 * function.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: a call to an aggregate function ({@link SqlCall})
 * <li>1: predicate
 * </ul>
 */
public class SqlFilterOperator extends SqlBinaryOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlFilterOperator() {
    super("FILTER", SqlKind.FILTER, 20, true, ReturnTypes.ARG0_FORCE_NULLABLE,
        null, OperandTypes.ANY_ANY);
  }

  //~ Methods ----------------------------------------------------------------


  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    call.operand(0).unparse(writer, leftPrec, getLeftPrec());
    writer.sep(getName());
    writer.sep("(");
    writer.sep("WHERE");
    call.operand(1).unparse(writer, getRightPrec(), rightPrec);
    writer.sep(")");
    writer.endList(frame);
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    assert call.operandCount() == 2;
    final SqlValidatorUtil.FlatAggregate flat = SqlValidatorUtil.flatten(call);
    SqlCall aggCall = flat.aggregateCall;
    if (!aggCall.getOperator().isAggregator()) {
      throw validator.newValidationError(aggCall,
          RESOURCE.filterNonAggregate());
    }
    validator.validateAggregateParams(aggCall, flat.filter,
        flat.distinctList, flat.orderList, scope);

    final SqlNode filter = requireNonNull(flat.filter);
    final RelDataType type = validator.deriveType(scope, filter);
    if (!SqlTypeUtil.inBooleanFamily(type)) {
      throw validator.newValidationError(filter,
          RESOURCE.condMustBeBoolean("FILTER"));
    }
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Validate type of the inner aggregate call
    validateOperands(validator, scope, call);

    // Assume the first operand is an aggregate call and derive its type.
    final SqlValidatorUtil.FlatAggregate flat = SqlValidatorUtil.flatten(call);
    final SqlCall aggCall = flat.aggregateCall;

    // Pretend that group-count is 0. This tells the aggregate function that it
    // might be invoked with 0 rows in a group. Most aggregate functions will
    // return NULL in this case.
    SqlCallBinding opBinding = new SqlCallBinding(validator, scope, aggCall) {
      @Override public int getGroupCount() {
        return 0;
      }
    };

    RelDataType ret = aggCall.getOperator().inferReturnType(opBinding);

    // Copied from validateOperands
    final SqlValidatorImpl validator1 = (SqlValidatorImpl) validator;
    validator1.setValidatedNodeType(call, ret);
    validator1.setValidatedNodeType(aggCall, ret);
    if (flat.distinctList != null) {
      validator1.setValidatedNodeType(
          requireNonNull(flat.distinctCall), ret);
    }
    if (flat.orderList != null) {
      validator1.setValidatedNodeType(
          requireNonNull(flat.orderCall), ret);
    }
    return ret;
  }

}
