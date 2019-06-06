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

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * An operator that applies a filter before rows are included in an aggregate
 * function.
 *
 * <p>Operands are as follows:</p>
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

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    assert call.operandCount() == 2;
    SqlCall aggCall = getAggCall(call);
    if (!aggCall.getOperator().isAggregator()) {
      throw validator.newValidationError(aggCall,
          RESOURCE.filterNonAggregate());
    }
    final SqlNode condition = call.operand(1);
    SqlNodeList orderList = null;
    if (hasWithinGroupCall(call)) {
      SqlCall withinGroupCall = getWithinGroupCall(call);
      orderList = withinGroupCall.operand(1);
    }
    validator.validateAggregateParams(aggCall, condition, orderList, scope);

    final RelDataType type = validator.deriveType(scope, condition);
    if (!SqlTypeUtil.inBooleanFamily(type)) {
      throw validator.newValidationError(condition,
          RESOURCE.condMustBeBoolean("FILTER"));
    }
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Validate type of the inner aggregate call
    validateOperands(validator, scope, call);

    // Assume the first operand is an aggregate call and derive its type.
    final SqlCall aggCall = getAggCall(call);

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
    ((SqlValidatorImpl) validator).setValidatedNodeType(call, ret);
    ((SqlValidatorImpl) validator).setValidatedNodeType(aggCall, ret);
    if (hasWithinGroupCall(call)) {
      ((SqlValidatorImpl) validator).setValidatedNodeType(getWithinGroupCall(call), ret);
    }
    return ret;
  }

  private static SqlCall getAggCall(SqlCall call) {
    assert call.getOperator().getKind() == SqlKind.FILTER;
    call = call.operand(0);
    if (call.getOperator().getKind() == SqlKind.WITHIN_GROUP) {
      call = call.operand(0);
    }
    return call;
  }

  private static SqlCall getWithinGroupCall(SqlCall call) {
    assert call.getOperator().getKind() == SqlKind.FILTER;
    call = call.operand(0);
    if (call.getOperator().getKind() == SqlKind.WITHIN_GROUP) {
      return call;
    }
    throw new AssertionError();
  }

  private static boolean hasWithinGroupCall(SqlCall call) {
    assert call.getOperator().getKind() == SqlKind.FILTER;
    call = call.operand(0);
    return call.getOperator().getKind() == SqlKind.WITHIN_GROUP;
  }
}

// End SqlFilterOperator.java
