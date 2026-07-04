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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * An operator describing a window function specification.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: name of window function ({@link org.apache.calcite.sql.SqlCall})</li>
 *
 * <li>1: window name ({@link org.apache.calcite.sql.SqlLiteral}) or
 * window in-line specification ({@link SqlWindow})</li>
 *
 * </ul>
 */
public class SqlOverOperator extends SqlBinaryOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlOverOperator() {
    super(
        "OVER",
        SqlKind.OVER,
        20,
        true,
        ReturnTypes.ARG0_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_IGNORE);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    assert call.operandCount() == 2;
    SqlCall aggCall = call.operand(0);
    boolean hasFilter = false;
    switch (aggCall.getKind()) {
    case RESPECT_NULLS:
    case IGNORE_NULLS:
    case FILTER:
      if (aggCall.getKind() == SqlKind.FILTER) {
        hasFilter = true;
      }
      validator.validateCall(aggCall, scope);
      aggCall = aggCall.operand(0);
      break;
    default:
      break;
    }
    // Support "agg WITHIN GROUP (ORDER BY ...) OVER (...)" for inverse
    // distribution functions such as PERCENTILE_CONT/PERCENTILE_DISC. The
    // WITHIN GROUP wrapper carries the sort key, and the underlying operand
    // is the actual aggregate. This is non-standard (Oracle) syntax, gated by
    // conformance; otherwise the WITHIN GROUP call is not an aggregator and the
    // overNonAggregate error below fires.
    SqlNodeList groupOrderList = null;
    if (aggCall.getKind() == SqlKind.WITHIN_GROUP
        && validator.config().conformance().allowWithinGroupOverAggregate()) {
      validator.validateCall(aggCall, scope);
      groupOrderList = aggCall.operand(1);
      aggCall = aggCall.operand(0);
    }
    if (!aggCall.getOperator().isAggregator()) {
      throw validator.newValidationError(aggCall, RESOURCE.overNonAggregate());
    }
    // COUNT(DISTINCT) is not allowed in window functions with FILTER
    if (hasFilter && aggCall.getKind() == SqlKind.COUNT
        && aggCall.getFunctionQuantifier() != null) {
      throw validator.newValidationError(aggCall, RESOURCE.overNonAggregate());
    }
    final SqlNode window = call.operand(1);
    validator.validateWindow(window, scope, aggCall, groupOrderList);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Validate type of the inner aggregate call
    validateOperands(validator, scope, call);

    // Assume the first operand is an aggregate call and derive its type.
    // When we are sure the window is not empty, pass that information to the
    // aggregate's operator return type inference as groupCount=1
    // Otherwise pass groupCount=0 so the agg operator understands the window
    // can be empty
    SqlNode agg = call.operand(0);

    if (!(agg instanceof SqlCall)) {
      throw new IllegalStateException("Argument to SqlOverOperator"
          + " should be SqlCall, got " + agg.getClass() + ": " + agg);
    }

    SqlNode window = call.operand(1);
    SqlWindow w = validator.resolveWindow(window, scope);

    SqlCall aggCall = (SqlCall) agg;
    // "agg WITHIN GROUP (ORDER BY ...) OVER (...)": the WITHIN GROUP wrapper
    // has already derived the correct return type (e.g. the collation column
    // type for PERCENTILE_CONT/DISC via SqlWithinGroupOperator.deriveType), so
    // reuse it rather than re-inferring from the bare aggregate call, which
    // would fail because the sort key is not available to the aggregate alone.
    if (aggCall.getKind() == SqlKind.WITHIN_GROUP) {
      RelDataType ret = validator.deriveType(scope, aggCall);
      validator.setValidatedNodeType(call, ret);
      validator.setValidatedNodeType(agg, ret);
      return ret;
    }
    // Unwrap FILTER, RESPECT_NULLS, or IGNORE_NULLS to get the actual aggregate call
    while (aggCall != null
        && (aggCall.getKind() == SqlKind.FILTER
            || aggCall.getKind() == SqlKind.RESPECT_NULLS
            || aggCall.getKind() == SqlKind.IGNORE_NULLS)) {
      aggCall = aggCall.operand(0);
    }

    SqlCallBinding opBinding = new SqlCallBinding(validator, scope, aggCall) {
      @Override public boolean hasEmptyGroup() {
        return !w.isAlwaysNonEmpty();
      }
    };

    RelDataType ret = aggCall.getOperator().inferReturnType(opBinding);

    // Copied from validateOperands
    validator.setValidatedNodeType(call, ret);
    validator.setValidatedNodeType(agg, ret);
    return ret;
  }

  /**
   * Accepts a {@link SqlVisitor}, and tells it to visit each child.
   *
   * @param visitor Visitor
   */
  @Override public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (onlyExpressions) {
      for (Ord<SqlNode> operand : Ord.zip(call.getOperandList())) {
        // if the second param is an Identifier then it's supposed to
        // be a name from a window clause and isn't part of the
        // group by check
        if (operand == null) {
          continue;
        }
        if (operand.i == 1 && operand.e instanceof SqlIdentifier) {
          continue;
        }
        argHandler.visitChild(visitor, call, operand.i, operand.e);
      }
    } else {
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }
}
