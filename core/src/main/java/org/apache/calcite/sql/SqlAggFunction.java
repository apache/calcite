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

import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Abstract base class for the definition of an aggregate function: an operator
 * which aggregates sets of values into a result.
 */
public abstract class SqlAggFunction extends SqlFunction implements Context {
  private final boolean requiresOrder;
  private final boolean requiresOver;

  //~ Constructors -----------------------------------------------------------

  /** Creates a built-in SqlAggFunction. */
  @Deprecated // to be removed before 2.0
  protected SqlAggFunction(
      String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory funcType) {
    // We leave sqlIdentifier as null to indicate that this is a builtin.
    this(name, null, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, funcType, false, false);
  }

  /** Creates a user-defined SqlAggFunction. */
  @Deprecated // to be removed before 2.0
  protected SqlAggFunction(
      String name,
      SqlIdentifier sqlIdentifier,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory funcType) {
    this(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, funcType, false, false);
  }

  /** Creates a built-in or user-defined SqlAggFunction or window function.
   *
   * <p>A user-defined function will have a value for {@code sqlIdentifier}; for
   * a built-in function it will be null. */
  protected SqlAggFunction(
      String name,
      SqlIdentifier sqlIdentifier,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory funcType,
      boolean requiresOrder,
      boolean requiresOver) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, null, funcType);
    this.requiresOrder = requiresOrder;
    this.requiresOver = requiresOver;
  }

  //~ Methods ----------------------------------------------------------------

  public <T> T unwrap(Class<T> clazz) {
    return clazz.isInstance(this) ? clazz.cast(this) : null;
  }

  @Override public boolean isAggregator() {
    return true;
  }

  @Override public boolean isQuantifierAllowed() {
    return true;
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);
    validator.validateAggregateParams(call, null, scope);
  }

  @Override public final boolean requiresOrder() {
    return requiresOrder;
  }

  @Override public final boolean requiresOver() {
    return requiresOver;
  }

  @Deprecated // to be removed before 2.0
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    throw new UnsupportedOperationException("remove before calcite-2.0");
  }

  @Deprecated // to be removed before 2.0
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    throw new UnsupportedOperationException("remove before calcite-2.0");
  }

  /** Whether this aggregate function allows a {@code FILTER (WHERE ...)}
   * clause. */
  public boolean allowsFilter() {
    return true;
  }
}

// End SqlAggFunction.java
