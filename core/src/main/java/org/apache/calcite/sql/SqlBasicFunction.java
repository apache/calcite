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

import org.apache.calcite.sql.type.OperandHandlers;
import org.apache.calcite.sql.type.SqlOperandHandler;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Concrete implementation of {@link SqlFunction}.
 *
 * <p>The class is final, and instances are immutable.
 *
 * <p>Instances are created only by {@link SqlBasicFunction#create} and are
 * "modified" by "wither" methods such as {@link #withName} to create a new
 * instance with one property changed. Since the class is final, you can modify
 * behavior only by providing strategy objects, not by overriding methods in a
 * subclass.
 */
public class SqlBasicFunction extends SqlFunction {
  private final SqlSyntax syntax;
  private final boolean deterministic;
  private final SqlOperandHandler operandHandler;
  private final int callValidator;
  private final Function<SqlOperatorBinding, SqlMonotonicity> monotonicityInference;
  private final boolean dynamic;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new SqlFunction for a call to a built-in function.
   *
   * @param name Name of built-in function
   * @param kind Kind of operator implemented by function
   * @param syntax Syntax
   * @param deterministic Whether the function is deterministic
   * @param returnTypeInference Strategy to use for return type inference
   * @param operandTypeInference Strategy to use for parameter type inference
   * @param operandHandler Strategy to use for handling operands
   * @param operandTypeChecker Strategy to use for parameter type checking
   * @param callValidator Strategy to validate calls
   * @param category Categorization for function
   * @param monotonicityInference Strategy to infer monotonicity of a call
   */
  private SqlBasicFunction(String name, SqlKind kind, SqlSyntax syntax,
      boolean deterministic, SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      SqlOperandHandler operandHandler,
      SqlOperandTypeChecker operandTypeChecker,
      Integer callValidator,
      SqlFunctionCategory category,
      Function<SqlOperatorBinding, SqlMonotonicity> monotonicityInference,
      boolean dynamic) {
    super(name, kind,
        requireNonNull(returnTypeInference, "returnTypeInference"),
        operandTypeInference,
        requireNonNull(operandTypeChecker, "operandTypeChecker"), category);
    this.syntax = requireNonNull(syntax, "syntax");
    this.deterministic = deterministic;
    this.operandHandler = requireNonNull(operandHandler, "operandHandler");
    this.callValidator = requireNonNull(callValidator, "callValidator");
    this.monotonicityInference =
        requireNonNull(monotonicityInference, "monotonicityInference");
    this.dynamic = dynamic;
  }

  /** Creates a {@code SqlBasicFunction} whose name is the same as its kind
   * and whose category {@link SqlFunctionCategory#SYSTEM}. */
  public static SqlBasicFunction create(SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return new SqlBasicFunction(kind.name(), kind,
        SqlSyntax.FUNCTION, true, returnTypeInference, null,
        OperandHandlers.DEFAULT, operandTypeChecker, 0,
        SqlFunctionCategory.SYSTEM, call -> SqlMonotonicity.NOT_MONOTONIC, false);
  }

  /** Creates a {@code SqlBasicFunction}
   * with kind {@link SqlKind#OTHER_FUNCTION}
   * and category {@link SqlFunctionCategory#NUMERIC}. */
  public static SqlBasicFunction create(String name,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return new SqlBasicFunction(name, SqlKind.OTHER_FUNCTION,
        SqlSyntax.FUNCTION, true, returnTypeInference, null,
        OperandHandlers.DEFAULT, operandTypeChecker, 0,
        SqlFunctionCategory.NUMERIC, call -> SqlMonotonicity.NOT_MONOTONIC, false);
  }

  /** Creates a {@code SqlBasicFunction}
   * with kind {@link SqlKind#OTHER_FUNCTION}. */
  public static SqlBasicFunction create(String name,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category) {
    return new SqlBasicFunction(name, SqlKind.OTHER_FUNCTION,
        SqlSyntax.FUNCTION, true, returnTypeInference, null,
        OperandHandlers.DEFAULT, operandTypeChecker, 0,
        category, call -> SqlMonotonicity.NOT_MONOTONIC, false);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlReturnTypeInference getReturnTypeInference() {
    return requireNonNull(super.getReturnTypeInference(), "returnTypeInference");
  }

  @Override public SqlOperandTypeChecker getOperandTypeChecker() {
    return requireNonNull(super.getOperandTypeChecker(), "operandTypeChecker");
  }

  @Override public SqlSyntax getSyntax() {
    return syntax;
  }

  @Override public boolean isDeterministic() {
    return deterministic;
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return monotonicityInference.apply(call);
  }

  @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return operandHandler.rewriteCall(validator, call);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);
  }

  @Override public boolean isDynamicFunction() {
    return dynamic;
  }

  /** Returns a copy of this function with a given name. */
  public SqlBasicFunction withName(String name) {
    return new SqlBasicFunction(name, kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given kind. */
  public SqlBasicFunction withKind(SqlKind kind) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given category. */
  public SqlBasicFunction withFunctionType(SqlFunctionCategory category) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator, category, monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given syntax. */
  public SqlBasicFunction withSyntax(SqlSyntax syntax) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given strategy for inferring
   * returned type. */
  public SqlBasicFunction withReturnTypeInference(
      SqlReturnTypeInference returnTypeInference) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        returnTypeInference, getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given strategy for inferring
   * the types of its operands. */
  public SqlBasicFunction withOperandTypeInference(
      SqlOperandTypeInference operandTypeInference) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), operandTypeInference, operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given strategy for handling
   * operands. */
  public SqlBasicFunction withOperandHandler(SqlOperandHandler operandHandler) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }
  /** Returns a copy of this function with a given determinism. */
  public SqlBasicFunction withDeterministic(boolean deterministic) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  /** Returns a copy of this function with a given strategy for inferring
   * whether a call is monotonic. */
  public SqlBasicFunction withMonotonicityInference(
      Function<SqlOperatorBinding, SqlMonotonicity> monotonicityInference) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  public SqlBasicFunction withValidation(int callValidator) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  public SqlBasicFunction withDynamic(boolean dynamic) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        getOperandTypeChecker(), callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }

  public SqlBasicFunction withOperandTypeChecker(SqlOperandTypeChecker operandTypeChecker) {
    return new SqlBasicFunction(getName(), kind, syntax, deterministic,
        getReturnTypeInference(), getOperandTypeInference(), operandHandler,
        operandTypeChecker, callValidator,
        getFunctionType(), monotonicityInference, dynamic);
  }
}
