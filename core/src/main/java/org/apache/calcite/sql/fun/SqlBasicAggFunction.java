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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlStaticAggFunction;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Concrete implementation of {@link SqlAggFunction}.
 *
 * <p>The class is final, and instances are immutable.
 *
 * <p>Instances are created only by {@link SqlBasicAggFunction#create} and are
 * "modified" by "wither" methods such as {@link #withDistinct} to create a new
 * instance with one property changed. Since the class is final, you can modify
 * behavior only by providing strategy objects, not by overriding methods in a
 * sub-class.
 */
public final class SqlBasicAggFunction extends SqlAggFunction {
  private final @Nullable SqlStaticAggFunction staticFun;
  private final Optionality distinctOptionality;
  private final SqlSyntax syntax;
  private final boolean allowsNullTreatment;
  private final boolean allowsSeparator;
  private final boolean percentile;
  private final boolean allowsFraming;

  //~ Constructors -----------------------------------------------------------

  private SqlBasicAggFunction(String name, @Nullable SqlIdentifier sqlIdentifier,
      SqlKind kind, SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      @Nullable SqlStaticAggFunction staticFun,
      SqlFunctionCategory funcType,
      boolean requiresOrder, boolean requiresOver,
      Optionality requiresGroupOrder, Optionality distinctOptionality,
      SqlSyntax syntax, boolean allowsNullTreatment, boolean allowsSeparator,
      boolean percentile, boolean allowsFraming) {
    super(name, sqlIdentifier, kind,
        requireNonNull(returnTypeInference, "returnTypeInference"),
        operandTypeInference,
        requireNonNull(operandTypeChecker, "operandTypeChecker"),
        requireNonNull(funcType, "funcType"), requiresOrder, requiresOver,
        requiresGroupOrder);
    this.staticFun = staticFun;
    this.distinctOptionality =
        requireNonNull(distinctOptionality, "distinctOptionality");
    this.syntax = requireNonNull(syntax, "syntax");
    this.allowsNullTreatment = allowsNullTreatment;
    this.allowsSeparator = allowsSeparator;
    this.percentile = percentile;
    this.allowsFraming = allowsFraming;
  }

  /** Creates a SqlBasicAggFunction whose name is the same as its kind. */
  public static SqlBasicAggFunction create(SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return create(kind.name(), kind, returnTypeInference, operandTypeChecker);
  }

  /** Creates a SqlBasicAggFunction. */
  public static SqlBasicAggFunction create(String name, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return new SqlBasicAggFunction(name, null, kind, returnTypeInference, null,
        operandTypeChecker, null, SqlFunctionCategory.NUMERIC, false, false,
        Optionality.FORBIDDEN, Optionality.OPTIONAL, SqlSyntax.FUNCTION, false,
        false, false, true);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz.isInstance(staticFun)) {
      return clazz.cast(staticFun);
    }
    return super.unwrap(clazz);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    SqlCall strippedCall = call;
    if (syntax == SqlSyntax.ORDERED_FUNCTION) {
      if (allowsSeparator) {
        strippedCall = ReturnTypes.stripSeparator(strippedCall);
      }
      strippedCall = ReturnTypes.stripOrderBy(strippedCall);
    }

    RelDataType derivedType = super.deriveType(validator, scope, strippedCall);

    // Assigning back the operands that might have been casted by validator
    for (int i = 0; i < strippedCall.getOperandList().size(); i++) {
      call.setOperand(i, strippedCall.getOperandList().get(i));
    }

    return derivedType;
  }

  @Override public Optionality getDistinctOptionality() {
    return distinctOptionality;
  }

  @Override public SqlReturnTypeInference getReturnTypeInference() {
    // constructor ensures it is non-null
    return requireNonNull(super.getReturnTypeInference(), "returnTypeInference");
  }

  @Override public SqlOperandTypeChecker getOperandTypeChecker() {
    // constructor ensures it is non-null
    return requireNonNull(super.getOperandTypeChecker(), "operandTypeChecker");
  }

  /** Sets {@link #getName()}. */
  public SqlAggFunction withName(String name) {
    return new SqlBasicAggFunction(name, getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Sets {@link #getDistinctOptionality()}. */
  SqlBasicAggFunction withDistinct(Optionality distinctOptionality) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Sets {@link #getFunctionType()}. */
  public SqlBasicAggFunction withFunctionType(SqlFunctionCategory category) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, category, requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  @Override public SqlSyntax getSyntax() {
    return syntax;
  }

  /** Sets {@link #getSyntax()}. */
  public SqlBasicAggFunction withSyntax(SqlSyntax syntax) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  @Override public boolean allowsNullTreatment() {
    return allowsNullTreatment;
  }

  /** Sets {@link #allowsNullTreatment()}. */
  public SqlBasicAggFunction withAllowsNullTreatment(boolean allowsNullTreatment) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Returns whether this aggregate function allows '{@code SEPARATOR string}'
   * among its arguments. */
  public boolean allowsSeparator() {
    return allowsSeparator;
  }

  /** Sets {@link #allowsSeparator()}. */
  public SqlBasicAggFunction withAllowsSeparator(boolean allowsSeparator) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  @Override public boolean isPercentile() {
    return percentile;
  }

  /** Sets {@link #isPercentile()}. */
  public SqlBasicAggFunction withPercentile(boolean percentile) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  @Override public boolean allowsFraming() {
    return allowsFraming;
  }

  /** Sets {@link #allowsFraming()}. */
  public SqlBasicAggFunction withAllowsFraming(boolean allowsFraming) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Sets {@link #requiresOver()}. */
  public SqlBasicAggFunction withOver(boolean over) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        over, requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Sets {@link #requiresGroupOrder()}. */
  public SqlBasicAggFunction withGroupOrder(Optionality groupOrder) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), groupOrder, distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }

  /** Sets that value to be returned when {@link #unwrap} is applied to
   * {@link SqlStaticAggFunction}{@code .class}. */
  public SqlBasicAggFunction withStatic(SqlStaticAggFunction staticFun) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), staticFun, getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment, allowsSeparator, percentile, allowsFraming);
  }
}
