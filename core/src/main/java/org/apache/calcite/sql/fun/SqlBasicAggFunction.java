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
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

import java.util.Objects;

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
  private final Optionality distinctOptionality;
  private final SqlSyntax syntax;
  private final boolean allowsNullTreatment;

  //~ Constructors -----------------------------------------------------------

  private SqlBasicAggFunction(String name, SqlIdentifier sqlIdentifier,
      SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory funcType,
      boolean requiresOrder, boolean requiresOver,
      Optionality requiresGroupOrder, Optionality distinctOptionality,
      SqlSyntax syntax, boolean allowsNullTreatment) {
    super(name, sqlIdentifier, kind,
        Objects.requireNonNull(returnTypeInference), operandTypeInference,
        Objects.requireNonNull(operandTypeChecker),
        Objects.requireNonNull(funcType), requiresOrder, requiresOver,
        requiresGroupOrder);
    this.distinctOptionality = Objects.requireNonNull(distinctOptionality);
    this.syntax = Objects.requireNonNull(syntax);
    this.allowsNullTreatment = allowsNullTreatment;
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
        operandTypeChecker, SqlFunctionCategory.NUMERIC, false, false,
        Optionality.FORBIDDEN, Optionality.OPTIONAL, SqlSyntax.FUNCTION, false);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    if (syntax == SqlSyntax.ORDERED_FUNCTION) {
      call = ReturnTypes.stripOrderBy(call);
    }
    return super.deriveType(validator, scope, call);
  }

  @Override public Optionality getDistinctOptionality() {
    return distinctOptionality;
  }

  /** Sets {@link #getDistinctOptionality()}. */
  SqlBasicAggFunction withDistinct(Optionality distinctOptionality) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment);
  }

  /** Sets {@link #getFunctionType()}. */
  public SqlBasicAggFunction withFunctionType(SqlFunctionCategory category) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), category, requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment);
  }

  @Override public SqlSyntax getSyntax() {
    return syntax;
  }

  /** Sets {@link #getSyntax()}. */
  public SqlBasicAggFunction withSyntax(SqlSyntax syntax) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment);
  }

  @Override public boolean allowsNullTreatment() {
    return allowsNullTreatment;
  }

  /** Sets {@link #allowsNullTreatment()}. */
  public SqlBasicAggFunction withAllowsNullTreatment(boolean allowsNullTreatment) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), getFunctionType(), requiresOrder(),
        requiresOver(), requiresGroupOrder(), distinctOptionality, syntax,
        allowsNullTreatment);
  }

  /** Sets {@link #requiresGroupOrder()}. */
  public SqlBasicAggFunction withGroupOrder(Optionality groupOrder) {
    return new SqlBasicAggFunction(getName(), getSqlIdentifier(), kind,
        getReturnTypeInference(), getOperandTypeInference(),
        getOperandTypeChecker(), getFunctionType(), requiresOrder(),
        requiresOver(), groupOrder, distinctOptionality, syntax,
        allowsNullTreatment);
  }
}
