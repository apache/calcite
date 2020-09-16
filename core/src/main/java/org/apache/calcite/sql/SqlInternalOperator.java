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
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Generic operator for nodes with internal syntax.
 *
 * <p>If you do not override {@link #getSyntax()} or
 * {@link #unparse(SqlWriter, SqlCall, int, int)}, they will be unparsed using
 * function syntax, {@code F(arg1, arg2, ...)}. This may be OK for operators
 * that never appear in SQL, only as structural elements in an abstract syntax
 * tree.
 *
 * <p>You can use this operator, without creating a sub-class, for
 * non-expression nodes. Validate will validate the arguments, but will not
 * attempt to deduce a type.
 */
public class SqlInternalOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlInternalOperator(
      String name,
      SqlKind kind) {
    this(name, kind, 2);
  }

  public SqlInternalOperator(
      String name,
      SqlKind kind,
      int prec) {
    this(name, kind, prec, true, ReturnTypes.ARG0, null, OperandTypes.VARIADIC);
  }

  public SqlInternalOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean isLeftAssoc,
      SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        isLeftAssoc,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    return validateOperands(validator, scope, call);
  }
}
