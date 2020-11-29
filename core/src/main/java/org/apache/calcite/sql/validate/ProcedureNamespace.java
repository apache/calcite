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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Namespace whose contents are defined by the result of a call to a
 * user-defined procedure.
 */
public class ProcedureNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlValidatorScope scope;

  private final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  ProcedureNamespace(
      SqlValidatorImpl validator,
      SqlValidatorScope scope,
      SqlCall call,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.scope = scope;
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType validateImpl(RelDataType targetRowType) {
    validator.inferUnknownTypes(validator.unknownType, scope, call);
    final RelDataType type = validator.deriveTypeImpl(scope, call);
    final SqlOperator operator = call.getOperator();
    final SqlCallBinding callBinding =
        new SqlCallBinding(validator, scope, call);
    if (!(operator instanceof SqlTableFunction)) {
      throw new IllegalArgumentException("Argument must be a table function: "
          + operator.getNameAsId());
    }
    final SqlTableFunction tableFunction = (SqlTableFunction) operator;
    if (type.getSqlTypeName() != SqlTypeName.CURSOR) {
      throw new IllegalArgumentException("Table function should have CURSOR "
          + "type, not " + type);
    }
    final SqlReturnTypeInference rowTypeInference =
        tableFunction.getRowTypeInference();
    return requireNonNull(
        rowTypeInference.inferReturnType(callBinding),
        () -> "got null from inferReturnType for call " + callBinding.getCall());
  }

  @Override public @Nullable SqlNode getNode() {
    return call;
  }
}
