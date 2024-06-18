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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransform;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Definition of the SQL:2003 standard MULTISET query constructor, <code>
 * MULTISET (&lt;query&gt;)</code>.
 *
 * @see SqlMultisetValueConstructor
 */
public class SqlMultisetQueryConstructor extends SqlSpecialOperator {

  final SqlTypeTransform typeTransform;

  //~ Constructors -----------------------------------------------------------

  public SqlMultisetQueryConstructor() {
    this("MULTISET", SqlKind.MULTISET_QUERY_CONSTRUCTOR,
        SqlTypeTransforms.TO_MULTISET_QUERY);
  }

  protected SqlMultisetQueryConstructor(String name, SqlKind kind,
      SqlTypeTransform typeTransform) {
    super(name, kind, MDX_PRECEDENCE, false,
        ReturnTypes.ARG0.andThen(typeTransform), null, OperandTypes.VARIADIC);
    this.typeTransform = typeTransform;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final List<RelDataType> argTypes = SqlTypeUtil.deriveType(callBinding, callBinding.operands());
    final RelDataType componentType =
        callBinding.getTypeFactory().leastRestrictive(argTypes);
    if (null == componentType) {
      if (throwOnFailure) {
        throw callBinding.newValidationError(RESOURCE.needSameTypeParameter());
      }
      return false;
    }
    return true;
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    SqlSelect subSelect = call.operand(0);
    subSelect.validateExpr(validator, scope);
    final SqlValidatorNamespace ns =
        requireNonNull(validator.getNamespace(subSelect),
            () -> "namespace is missing for " + subSelect);
    final RelDataType rowType = requireNonNull(ns.getRowType(), "rowType");
    final SqlCallBinding opBinding = new SqlCallBinding(validator, scope, call);
    return typeTransform.transformType(opBinding, rowType);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    writer.keyword(getName());
    final SqlWriter.Frame frame = writer.startList("(", ")");
    assert call.operandCount() == 1;
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }
}
