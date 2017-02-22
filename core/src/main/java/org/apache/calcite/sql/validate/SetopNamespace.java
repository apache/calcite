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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
 */
public class SetopNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SetopNamespace</code>.
   *
   * @param validator     Validator
   * @param call          Call to set operator
   * @param enclosingNode Enclosing node
   */
  protected SetopNamespace(
      SqlValidatorImpl validator,
      SqlCall call,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return call;
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    SqlMonotonicity monotonicity = null;
    int index = getRowType().getFieldNames().indexOf(columnName);
    if (index < 0) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
    for (SqlNode operand : call.getOperandList()) {
      final SqlValidatorNamespace namespace = validator.getNamespace(operand);
      monotonicity = combine(monotonicity,
          namespace.getMonotonicity(
              namespace.getRowType().getFieldNames().get(index)));
    }
    return monotonicity;
  }

  private SqlMonotonicity combine(SqlMonotonicity m0, SqlMonotonicity m1) {
    if (m0 == null) {
      return m1;
    }
    if (m1 == null) {
      return m0;
    }
    if (m0 == m1) {
      return m0;
    }
    if (m0.unstrict() == m1) {
      return m1;
    }
    if (m1.unstrict() == m0) {
      return m0;
    }
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  public RelDataType validateImpl(RelDataType targetRowType) {
    switch (call.getKind()) {
    case UNION:
    case INTERSECT:
    case EXCEPT:
      final SqlValidatorScope scope = validator.scopes.get(call);
      for (SqlNode operand : call.getOperandList()) {
        if (!(operand.isA(SqlKind.QUERY))) {
          throw validator.newValidationError(operand,
              RESOURCE.needQueryOp(operand.toString()));
        }
        validator.validateQuery(operand, scope, targetRowType);
      }
      return call.getOperator().deriveType(
          validator,
          scope,
          call);
    default:
      throw new AssertionError("Not a query: " + call.getKind());
    }
  }
}

// End SetopNamespace.java
