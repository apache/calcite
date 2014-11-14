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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;

/**
 * Namespace for UNNEST.
 */
class UnnestNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall unnest;
  private final SqlValidatorScope scope;

  //~ Constructors -----------------------------------------------------------

  UnnestNamespace(
      SqlValidatorImpl validator,
      SqlCall unnest,
      SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    assert scope != null;
    assert unnest.getOperator() == SqlStdOperatorTable.UNNEST;
    this.unnest = unnest;
    this.scope = scope;
  }

  //~ Methods ----------------------------------------------------------------

  protected RelDataType validateImpl() {
    // Validate the call and its arguments, and infer the return type.
    validator.validateCall(unnest, scope);
    RelDataType type =
        unnest.getOperator().validateOperands(validator, scope, unnest);

    if (type.isStruct()) {
      return type;
    }
    return validator.getTypeFactory().builder()
        .add(validator.deriveAlias(unnest, 0), type)
        .build();
  }

  /**
   * Returns the type of the argument to UNNEST.
   */
  private RelDataType inferReturnType() {
    final SqlNode operand = unnest.operand(0);
    RelDataType type = validator.getValidatedNodeType(operand);

    // If sub-query, pick out first column.
    // TODO: Handle this using usual sub-select validation.
    if (type.isStruct()) {
      type = type.getFieldList().get(0).getType();
    }
    MultisetSqlType t = (MultisetSqlType) type;
    return t.getComponentType();
  }

  public SqlNode getNode() {
    return unnest;
  }
}

// End UnnestNamespace.java
