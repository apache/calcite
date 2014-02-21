/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlWithOperator;
import org.eigenbase.util.Util;

/**
 * Namespace for <code>WITH</code> clause.
 */
public class WithNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall with;
  private final SqlValidatorScope scope;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a TableConstructorNamespace.
   *
   * @param validator     Validator
   * @param with          WITH clause
   * @param scope         Scope
   * @param enclosingNode Enclosing node
   */
  WithNamespace(SqlValidatorImpl validator,
      SqlCall with,
      SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.with = with;
    this.scope = scope;
    assert with.getKind() == SqlKind.WITH;
  }

  //~ Methods ----------------------------------------------------------------

  protected RelDataType validateImpl() {
    final SqlWithOperator.Call call = SqlWithOperator.Call.of(with);
    for (SqlNode with : call.withList) {
      validator.validateWithItem((SqlCall) with);
    }
    final SqlValidatorScope scope2 =
        validator.getWithScope(Util.last(call.withList.getList()));
    validator.validateQuery(call.body, scope2);
    final RelDataType rowType = validator.getValidatedNodeType(call.body);
    validator.setValidatedNodeType(with, rowType);
    return rowType;
  }

  public SqlNode getNode() {
    return with;
  }

  /**
   * Returns the scope.
   *
   * @return scope
   */
  public SqlValidatorScope getScope() {
    return scope;
  }
}

// End TableConstructorNamespace.java
