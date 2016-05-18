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

/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * <p>Examples:
 *
 * <ul>
 * <li><code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code>,
 * <li><code>SELECT * FROM (TABLE getEmpsInDept(30))</code>.
 * </ul>
 *
 * <p>NOTE: jhyde, 2006/4/24: These days, this class seems to be used
 * exclusively for the <code>MULTISET</code> construct.
 *
 * @see CollectScope
 */
public class CollectNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall child;
  private final SqlValidatorScope scope;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a CollectNamespace.
   *
   * @param child         Parse tree node
   * @param scope         Scope
   * @param enclosingNode Enclosing parse tree node
   */
  CollectNamespace(
      SqlCall child,
      SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super((SqlValidatorImpl) scope.getValidator(), enclosingNode);
    this.child = child;
    this.scope = scope;
    assert child.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR
        || child.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR;
  }

  //~ Methods ----------------------------------------------------------------

  protected RelDataType validateImpl(RelDataType targetRowType) {
    return child.getOperator().deriveType(validator, scope, child);
  }

  public SqlNode getNode() {
    return child;
  }

  public SqlValidatorScope getScope() {
    return scope;
  }
}

// End CollectNamespace.java
