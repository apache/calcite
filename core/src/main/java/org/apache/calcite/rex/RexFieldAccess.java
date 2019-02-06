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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

/**
 * Access to a field of a row-expression.
 *
 * <p>You might expect to use a <code>RexFieldAccess</code> to access columns of
 * relational tables, for example, the expression <code>emp.empno</code> in the
 * query
 *
 * <blockquote>
 * <pre>SELECT emp.empno FROM emp</pre>
 * </blockquote>
 *
 * <p>but there is a specialized expression {@link RexInputRef} for this
 * purpose. So in practice, <code>RexFieldAccess</code> is usually used to
 * access fields of correlating variables, for example the expression
 * <code>emp.deptno</code> in
 *
 * <blockquote>
 * <pre>SELECT ename
 * FROM dept
 * WHERE EXISTS (
 *     SELECT NULL
 *     FROM emp
 *     WHERE emp.deptno = dept.deptno
 *     AND gender = 'F')</pre>
 * </blockquote>
 */
public class RexFieldAccess extends RexNode {
  //~ Instance fields --------------------------------------------------------

  private final RexNode expr;
  private final RelDataTypeField field;

  //~ Constructors -----------------------------------------------------------

  RexFieldAccess(
      RexNode expr,
      RelDataTypeField field) {
    this.expr = expr;
    this.field = field;
    this.digest = expr + "." + field.getName();
    assert expr.getType().getFieldList().get(field.getIndex()) == field;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataTypeField getField() {
    return field;
  }

  public RelDataType getType() {
    return field.getType();
  }

  public SqlKind getKind() {
    return SqlKind.FIELD_ACCESS;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitFieldAccess(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitFieldAccess(this, arg);
  }

  /**
   * Returns the expression whose field is being accessed.
   */
  public RexNode getReferenceExpr() {
    return expr;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RexFieldAccess that = (RexFieldAccess) o;

    return field.equals(that.field) && expr.equals(that.expr);
  }

  @Override public int hashCode() {
    int result = expr.hashCode();
    result = 31 * result + field.hashCode();
    return result;
  }
}

// End RexFieldAccess.java
