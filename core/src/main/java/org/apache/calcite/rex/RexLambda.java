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
import org.apache.calcite.sql.SqlKind;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RexLambda extends RexNode {
  //~ Instance fields --------------------------------------------------------

  private final List<RexVariable> variables;
  private final RexNode expression;

  //~ Constructors -----------------------------------------------------------

  RexLambda(List<RexVariable> variables, RexNode expression) {
    this.expression = expression;
    this.variables = variables;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType getType() {
    return expression.getType();
  }

  public SqlKind getKind() {
    return SqlKind.LAMBDA;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitLambda(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitLambda(this, arg);
  }

  /**
   * Returns the expression whose field is being accessed.
   */
  public RexNode getReferenceExpr() {
    return expression;
  }

  public List<RexVariable> getVariables() {
    return variables;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RexLambda rexLambda = (RexLambda) o;
    return Objects.equals(expression, rexLambda.expression)
        && Objects.equals(variables, rexLambda.variables);
  }

  @Override public int hashCode() {
    return Objects.hash(expression, variables);
  }

  @Override public String toString() {
    if (digest == null) {
      digest = new StringBuilder().append("(")
          .append(StringUtils
              .join(
                  variables.stream().map(s -> s.getType().getSqlTypeName().getName()).collect(
                  Collectors.toList()), ','))
          .append(")->").append(getType().getSqlTypeName().getName()).toString();
    }
    return digest;
  }
}
