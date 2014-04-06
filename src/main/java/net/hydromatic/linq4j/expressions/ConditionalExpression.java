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
package net.hydromatic.linq4j.expressions;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents an expression that has a conditional operator.
 *
 * <p>With an odd number of expressions
 * {c0, e0, c1, e1, ..., c<sub>n-1</sub>, e<sub>n-1</sub>, e<sub>n</sub>}
 * represents "if (c0) e0 else if (c1) e1 ... else e<sub>n</sub>";
 * with an even number of expressions
 * {c0, e0, c1, e1, ..., c<sub>n-1</sub>, e<sub>n-1</sub>}
 * represents
 * "if (c0) e0 else if (c1) e1 ... else if (c<sub>n-1</sub>) e<sub>n-1</sub>".
 * </p>
 */
public class ConditionalExpression extends AbstractNode {
  private final List<Node> expressionList;

  public ConditionalExpression(List<Node> expressionList, Type type) {
    super(ExpressionType.Conditional, type);
    assert expressionList != null : "expressionList should not be null";
    this.expressionList = expressionList;
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    for (int i = 0; i < expressionList.size(); i += 2) {
      writer.append(i > 0 ? " else if (" : "if (")
          .append(expressionList.get(i))
          .append(") ")
          .append(Blocks.toBlock(expressionList.get(i + 1)));
    }
    if (expressionList.size() % 2 == 1) {
      writer.append(" else ").append(Blocks.toBlock(expressionList.get(
          expressionList.size() - 1)));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ConditionalExpression that = (ConditionalExpression) o;

    if (!expressionList.equals(that.expressionList)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + expressionList.hashCode();
    return result;
  }
}

// End ConditionalExpression.java
