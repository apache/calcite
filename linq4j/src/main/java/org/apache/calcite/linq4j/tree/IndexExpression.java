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
package org.apache.calcite.linq4j.tree;

import java.util.List;
import java.util.Objects;

/**
 * Represents indexing a property or array.
 */
public class IndexExpression extends Expression {
  public final Expression array;
  public final List<Expression> indexExpressions;

  public IndexExpression(Expression array, List<Expression> indexExpressions) {
    super(ExpressionType.ArrayIndex, Types.getComponentType(array.getType()));
    assert array != null : "array should not be null";
    assert indexExpressions != null : "indexExpressions should not be null";
    assert !indexExpressions.isEmpty() : "indexExpressions should not be empty";
    this.array = array;
    this.indexExpressions = indexExpressions;
  }

  @Override public Expression accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    Expression array = this.array.accept(shuttle);
    List<Expression> indexExpressions = Expressions.acceptExpressions(
        this.indexExpressions, shuttle);
    return shuttle.visit(this, array, indexExpressions);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept(ExpressionWriter writer, int lprec, int rprec) {
    array.accept(writer, lprec, nodeType.lprec);
    writer.list("[", ", ", "]", indexExpressions);
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    IndexExpression that = (IndexExpression) o;

    if (!array.equals(that.array)) {
      return false;
    }
    if (!indexExpressions.equals(that.indexExpressions)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, array, indexExpressions);
  }
}

// End IndexExpression.java
