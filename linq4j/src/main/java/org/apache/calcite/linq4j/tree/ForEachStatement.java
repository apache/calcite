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

import java.util.Objects;

/**
 * Represents a "for-each" loop, "for (T v : iterable) { f(v); }"
 */
public class ForEachStatement extends Statement {
  public final ParameterExpression parameter;
  public final Expression iterable;
  public final Statement body;

  /** Cache the hash code for the expression */
  private int hash;

  public ForEachStatement(ParameterExpression parameter, Expression iterable,
      Statement body) {
    super(ExpressionType.ForEach, Void.TYPE);
    this.parameter = Objects.requireNonNull(parameter);
    this.iterable = Objects.requireNonNull(iterable);
    this.body = Objects.requireNonNull(body); // may be empty block, not null
  }

  @Override public ForEachStatement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    final Expression iterable1 = iterable.accept(shuttle);
    final Statement body1 = body.accept(shuttle);
    return shuttle.visit(this, parameter, iterable1, body1);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    writer.append("for (")
        .append(parameter.type)
        .append(" ")
        .append(parameter)
        .append(" : ")
        .append(iterable)
        .append(") ")
        .append(Blocks.toBlock(body));
  }

  @Override public boolean equals(Object o) {
    return this == o
        || o instanceof ForEachStatement
        && parameter.equals(((ForEachStatement) o).parameter)
        && iterable.equals(((ForEachStatement) o).iterable)
        && body.equals(((ForEachStatement) o).body);
  }

  @Override public int hashCode() {
    int result = hash;
    if (result == 0) {
      result =
          Objects.hash(nodeType, type, parameter, iterable, body);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

// End ForEachStatement.java
