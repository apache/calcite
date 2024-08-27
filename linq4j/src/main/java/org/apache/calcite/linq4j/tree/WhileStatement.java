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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a "while" statement.
 */
public class WhileStatement extends Statement {
  public final Expression condition;
  public final Statement body;

  public WhileStatement(Expression condition, Statement body) {
    super(ExpressionType.While, Void.TYPE);
    this.condition = requireNonNull(condition, "condition");
    this.body = requireNonNull(body, "body");
  }

  @Override public Statement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    final Expression condition1 = condition.accept(shuttle);
    final Statement body1 = body.accept(shuttle);
    return shuttle.visit(this, condition1, body1);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    writer.append("while (").append(condition).append(") ").append(
        Blocks.toBlock(body));
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    WhileStatement that = (WhileStatement) o;
    return body.equals(that.body)
        && condition.equals(that.condition);
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, condition, body);
  }
}
