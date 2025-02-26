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

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Represents an unconditional jump. This includes return statements, break and
 * continue statements, and other jumps.
 */
public class GotoStatement extends Statement {
  public final GotoExpressionKind kind;
  public final @Nullable LabelTarget labelTarget;
  public final @Nullable Expression expression;

  GotoStatement(GotoExpressionKind kind, @Nullable LabelTarget labelTarget,
      @Nullable Expression expression) {
    super(ExpressionType.Goto,
        expression == null ? Void.TYPE : expression.getType());
    this.kind = requireNonNull(kind, "kind");
    this.labelTarget = labelTarget;
    this.expression = expression;

    switch (kind) {
    case Break:
    case Continue:
      checkArgument(expression == null, "for %s, expression must be null",
          kind);
      break;
    case Goto:
      assert expression == null;
      requireNonNull(labelTarget, "labelTarget");
      break;
    case Return:
    case Sequence:
      checkArgument(labelTarget == null, "for %s, labelTarget must be null",
          kind);
      break;
    default:
      throw new RuntimeException("unexpected: " + kind);
    }
  }

  @Override public Statement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    Expression expression1 =
        expression == null ? null : expression.accept(shuttle);
    return shuttle.visit(this, expression1);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    writer.append(kind.prefix);
    if (labelTarget != null) {
      writer.append(' ').append(labelTarget.name);
    }
    if (expression != null) {
      if (!kind.prefix.isEmpty()) {
        writer.append(' ');
      }
      switch (kind) {
      case Sequence:
        // don't indent for sequence
        expression.accept(writer, 0, 0);
        break;
      default:
        writer.begin();
        expression.accept(writer, 0, 0);
        writer.end();
      }
    }
    writer.append(';').newlineAndIndent();
  }

  @Override public @Nullable Object evaluate(Evaluator evaluator) {
    switch (kind) {
    case Return:
    case Sequence:
      // NOTE: We ignore control flow. This is only correct if "return"
      // is the last statement in the block.
      return requireNonNull(expression, "expression").evaluate(evaluator);
    default:
      throw new AssertionError("evaluate not implemented");
    }
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

    GotoStatement that = (GotoStatement) o;
    return Objects.equals(expression, that.expression)
        && kind == that.kind
        && Objects.equals(labelTarget, that.labelTarget);
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, kind, labelTarget, expression);
  }
}
