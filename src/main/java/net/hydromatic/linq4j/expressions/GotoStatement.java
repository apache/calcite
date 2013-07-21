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

/**
 * Represents an unconditional jump. This includes return statements, break and
 * continue statements, and other jumps.
 */
public class GotoStatement extends Statement {
  public final GotoExpressionKind kind;
  public final LabelTarget labelTarget;
  public final Expression expression;

  GotoStatement(GotoExpressionKind kind, LabelTarget labelTarget,
      Expression expression) {
    super(ExpressionType.Goto,
        expression == null ? Void.TYPE : expression.getType());
    this.kind = kind;
    this.labelTarget = labelTarget;
    this.expression = expression;

    switch (kind) {
    case Break:
    case Continue:
      assert expression == null;
      break;
    case Goto:
      assert expression == null;
      assert labelTarget != null;
      break;
    case Return:
    case Sequence:
      assert labelTarget == null;
      break;
    default:
      throw new RuntimeException("unexpected: " + kind);
    }
  }

  @Override
  public Statement accept(Visitor visitor) {
    Expression expression1 =
        expression == null ? null : expression.accept(visitor);
    return visitor.visit(this, expression1);
  }

  @Override
  void accept0(ExpressionWriter writer) {
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

  @Override
  public Object evaluate(Evaluator evaluator) {
    switch (kind) {
    case Return:
    case Sequence:
      // NOTE: We ignore control flow. This is only correct if "return"
      // is the last statement in the block.
      return expression.evaluate(evaluator);
    default:
      throw new AssertionError("evaluate not implemented");
    }
  }
}

// End GotoStatement.java
