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
public class GotoExpression extends Expression {
    public final GotoExpressionKind kind;
    private final LabelTarget labelTarget;
    private final Expression expression;

    public GotoExpression(
        GotoExpressionKind kind,
        LabelTarget labelTarget,
        Expression expression)
    {
        super(ExpressionType.Goto, Void.TYPE);
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
            assert labelTarget == null;
            break;
        default:
            throw new RuntimeException("unexpected: " + kind);
        }
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        writer.append(kind.s);
        if (labelTarget != null) {
            writer.append(' ')
                .append(labelTarget.name);
        }
        if (expression != null) {
            writer.append(' ');
            writer.begin();
            expression.accept(writer, 0, 0);
            writer.end();
        }
        writer.append(';');
        writer.newlineAndIndent();
    }
}

// End GotoExpression.java
