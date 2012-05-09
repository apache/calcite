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
 * Represents an expression that has a constant value.
 */
public class ConstantExpression extends Expression {
    final Object value;

    public ConstantExpression(Class type, Object value) {
        super(ExpressionType.Constant, type);
        this.value = value;
    }

    public Object evaluate(Evaluator evaluator) {
        return value;
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        if (value instanceof String) {
            escapeString(writer.getBuf(), (String) value);
        } else {
            writer.append(value);
        }
    }

    private static void escapeString(StringBuilder buf, String s) {
        buf.append('"');
        int n = s.length();
        char lastChar = 0;
        for (int i = 0; i < n; ++i) {
            char c = s.charAt(i);
            switch  (c) {
            case '\\':
                buf.append("\\\\");
                break;
            case '"':
                buf.append("\\\"");
                break;
            case '\n':
                buf.append("\\n");
                break;
            case '\r':
                if (lastChar != '\n') {
                    buf.append("\\r");
                }
                break;
            default:
                buf.append(c);
                break;
            }
            lastChar = c;
        }
        buf.append('"');
    }
}

// End ConstantExpression.java
