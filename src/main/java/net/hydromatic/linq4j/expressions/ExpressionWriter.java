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

import java.util.*;

/**
 * Converts an expression to Java code.
 */
class ExpressionWriter {
    static Indent INDENT = new Indent(20);

    private final StringBuilder buf = new StringBuilder();
    private int level;
    private String indent;
    private boolean indentPending;

    public ExpressionWriter() {
    }

    public void write(Expression expression) {
        expression.accept(this, -1, -1);
    }

    @Override
    public String toString() {
        return buf.toString();
    }

    public boolean requireParentheses(
        Expression expression, int lprec, int rprec)
    {
        if (lprec < expression.nodeType.lprec
            && expression.nodeType.rprec >= rprec)
        {
            return false;
        }
        buf.append("(");
        expression.accept(this, -1, -1);
        buf.append(")");
        return true;
    }

    /**
     * Increases the indentation level.
     */
    public void begin() {
        indent = INDENT.get(++level);
    }

    /**
     * Decreases the indentation level.
     */
    public void end() {
        indent = INDENT.get(--level);
    }

    public ExpressionWriter newlineAndIndent() {
        buf.append("\n");
        indentPending = true;
        return this;
    }

    public ExpressionWriter indent() {
        buf.append(indent);
        return this;
    }

    public ExpressionWriter begin(String s) {
        append(s);
        begin();
        indentPending = s.endsWith("\n");
        return this;
    }

    public ExpressionWriter end(String s) {
        end();
        append(s);
        indentPending = s.endsWith("\n");
        return this;
    }

    public ExpressionWriter append(char c) {
        checkIndent();
        buf.append(c);
        return this;
    }

    public ExpressionWriter append(Class o) {
        checkIndent();
        buf.append(className(o));
        return this;
    }

    private static final Map<Class, String> PRIMITIVES =
        new HashMap<Class, String>();
    static {
        PRIMITIVES.put(Boolean.TYPE, "Boolean");
        PRIMITIVES.put(Byte.TYPE, "Byte");
        PRIMITIVES.put(Character.TYPE, "Character");
        PRIMITIVES.put(Short.TYPE, "Short");
        PRIMITIVES.put(Integer.TYPE, "Integer");
        PRIMITIVES.put(Long.TYPE, "Long");
        PRIMITIVES.put(Float.TYPE, "Float");
        PRIMITIVES.put(Double.TYPE, "Double");
        PRIMITIVES.put(Void.TYPE, "Void");
    }

    static String boxClassName(Class o) {
        if (o.isPrimitive()) {
            return PRIMITIVES.get(o);
        } else {
            return className(o);
        }
    }

    static String className(Class o) {
        if (o.isArray()) {
            return className(o.getComponentType()) + "[]";
        }
        String className = o.getName();
        if (o.getPackage() == Package.getPackage("java.lang")
            && !o.isPrimitive())
        {
            return className.substring("java.lang.".length());
        }
        return className;
    }

    public ExpressionWriter append(Expression o) {
        o.accept0(this);
        return this;
    }

    public ExpressionWriter append(Object o) {
        checkIndent();
        buf.append(o);
        return this;
    }

    public ExpressionWriter append(String s) {
        checkIndent();
        buf.append(s);
        return this;
    }

    private void checkIndent() {
        if (indentPending) {
            buf.append(indent);
            indentPending = false;
        }
    }

    public StringBuilder getBuf() {
        checkIndent();
        return buf;
    }

    public ExpressionWriter list(
        String begin, String sep, String end, Iterable<?> list)
    {
        begin(begin);
        int k = 0;
        for (Object o : list) {
            if (k++ > 0) {
                buf.append(sep);
                if (sep.endsWith("\n")) {
                    indentPending = true;
                }
            }
            if (o instanceof Expression) {
                ((Expression) o).accept(this, 0, 0);
            } else {
                append(o);
            }
        }
        end(end);
        return this;
    }

    private static class Indent extends ArrayList<String> {
        public Indent(int initialCapacity) {
            super(initialCapacity);
            ensureSize(initialCapacity);
        }

        public synchronized String of(int index) {
            ensureSize(index + 1);
            return get(index);
        }

        private void ensureSize(int targetSize) {
            if (targetSize < size()) {
                return;
            }
            char[] chars = new char[2 * targetSize];
            Arrays.fill(chars, ' ');
            String bigString = new String(chars);
            clear();
            for (int i = 0; i < targetSize; i++) {
                add(bigString.substring(0, i * 2));
            }
        }
    }
}

// End ExpressionWriter.java
