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

import org.apache.calcite.avatica.util.Spacer;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Converts an expression to Java code.
 */
class ExpressionWriter {
  /** How many spaces to indent Java code. */
  private static final int INDENT = 2;

  private final Spacer spacer = new Spacer(0);
  private final StringBuilder buf = new StringBuilder();
  private boolean indentPending;
  private final boolean generics;

  ExpressionWriter() {
    this(true);
  }

  ExpressionWriter(boolean generics) {
    this.generics = generics;
  }

  public void write(Node expression) {
    if (expression instanceof Expression) {
      Expression expression1 = (Expression) expression;
      expression1.accept(this, 0, 0);
    } else {
      expression.accept(this);
    }
  }

  @Override public String toString() {
    return buf.toString();
  }

  /**
   * If parentheses are required, writes this expression out with
   * parentheses and returns true. If they are not required, does nothing
   * and returns false.
   */
  public boolean requireParentheses(Expression expression, int lprec,
      int rprec) {
    if (lprec < expression.nodeType.lprec
        && expression.nodeType.rprec >= rprec) {
      return false;
    }
    buf.append("(");
    expression.accept(this, 0, 0);
    buf.append(")");
    return true;
  }

  /**
   * Increases the indentation level.
   */
  public void begin() {
    spacer.add(INDENT);
  }

  /**
   * Decreases the indentation level.
   */
  public void end() {
    spacer.subtract(INDENT);
  }

  public ExpressionWriter newlineAndIndent() {
    buf.append("\n");
    indentPending = true;
    return this;
  }

  public ExpressionWriter indent() {
    spacer.spaces(buf);
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

  public ExpressionWriter append(Type type) {
    checkIndent();
    if (!generics) {
      type = Types.stripGenerics(type);
    }
    buf.append(Types.className(type));
    return this;
  }

  public ExpressionWriter append(AbstractNode o) {
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
      spacer.spaces(buf);
      indentPending = false;
    }
  }

  public StringBuilder getBuf() {
    checkIndent();
    return buf;
  }

  public ExpressionWriter list(String begin, String sep, String end,
      Iterable<?> list) {
    final Iterator<?> iterator = list.iterator();
    if (iterator.hasNext()) {
      begin(begin);
      for (;;) {
        Object o = iterator.next();
        if (o instanceof Expression) {
          ((Expression) o).accept(this, 0, 0);
        } else if (o instanceof MemberDeclaration) {
          ((MemberDeclaration) o).accept(this);
        } else if (o instanceof Type) {
          append((Type) o);
        } else {
          append(o);
        }
        if (!iterator.hasNext()) {
          break;
        }
        buf.append(sep);
        if (sep.endsWith("\n")) {
          indentPending = true;
        }
      }
      end(end);
    } else {
      while (begin.endsWith("\n")) {
        begin = begin.substring(0, begin.length() - 1);
      }
      buf.append(begin).append(end);
    }
    return this;
  }

  public void backUp() {
    if (buf.lastIndexOf("\n") == buf.length() - 1) {
      buf.delete(buf.length() - 1, buf.length());
      indentPending = false;
    }
  }
}

// End ExpressionWriter.java
