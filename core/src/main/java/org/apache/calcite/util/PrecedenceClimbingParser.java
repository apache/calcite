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
package org.apache.calcite.util;

import org.apache.calcite.linq4j.Ord;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Parser that takes a collection of tokens (atoms and operators)
 * and groups them together according to the operators' precedence
 * and associativity.
 */
public class PrecedenceClimbingParser {
  private Token first;
  private Token last;

  private PrecedenceClimbingParser(List<Token> tokens) {
    Token p = null;
    for (Token token : tokens) {
      if (p != null) {
        p.next = token;
      } else {
        first = token;
      }
      token.previous = p;
      token.next = null;
      p = token;
    }
    last = p;
  }

  public Token atom(Object o) {
    return new Token(Type.ATOM, o, -1, -1);
  }

  public Call call(Op op, ImmutableList<Token> args) {
    return new Call(op, args);
  }

  public Op infix(Object o, int precedence, boolean left) {
    return new Op(Type.INFIX, o, precedence * 2 + (left ? 0 : 1),
        precedence * 2 + (left ? 1 : 0));
  }

  public Op prefix(Object o, int precedence) {
    return new Op(Type.PREFIX, o, -1, precedence * 2);
  }

  public Op postfix(Object o, int precedence) {
    return new Op(Type.POSTFIX, o, precedence * 2, -1);
  }

  public SpecialOp special(Object o, int leftPrec, int rightPrec,
      Special special) {
    return new SpecialOp(o, leftPrec * 2, rightPrec * 2, special);
  }

  public Token parse() {
    partialParse();
    if (first != last) {
      throw new AssertionError("could not find next operator to reduce: "
          + this);
    }
    return first;
  }

  public void partialParse() {
    for (;;) {
      Op op = highest();
      if (op == null) {
        return;
      }
      final Token t;
      switch (op.type) {
      case POSTFIX:
        t = call(op, ImmutableList.of(op.previous));
        replace(t, op.previous.previous, op.next);
        break;
      case PREFIX:
        t = call(op, ImmutableList.of(op.next));
        replace(t, op.previous, op.next.next);
        break;
      case INFIX:
        t = call(op, ImmutableList.of(op.previous, op.next));
        replace(t, op.previous.previous, op.next.next);
        break;
      case SPECIAL:
        Result r = ((SpecialOp) op).special.apply(this, (SpecialOp) op);
        Objects.requireNonNull(r);
        replace(r.replacement, r.first.previous, r.last.next);
        break;
      default:
        throw new AssertionError();
      }
      // debug: System.out.println(this);
    }
  }

  @Override public String toString() {
    return Util.commaList(all());
  }

  /** Returns a list of all tokens. */
  public List<Token> all() {
    return new TokenList();
  }

  private void replace(Token t, Token previous, Token next) {
    t.previous = previous;
    t.next = next;
    if (previous == null) {
      first = t;
    } else {
      previous.next = t;
    }
    if (next == null) {
      last = t;
    } else {
      next.previous = t;
    }
  }

  private Op highest() {
    int p = -1;
    Op highest = null;
    for (Token t = first; t != null; t = t.next) {
      if ((t.left > p || t.right > p)
          && (t.left < 0 || t.left >= prevRight(t.previous))
          && (t.right < 0 || t.right >= nextLeft(t.next))) {
        p = Math.max(t.left, t.right);
        highest = (Op) t;
      }
    }
    return highest;
  }

  /** Returns the right precedence of the preceding operator token. */
  private int prevRight(Token token) {
    for (; token != null; token = token.previous) {
      if (token.type == Type.POSTFIX) {
        return Integer.MAX_VALUE;
      }
      if (token.right >= 0) {
        return token.right;
      }
    }
    return -1;
  }

  /** Returns the left precedence of the following operator token. */
  private int nextLeft(Token token) {
    for (; token != null; token = token.next) {
      if (token.type == Type.PREFIX) {
        return Integer.MAX_VALUE;
      }
      if (token.left >= 0) {
        return token.left;
      }
    }
    return -1;
  }

  public String print(Token token) {
    return token.toString();
  }

  public PrecedenceClimbingParser copy(int start, Predicate<Token> predicate) {
    final List<Token> tokens = new ArrayList<>();
    for (Token token : Util.skip(all(), start)) {
      if (predicate.test(token)) {
        break;
      }
      tokens.add(token.copy());
    }
    return new PrecedenceClimbingParser(tokens);
  }

  /** Token type. */
  public enum Type {
    ATOM,
    CALL,
    PREFIX,
    INFIX,
    POSTFIX,
    SPECIAL
  }

  /** A token: either an atom, a call to an operator with arguments,
   * or an unmatched operator. */
  public static class Token {
    Token previous;
    Token next;
    public final Type type;
    public final Object o;
    final int left;
    final int right;

    Token(Type type, Object o, int left, int right) {
      this.type = type;
      this.o = o;
      this.left = left;
      this.right = right;
    }

    @Override public String toString() {
      return o.toString();
    }

    protected StringBuilder print(StringBuilder b) {
      return b.append(o);
    }

    public Token copy() {
      return new Token(type, o, left, right);
    }
  }

  /** An operator token. */
  public static class Op extends Token {
    Op(Type type, Object o, int left, int right) {
      super(type, o, left, right);
    }

    @Override public Token copy() {
      return new Op(type, o, left, right);
    }
  }

  /** An token corresponding to a special operator. */
  public static class SpecialOp extends Op {
    public final Special special;

    SpecialOp(Object o, int left, int right, Special special) {
      super(Type.SPECIAL, o, left, right);
      this.special = special;
    }

    @Override public Token copy() {
      return new SpecialOp(o, left, right, special);
    }
  }


  /** A token that is a call to an operator with arguments. */
  public static class Call extends Token {
    public final Op op;
    public final ImmutableList<Token> args;

    Call(Op op, ImmutableList<Token> args) {
      super(Type.CALL, null, -1, -1);
      this.op = op;
      this.args = args;
    }

    @Override public Token copy() {
      return new Call(op, args);
    }

    @Override public String toString() {
      return print(new StringBuilder()).toString();
    }

    protected StringBuilder print(StringBuilder b) {
      switch (op.type) {
      case PREFIX:
        b.append('(');
        printOp(b, false, true);
        args.get(0).print(b);
        return b.append(')');
      case POSTFIX:
        b.append('(');
        args.get(0).print(b);
        return printOp(b, true, false).append(')');
      case INFIX:
        b.append('(');
        args.get(0).print(b);
        printOp(b, true, true);
        args.get(1).print(b);
        return b.append(')');
      case SPECIAL:
        printOp(b, false, false)
            .append('(');
        for (Ord<Token> arg : Ord.zip(args)) {
          if (arg.i > 0) {
            b.append(", ");
          }
          arg.e.print(b);
        }
        return b.append(')');
      default:
        throw new AssertionError();
      }
    }

    private StringBuilder printOp(StringBuilder b, boolean leftSpace,
        boolean rightSpace) {
      String s = op.o.toString();
      if (leftSpace) {
        b.append(' ');
      }
      b.append(s);
      if (rightSpace) {
        b.append(' ');
      }
      return b;
    }
  }

  /** Callback defining the behavior of a special function. */
  public interface Special {
    /** Given an occurrence of this operator, identifies the range of tokens to
     * be collapsed into a call of this operator, and the arguments to that
     * call. */
    Result apply(PrecedenceClimbingParser parser, SpecialOp op);
  }

  /** Result of a call to {@link Special#apply}. */
  public static class Result {
    final Token first;
    final Token last;
    final Token replacement;

    public Result(Token first, Token last, Token replacement) {
      this.first = first;
      this.last = last;
      this.replacement = replacement;
    }
  }

  /** Fluent helper to build a parser containing a list of tokens. */
  public static class Builder {
    final List<Token> tokens = new ArrayList<>();
    private final PrecedenceClimbingParser dummy =
        new PrecedenceClimbingParser(ImmutableList.of());

    private Builder add(Token t) {
      tokens.add(t);
      return this;
    }

    public Builder atom(Object o) {
      return add(dummy.atom(o));
    }

    public Builder call(Op op, Token arg0, Token arg1) {
      return add(dummy.call(op, ImmutableList.of(arg0, arg1)));
    }

    public Builder infix(Object o, int precedence, boolean left) {
      return add(dummy.infix(o, precedence, left));
    }

    public Builder prefix(Object o, int precedence) {
      return add(dummy.prefix(o, precedence));
    }

    public Builder postfix(Object o, int precedence) {
      return add(dummy.postfix(o, precedence));
    }

    public Builder special(Object o, int leftPrec, int rightPrec,
        Special special) {
      return add(dummy.special(o, leftPrec, rightPrec, special));
    }

    public PrecedenceClimbingParser build() {
      return new PrecedenceClimbingParser(tokens);
    }
  }

  /** List view onto the tokens in a parser. The view is semi-mutable; it
   * supports {@link List#remove(int)} but not {@link List#set} or
   * {@link List#add}. */
  private class TokenList extends AbstractList<Token> {
    @Override public Token get(int index) {
      for (Token t = first; t != null; t = t.next) {
        if (index-- == 0) {
          return t;
        }
      }
      throw new IndexOutOfBoundsException();
    }

    @Override public int size() {
      int n = 0;
      for (Token t = first; t != null; t = t.next) {
        ++n;
      }
      return n;
    }

    @Override public Token remove(int index) {
      Token t = get(index);
      if (t.previous == null) {
        first = t.next;
      } else {
        t.previous.next = t.next;
      }
      if (t.next == null) {
        last = t.previous;
      } else {
        t.next.previous = t.previous;
      }
      return t;
    }

    @Override public Token set(int index, Token element) {
      final Token t = get(index);
      element.previous = t.previous;
      if (t.previous == null) {
        first = element;
      } else {
        t.previous.next = element;
      }
      element.next = t.next;
      if (t.next == null) {
        last = element;
      } else {
        t.next.previous = element;
      }
      return t;
    }
  }
}

// End PrecedenceClimbingParser.java
