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
package org.apache.calcite.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;

/** Regular expression, to be compiled into an {@link Automaton}. */
public interface Pattern {
  default Automaton toAutomaton() {
    return new AutomatonBuilder().add(this).build();
  }

  /** Creates a builder. */
  static PatternBuilder builder() {
    return new PatternBuilder();
  }

  /** Operator that constructs composite {@link Pattern} instances. */
  enum Op {
    /** A leaf pattern, consisting of a single symbol. */
    SYMBOL(0, 0),
    /** Anchor for start "^" */
    ANCHOR_START(0, 0),
    /** Anchor for end "$" */
    ANCHOR_END(0, 0),
    /** Pattern that matches one pattern followed by another. */
    SEQ(2, -1),
    /** Pattern that matches one pattern or another. */
    OR(2, -1),
    /** Pattern that matches a pattern repeated zero or more times. */
    STAR(1, 1),
    /** Pattern that matches a pattern repeated one or more times. */
    PLUS(1, 1),
    /** Pattern that matches a pattern repeated between {@code minRepeat}
     * and {@code maxRepeat} times. */
    REPEAT(1, 1),
    /** Pattern that machtes a pattern one time or zero times */
    OPTIONAL(1, 1);

    private final int minArity;
    private final int maxArity;

    Op(int minArity, int maxArity) {
      this.minArity = minArity;
      this.maxArity = maxArity;
    }
  }

  /** Builds a pattern expression. */
  class PatternBuilder {
    final Stack<Pattern> stack = new Stack<>();

    private PatternBuilder() {}

    private PatternBuilder push(Pattern item) {
      stack.push(item);
      return this;
    }

    /** Returns the resulting pattern. */
    public Pattern build() {
      if (stack.size() != 1) {
        throw new AssertionError("expected stack to have one item, but was "
            + stack);
      }
      return stack.pop();
    }

    /** Returns the resulting automaton. */
    public Automaton automaton() {
      return new AutomatonBuilder().add(build()).build();
    }

    /** Creates a pattern that matches symbol,
     * and pushes it onto the stack.
     *
     * @see SymbolPattern */
    public PatternBuilder symbol(String symbolName) {
      return push(new SymbolPattern(symbolName));
    }

    /** Creates a pattern that matches the two patterns at the top of the
     * stack in sequence,
     * and pushes it onto the stack. */
    public PatternBuilder seq() {
      final Pattern pattern1 = stack.pop();
      final Pattern pattern0 = stack.pop();
      return push(new OpPattern(Op.SEQ, pattern0, pattern1));
    }

    /** Creates a pattern that matches the patterns at the top
     * of the stack zero or more times,
     * and pushes it onto the stack. */
    public PatternBuilder star() {
      return push(new OpPattern(Op.STAR, stack.pop()));
    }

    /** Creates a pattern that matches the patterns at the top
     * of the stack one or more times,
     * and pushes it onto the stack. */
    public PatternBuilder plus() {
      return push(new OpPattern(Op.PLUS, stack.pop()));
    }

    /** Creates a pattern that matches either of the two patterns at the top
     * of the stack,
     * and pushes it onto the stack. */
    public PatternBuilder or() {
      if (stack.size() < 2) {
        throw new AssertionError("Expecting stack to have at least 2 items, but has "
            + stack.size());
      }
      final Pattern pattern1 = stack.pop();
      final Pattern pattern0 = stack.pop();
      return push(new OpPattern(Op.OR, pattern0, pattern1));
    }

    public PatternBuilder repeat(int minRepeat, int maxRepeat) {
      final Pattern pattern = stack.pop();
      return push(new RepeatPattern(minRepeat, maxRepeat, pattern));
    }

    public PatternBuilder optional() {
      final Pattern pattern = stack.pop();
      return push(new OpPattern(Op.OPTIONAL, pattern));
    }

  }

  /** Base class for implementations of {@link Pattern}. */
  abstract class AbstractPattern implements Pattern {
    final Op op;

    AbstractPattern(Op op) {
      this.op = Objects.requireNonNull(op);
    }

    public Automaton toAutomaton() {
      return new AutomatonBuilder().add(this).build();
    }
  }

  /** Pattern that matches a symbol. */
  class SymbolPattern extends AbstractPattern {
    final String name;

    SymbolPattern(String name) {
      super(Op.SYMBOL);
      this.name = Objects.requireNonNull(name);
    }

    @Override public String toString() {
      return name;
    }

  }

  /** Pattern with one or more arguments. */
  class OpPattern extends AbstractPattern {
    final ImmutableList<Pattern> patterns;

    OpPattern(Op op, Pattern... patterns) {
      super(op);
      Preconditions.checkArgument(patterns.length >= op.minArity);
      Preconditions.checkArgument(op.maxArity == -1
          || patterns.length <= op.maxArity);
      this.patterns = ImmutableList.copyOf(patterns);
    }

    @Override public String toString() {
      switch (op) {
      case SEQ:
        return patterns.stream().map(Object::toString)
            .collect(Collectors.joining(" "));
      case STAR:
        return "(" + patterns.get(0) + ")*";
      case PLUS:
        return "(" + patterns.get(0) + ")+";
      case OR:
        return patterns.get(0) + "|" + patterns.get(1);
      case OPTIONAL:
        return patterns.get(0) + "?";
      default:
        throw new AssertionError("unknown op " + op);
      }
    }

  }

  /** Pattern that matches a pattern repeated between {@code minRepeat}
   * and {@code maxRepeat} times. */
  class RepeatPattern extends OpPattern {
    final int minRepeat;
    final int maxRepeat;

    RepeatPattern(int minRepeat, int maxRepeat, Pattern pattern) {
      super(Op.REPEAT, pattern);
      this.minRepeat = minRepeat;
      this.maxRepeat = maxRepeat;
    }

    @Override public String toString() {
      return "(" + patterns.get(0) + "){" + minRepeat
          + (maxRepeat == minRepeat ? "" : (", " + maxRepeat))
          + "}";
    }

  }
}

// End Pattern.java
