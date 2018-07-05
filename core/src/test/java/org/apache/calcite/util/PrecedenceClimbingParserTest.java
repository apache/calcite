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

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link PrecedenceClimbingParser}.
 */
public class PrecedenceClimbingParserTest {
  @Test public void testBasic() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("a")
        .infix("+", 1, true)
        .prefix("-", 3)
        .atom("b")
        .infix("*", 2, true)
        .atom("c")
        .postfix("!", 4)
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("(a + ((- b) * (c !)))"));
  }

  @Test public void testRepeatedPrefixPostfix() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .prefix("+", 3)
        .prefix("-", 3)
        .prefix("+", 3)
        .prefix("+", 3)
        .atom("a")
        .postfix("!", 4)
        .infix("+", 1, true)
        .prefix("-", 3)
        .prefix("-", 3)
        .atom("b")
        .postfix("!", 4)
        .postfix("!", 4)
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token),
        is("((+ (- (+ (+ (a !))))) + (- (- ((b !) !))))"));
  }

  @Test public void testAtom() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("a")
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("a"));
  }

  @Test public void testOnlyPrefix() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .prefix("-", 3)
        .prefix("-", 3)
        .atom(1)
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("(- (- 1))"));
  }

  @Test public void testOnlyPostfix() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom(1)
        .postfix("!", 33333)
        .postfix("!", 33333)
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("((1 !) !)"));
  }

  @Test public void testLeftAssociative() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("a")
        .infix("*", 2, true)
        .atom("b")
        .infix("+", 1, true)
        .atom("c")
        .infix("+", 1, true)
        .atom("d")
        .infix("+", 1, true)
        .atom("e")
        .infix("*", 2, true)
        .atom("f")
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("((((a * b) + c) + d) + (e * f))"));
  }

  @Test public void testRightAssociative() {
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("a")
        .infix("^", 3, false)
        .atom("b")
        .infix("^", 3, false)
        .atom("c")
        .infix("^", 3, false)
        .atom("d")
        .infix("+", 1, true)
        .atom("e")
        .infix("*", 2, true)
        .atom("f")
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("((a ^ (b ^ (c ^ d))) + (e * f))"));
  }

  @Test public void testSpecial() {
    // price > 5 and price between 1 + 2 and 3 * 4 and price is null
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("price")
        .infix(">", 4, true)
        .atom("5")
        .infix("and", 2, true)
        .atom("price")
        .special("between", 3, 3,
            (parser, op) ->
                new PrecedenceClimbingParser.Result(op.previous,
                    op.next.next.next,
                    parser.call(op,
                        ImmutableList.of(op.previous, op.next,
                            op.next.next.next))))
        .atom("1")
        .infix("+", 5, true)
        .atom("2")
        .infix("and", 2, true)
        .atom("3")
        .infix("*", 6, true)
        .atom("4")
        .infix("and", 2, true)
        .atom("price")
        .postfix("is null", 4)
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token),
        is("(((price > 5) and between(price, (1 + 2), (3 * 4)))"
            + " and (price is null))"));
  }

  @Test public void testEqualPrecedence() {
    // LIKE has same precedence as '='; LIKE is right-assoc, '=' is left
    final PrecedenceClimbingParser p = new PrecedenceClimbingParser.Builder()
        .atom("a")
        .infix("=", 3, true)
        .atom("b")
        .infix("like", 3, false)
        .atom("c")
        .infix("=", 3, true)
        .atom("d")
        .build();
    final PrecedenceClimbingParser.Token token = p.parse();
    assertThat(p.print(token), is("(((a = b) like c) = d)"));
  }
}

// End PrecedenceClimbingParserTest.java
