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

import org.junit.Test;

import java.util.AbstractList;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link Automaton}. */
public class AutomatonTest {
  @Test public void testSimple() {
    // pattern(a)
    final Pattern p = Pattern.builder().symbol("a").build();
    assertThat(p.toString(), is("a"));

    final String[] rows = {"", "a", "", "a"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", (s, list) -> s.contains("a"))
            .build();
    final String expected = "[[a], [a]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testSequence() {
    // pattern(a b)
    final Pattern p =
        Pattern.builder().symbol("a").symbol("b").seq().build();
    assertThat(p.toString(), is("a b"));

    final String[] rows = {"", "a", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", (s, list) -> s.contains("a"))
            .add("b", (s, list) -> s.contains("b"))
            .build();
    final String expected = "[[a, ab], [ab, b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testStar() {
    // pattern(a* b)
    final Pattern p = Pattern.builder()
        .symbol("a").star()
        .symbol("b").seq().build();
    assertThat(p.toString(), is("(a)* b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", (s, list) -> s.contains("a"))
            .add("b", (s, list) -> s.contains("b"))
            .build();
    final String expected = "[[b], [ab], [a, ab], [ab], [b], [b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testPlus() {
    // pattern(a+ b)
    final Pattern p = Pattern.builder()
        .symbol("a").plus()
        .symbol("b").seq().build();
    assertThat(p.toString(), is("(a)+ b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", (s, list) -> s.contains("a"))
            .add("b", (s, list) -> s.contains("b"))
            .build();
    final String expected = "[[ab, a, ab], [a, ab], [ab, b]]";
    assertThat(matcher.match(rows).toString(), is(expected));
  }

  @Test public void testRepeat() {
    // pattern(a b{0, 2} c)
    checkRepeat(0, 2, "a (b){0, 2} c", "[[a, c], [a, b, c], [a, b, b, c]]");
    // pattern(a b{0, 1} c)
    checkRepeat(0, 1, "a (b){0, 1} c", "[[a, c], [a, b, c]]");
    // pattern(a b{1, 1} c)
    checkRepeat(1, 1, "a (b){1} c", "[[a, b, c]]");
    // pattern(a b{1,3} c)
    checkRepeat(1, 3, "a (b){1, 3} c",
        "[[a, b, c], [a, b, b, c], [a, b, b, b, c]]");
    // pattern(a b{1,2} c)
    checkRepeat(1, 2, "a (b){1, 2} c", "[[a, b, c], [a, b, b, c]]");
    // pattern(a b{2,3} c)
    checkRepeat(2, 3, "a (b){2, 3} c", "[[a, b, b, c], [a, b, b, b, c]]");
  }

  private void checkRepeat(int minRepeat, int maxRepeat, String pattern,
      String expected) {
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").repeat(minRepeat, maxRepeat).seq()
        .symbol("c").seq()
        .build();
    assertThat(p.toString(), is(pattern));

    final String rows = "acabcabbcabbbcabbbbcabdbc";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p.toAutomaton())
            .add("a", (c, list) -> c == 'a')
            .add("b", (c, list) -> c == 'b')
            .add("c", (c, list) -> c == 'c')
            .build();
    assertThat(matcher.match(chars(rows)).toString(), is(expected));
  }

  @Test public void testRepeatComposite() {
    // pattern(a (b a){1, 2} c)
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").symbol("a").seq()
        .repeat(1, 2).seq()
        .symbol("c").seq()
        .build();
    assertThat(p.toString(), is("a (b a){1, 2} c"));

    final String rows = "acabcabbcabbbcabbbbcabdbcabacababcababac";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p.toAutomaton())
            .add("a", (c, list) -> c == 'a')
            .add("b", (c, list) -> c == 'b')
            .add("c", (c, list) -> c == 'c')
            .build();
    assertThat(matcher.match(chars(rows)).toString(),
        is("[[a, b, a, c], [a, b, a, b, a, c], [a, b, a, c]]"));
  }

  /** Converts a string into an iterable collection of its characters. */
  private static Iterable<Character> chars(String s) {
    return new AbstractList<Character>() {
      @Override public Character get(int index) {
        return s.charAt(index);
      }

      @Override public int size() {
        return s.length();
      }
    };
  }
}

// End AutomatonTest.java
