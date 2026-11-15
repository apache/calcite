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
package org.apache.calcite.adapter.pig;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link PigUtils}.
 */
class PigUtilsTest {

  @Test void validIdentifiersAccepted() {
    // Examples of valid identifiers from the Pig Latin reference
    for (String name : new String[] {"A", "A123", "abc_123_BeX_", "t", "tc0"}) {
      assertDoesNotThrow(() -> PigUtils.checkValidIdentifier(name),
          () -> "expected '" + name + "' to be accepted");
    }
  }

  @Test void invalidIdentifiersRejected() {
    // Examples of invalid identifiers from the Pig Latin reference, plus
    // names that would be mis-parsed as Pig Latin operators if passed through
    for (String name : new String[] {"_A123", "abc_$", "A!B", "1abc", "",
        "a b", "a-b", "a;b", "a=b", "a.b", "EXPR$0"}) {
      final IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class,
              () -> PigUtils.checkValidIdentifier(name),
              () -> "expected '" + name + "' to be rejected");
      assertThat(
          e.getMessage(), is("Invalid Pig Latin identifier: '" + name
          + "'. Pig Latin identifiers must start with a letter, followed by"
          + " letters, digits or underscores"));
    }
  }

  @Test void plainValueQuoted() {
    assertThat(PigUtils.quoteStringLiteral("alice"), is("'alice'"));
  }

  @Test void valueWithBackslash() {
    assertThat(PigUtils.quoteStringLiteral("a\\b"), is("'a\\\\b'"));
  }

  @Test void valueWithBackslashBeforeApostrophe() {
    assertThat(PigUtils.quoteStringLiteral("a\\'b"), is("'a\\\\\\'b'"));
  }

  @Test void valueWithTrailingBackslash() {
    assertThat(PigUtils.quoteStringLiteral("a\\"), is("'a\\\\'"));
  }

  @Test void valueWithApostrophe() {
    assertThat(PigUtils.quoteStringLiteral("O'Brien"), is("'O\\'Brien'"));
  }

  @Test void valueWithApostropheAtTheEnd() {
    assertThat(PigUtils.quoteStringLiteral("a'"), is("'a\\''"));
  }

  @Test void valueWithApostropheAtTheStart() {
    assertThat(PigUtils.quoteStringLiteral("'a"), is("'\\'a'"));
  }

  @Test void valueWithMultipleApostrophes() {
    assertThat(PigUtils.quoteStringLiteral("a''b"), is("'a\\'\\'b'"));
  }

  @Test void emptyValue() {
    assertThat(PigUtils.quoteStringLiteral(""), is("''"));
  }
}
