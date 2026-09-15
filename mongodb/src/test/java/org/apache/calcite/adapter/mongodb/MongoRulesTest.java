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
package org.apache.calcite.adapter.mongodb;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for the BSON identifier-quoting helpers in
 * {@link MongoRules}. Field names emitted verbatim into a BSON
 * document (as {@code $group._id} keys or aggregate output names)
 * must be quoted whenever they contain a character that BsonDocument's
 * parser would otherwise treat as part of a different token, so a
 * value containing punctuation cannot shift the document boundaries.
 */
class MongoRulesTest {

  @Test void plainNameNotQuoted() {
    assertThat(MongoRules.maybeQuote("field"), is("field"));
    assertThat(MongoRules.maybeQuote("_field"), is("_field"));
    assertThat(MongoRules.maybeQuote("Field1"), is("Field1"));
  }

  @Test void dollarNameQuoted() {
    // A leading {@code $} would otherwise be parsed by BsonDocument
    // as an operator reference; quote it so the name stays literal.
    assertThat(MongoRules.maybeQuote("$field"), is("'$field'"));
  }

  @Test void syntheticExprNameQuoted() {
    // Calcite emits synthetic aggregate column names like EXPR$0.
    // The $ makes them fail the "plain identifier" check, so
    // maybeQuote must wrap them in quotes.
    assertThat(MongoRules.maybeQuote("EXPR$0"), is("'EXPR$0'"));
  }

  @Test void nameWithPunctuationQuoted() {
    assertThat(MongoRules.maybeQuote("a b"), is("'a b'"));
    assertThat(MongoRules.maybeQuote("a.b"), is("'a.b'"));
    assertThat(MongoRules.maybeQuote("a,b"), is("'a,b'"));
    assertThat(MongoRules.maybeQuote("a:b"), is("'a:b'"));
  }

  @Test void quoteEscapesBackslashAndApostrophe() {
    // Inside the quoted BSON literal a backslash or an apostrophe
    // would otherwise let the string terminate early and shift the
    // document boundaries; both must be escaped.
    assertThat(MongoRules.quote("a\\b"), is("'a\\\\b'"));
    assertThat(MongoRules.quote("a'b"), is("'a\\'b'"));
    assertThat(MongoRules.quote("a\\'b"), is("'a\\\\\\'b'"));
  }

  @Test void quoteTrailingBackslash() {
    // A trailing backslash must be doubled or it would otherwise
    // consume the closing apostrophe.
    assertThat(MongoRules.quote("a\\"), is("'a\\\\'"));
  }

  @Test void quoteApostropheOnly() {
    assertThat(MongoRules.quote("'"), is("'\\''"));
  }
}
