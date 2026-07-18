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

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link MongoRules}. */
class MongoRulesTest {

  /** A string without special characters is quoted verbatim. */
  @Test void testQuotePlain() {
    assertThat(MongoRules.quote("city"), is("'city'"));
    assertThat(MongoRules.quote("$city"), is("'$city'"));
  }

  /** A single quote in the value must not let it escape the quoted token,
   * otherwise an {@code ITEM} key or field name taken from the SQL statement
   * could inject additional stages into the pipeline handed to
   * {@code BsonDocument.parse}. */
  @Test void testQuoteEscapesEmbeddedQuote() {
    final String key = "x', injected: {$literal: 1}, y: 'z";
    final String pipeline = "{$project: {c: " + MongoRules.quote("$" + key) + "}}";
    final BsonDocument doc = BsonDocument.parse(pipeline);
    final BsonDocument project = doc.getDocument("$project");
    // The whole key stays a single string value; no extra keys are injected.
    assertThat(project.containsKey("injected"), is(false));
    assertThat(project.getString("c").getValue(), is("$" + key));
  }

  /** A backslash is escaped so it cannot consume the following quote. */
  @Test void testQuoteEscapesBackslash() {
    final BsonDocument doc =
        BsonDocument.parse("{f: " + MongoRules.quote("a\\") + "}");
    assertThat(doc.getString("f").getValue(), is("a\\"));
  }
}
