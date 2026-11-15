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
package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for the escaping helpers in {@link ElasticsearchRules} that
 * {@link ElasticsearchProject} uses when field names are concatenated into
 * the JSON search request.
 *
 * <p>Field names and column aliases are arbitrary SQL-controlled strings;
 * without escaping, a value containing {@code "} or {@code \} would close
 * the surrounding JSON string and let sibling top-level keys leak into
 * the request body, or be executed as script code inside a scripted
 * field. The tests exercise the helpers directly and do not require a
 * running Elasticsearch cluster.
 */
class ElasticsearchRulesTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test void quoteWrapsInDoubleQuotes() {
    // quote() is deliberately raw: callers that embed the value inside a
    // JSON body route it through escapeSpecialSymbols first.
    assertThat(ElasticsearchRules.quote("field"), is("\"field\""));
    assertThat(ElasticsearchRules.quote(""), is("\"\""));
  }

  @Test void plainNamesUnchangedByEscaping() {
    assertThat(ElasticsearchRules.escapeJsonString("city"), is("city"));
    assertThat(ElasticsearchRules.escapeJsonString("b.a"), is("b.a"));
  }

  @Test void quotesAndBackslashesEscaped() {
    // Double quote and backslash are the two characters that would let
    // the value terminate the surrounding JSON string.
    assertThat(ElasticsearchRules.escapeJsonString("a\"b"), is("a\\\"b"));
    assertThat(ElasticsearchRules.escapeJsonString("a\\b"), is("a\\\\b"));
    assertThat(ElasticsearchRules.escapeJsonString("a\\"), is("a\\\\"));
    // Order matters: escape backslash first, then double quote, so
    // "a\"b" doesn't become "a\\\"b" with a lost closing quote.
    assertThat(ElasticsearchRules.escapeJsonString("a\\\"b"),
        is("a\\\\\\\"b"));
  }

  @Test void namesWithJsonMetacharactersStayASingleSourceField() throws Exception {
    // A name that tries to close the "_source" array and add a sibling
    // "query" key must stay confined to the string, so the emitted JSON
    // remains a single top-level object with one member.
    final String awkward =
        "a\"], \"query\": {\"bool\": {\"filter\": {\"script\": "
            + "{\"script\": \"while(true){}\"}}}}, \"docvalue_fields\": [\"b";
    final String json = "{\"_source\" : ["
        + ElasticsearchRules.quote(
            ElasticsearchRules.escapeJsonString(awkward)) + "]}";
    final JsonNode node = mapper.readTree(json);
    final int rootMembers = node.size();
    final int sourceEntries = node.get("_source").size();
    assertThat(rootMembers, is(1));
    assertThat(sourceEntries, is(1));
    assertThat(node.get("_source").get(0).asText(), is(awkward));
  }

  @Test void simplePathsKeepDotNotation() {
    // Historical dot notation stays in place for identifier-shaped paths
    // so existing scripted fields render exactly as before.
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "city"),
        is("params._source.city"));
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "b.a"),
        is("params._source.b.a"));
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "loc[0]"),
        is("params._source.loc[0]"));
  }

  @Test void nonIdentifierPathsBecomeMapSubscripts() {
    // Any path that does not fit the historical dot notation is rendered
    // via map subscripts so the segments stay data.
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "@timestamp"),
        is("params._source['@timestamp']"));
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "we ird[3]"),
        is("params._source['we ird'][3]"));
    assertThat(ElasticsearchRules.scriptedFieldAccess("params._source", "a.b c"),
        is("params._source['a']['b c']"));
  }

  @Test void fieldNameWithScriptPunctuationIsSubscripted() {
    // A field name that contains {@code '} or {@code ;} would otherwise
    // be spliced into script source and parsed as Painless/Groovy code;
    // the map-subscript form quotes those characters so the whole name
    // stays a script string literal.
    assertThat(
        ElasticsearchRules.scriptedFieldAccess("params._source",
            "a'; long x=0; for(int i=0;i<10;i++){x+=i}; '"),
        is("params._source['a\\'; long x=0; for(int i=0;i<10;i++){x+=i}; \\'']"));
  }

  @Test void scriptFieldEntryStaysWellFormedJsonForAwkwardName() throws Exception {
    // End-to-end: an entry built the way ElasticsearchProject builds one
    // must still parse as a single-member JSON object with the value
    // recovered exactly.
    final String awkward = "a\"}}, \"other\": {\"script\": \"boom";
    final String entry =
        ElasticsearchRules.quote(ElasticsearchRules.escapeJsonString(awkward))
            + ":{\"script\": "
            + ElasticsearchRules.quote(
                ElasticsearchRules.escapeJsonString(
                    ElasticsearchRules.scriptedFieldAccess("params._source",
                        awkward)))
            + "}";
    final JsonNode node = mapper.readTree("{\"script_fields\": {" + entry + "}}");
    final int rootMembers = node.size();
    final int scriptFieldsMembers = node.get("script_fields").size();
    assertThat(rootMembers, is(1));
    assertThat(scriptFieldsMembers, is(1));
    assertThat(node.get("script_fields").get(awkward).get("script").asText(),
        is("params._source['a\"}}, \"other\": {\"script\": \"boom']"));
  }
}
