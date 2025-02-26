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

import org.apache.calcite.test.Unsafe;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.boolQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.matchesQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.rangeQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.termQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.termsQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuildersTest.JsonMatcher.hasJson;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Check that internal queries are correctly converted to ES search query (as
 * JSON).
 */
class QueryBuildersTest {

  /**
   * Test for simple scalar terms (boolean, int etc.)
   *
   */
  @Test void term() {
    assertThat(termQuery("foo", "bar"),
        hasJson("{\"term\":{\"foo\":\"bar\"}}"));
    assertThat(termQuery("bar", "foo"),
        hasJson("{\"term\":{\"bar\":\"foo\"}}"));
    assertThat(termQuery("foo", 'A'),
        hasJson("{\"term\":{\"foo\":\"A\"}}"));
    assertThat(termQuery("foo", true),
        hasJson("{\"term\":{\"foo\":true}}"));
    assertThat(termQuery("foo", false),
        hasJson("{\"term\":{\"foo\":false}}"));
    assertThat(termQuery("foo", (byte) 0),
        hasJson("{\"term\":{\"foo\":0}}"));
    assertThat(termQuery("foo", (long) 123),
        hasJson("{\"term\":{\"foo\":123}}"));
    assertThat(termQuery("foo", (short) 41),
        hasJson("{\"term\":{\"foo\":41}}"));
    assertThat(termQuery("foo", 42.42D),
        hasJson("{\"term\":{\"foo\":42.42}}"));
    assertThat(termQuery("foo", 1.1F),
        hasJson("{\"term\":{\"foo\":1.1}}"));
    assertThat(termQuery("foo", new BigDecimal(1)),
        hasJson("{\"term\":{\"foo\":1}}"));
    assertThat(termQuery("foo", new BigInteger("121")),
        hasJson("{\"term\":{\"foo\":121}}"));
    assertThat(termQuery("foo", new AtomicLong(111)),
        hasJson("{\"term\":{\"foo\":111}}"));
    assertThat(termQuery("foo", new AtomicInteger(222)),
        hasJson("{\"term\":{\"foo\":222}}"));
    assertThat(termQuery("foo", new AtomicBoolean(true)),
        hasJson("{\"term\":{\"foo\":true}}"));
  }

  @Test void terms() {
    assertThat(termsQuery("foo", Collections.emptyList()),
        hasJson("{\"terms\":{\"foo\":[]}}"));

    assertThat(termsQuery("bar", Collections.emptySet()),
        hasJson("{\"terms\":{\"bar\":[]}}"));

    assertThat(termsQuery("singleton", Collections.singleton(0)),
        hasJson("{\"terms\":{\"singleton\":[0]}}"));

    assertThat(termsQuery("foo", Collections.singleton(true)),
        hasJson("{\"terms\":{\"foo\":[true]}}"));

    assertThat(termsQuery("foo", Collections.singleton("bar")),
        hasJson("{\"terms\":{\"foo\":[\"bar\"]}}"));

    assertThat(termsQuery("foo", Collections.singletonList("bar")),
        hasJson("{\"terms\":{\"foo\":[\"bar\"]}}"));

    assertThat(termsQuery("foo", Arrays.asList(true, false)),
        hasJson("{\"terms\":{\"foo\":[true,false]}}"));

    assertThat(termsQuery("foo", Arrays.asList(1, 2, 3)),
        hasJson("{\"terms\":{\"foo\":[1,2,3]}}"));

    assertThat(termsQuery("foo", Arrays.asList(1.1, 2.2, 3.3)),
        hasJson("{\"terms\":{\"foo\":[1.1,2.2,3.3]}}"));
  }

  @Test void bool() {
    assertThat(boolQuery()
            .must(termQuery("foo", "bar")),
        hasJson("{\"bool\":{\"must\":{\"term\":{\"foo\":\"bar\"}}}}"));

    assertThat(boolQuery()
            .must(termQuery("f1", "v1"))
            .must(termQuery("f2", "v2")),
        hasJson("{\"bool\":{\"must\":[{\"term\":{\"f1\":\"v1\"}},"
            + "{\"term\":{\"f2\":\"v2\"}}]}}"));

    assertThat(boolQuery()
            .mustNot(termQuery("f1", "v1")),
        hasJson("{\"bool\":{\"must_not\":{\"term\":{\"f1\":\"v1\"}}}}"));
  }

  @Test void exists() {
    assertThat(QueryBuilders.existsQuery("foo"),
        hasJson("{\"exists\":{\"field\":\"foo\"}}"));
  }

  @Test void range() {
    assertThat(rangeQuery("f").lt(0),
        hasJson("{\"range\":{\"f\":{\"lt\":0}}}"));
    assertThat(rangeQuery("f").gt(0),
        hasJson("{\"range\":{\"f\":{\"gt\":0}}}"));
    assertThat(rangeQuery("f").gte(0),
        hasJson("{\"range\":{\"f\":{\"gte\":0}}}"));
    assertThat(rangeQuery("f").lte(0),
        hasJson("{\"range\":{\"f\":{\"lte\":0}}}"));
    assertThat(rangeQuery("f").gt(1).lt(2),
        hasJson("{\"range\":{\"f\":{\"gt\":1,\"lt\":2}}}"));
    assertThat(rangeQuery("f").lt(0).gt(11),
        hasJson("{\"range\":{\"f\":{\"gt\":11,\"lt\":0}}}"));
    assertThat(rangeQuery("f").gt(1).lte(2),
        hasJson("{\"range\":{\"f\":{\"gt\":1,\"lte\":2}}}"));
    assertThat(rangeQuery("f").gte(1).lte("zz"),
        hasJson("{\"range\":{\"f\":{\"gte\":1,\"lte\":\"zz\"}}}"));
    assertThat(rangeQuery("f").gte(1),
        hasJson("{\"range\":{\"f\":{\"gte\":1}}}"));
    assertThat(rangeQuery("f").gte("zz"),
        hasJson("{\"range\":{\"f\":{\"gte\":\"zz\"}}}"));
    assertThat(rangeQuery("f").gt("a").lt("z"),
        hasJson("{\"range\":{\"f\":{\"gt\":\"a\",\"lt\":\"z\"}}}"));
    assertThat(rangeQuery("f").gt(1).gt(2).gte(3),
        hasJson("{\"range\":{\"f\":{\"gte\":3}}}"));
    assertThat(rangeQuery("f").lt(1).lt(2).lte(3),
        hasJson("{\"range\":{\"f\":{\"lte\":3}}}"));
  }

  @Test void matchAll() {
    assertThat(QueryBuilders.matchAll(),
        hasJson("{\"match_all\":{}}"));
  }

  @Test void match() {
    assertThat(matchesQuery("foo", Collections.singleton("bar")),
        hasJson("{\"match\":{\"foo\":[\"bar\"]}}"));

    assertThat(matchesQuery("foo", Collections.singleton(true)),
        hasJson("{\"match\":{\"foo\":[true]}}"));
  }

  /** Matcher that succeeds if a
   * {@link org.apache.calcite.adapter.elasticsearch.QueryBuilders.QueryBuilder}
   * yields JSON that matches a given string. */
  static class JsonMatcher extends TypeSafeMatcher<QueryBuilders.QueryBuilder> {
    private final Matcher<String> matcher;

    protected JsonMatcher(Matcher<String> matcher) {
      super(QueryBuilders.QueryBuilder.class);
      this.matcher = matcher;
    }

    static Matcher<QueryBuilders.QueryBuilder> hasJson(String s) {
      return new JsonMatcher(is(s));
    }

    private static String toJson(QueryBuilders.QueryBuilder builder,
        ObjectMapper mapper) {
      try {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = mapper.getFactory().createGenerator(writer);
        builder.writeJson(gen);
        gen.flush();
        gen.close();
        return writer.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override protected boolean matchesSafely(QueryBuilders.QueryBuilder builder) {
      final String json = toJson(builder, new ObjectMapper());
      return Unsafe.matches(matcher, json);
    }

    @Override public void describeTo(Description description) {
      description.appendText("json ").appendDescriptionOf(matcher);
    }
  }
}
