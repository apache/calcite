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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Check that internal queries are correctly converted to ES search query (as JSON)
 */
public class QueryBuildersTest {

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Test for simple scalar terms (boolean, int etc.)
   * @throws Exception not expected
   */
  @Test
  public void term() throws Exception {
    assertEquals("{\"term\":{\"foo\":\"bar\"}}",
        toJson(QueryBuilders.termQuery("foo", "bar")));
    assertEquals("{\"term\":{\"bar\":\"foo\"}}",
        toJson(QueryBuilders.termQuery("bar", "foo")));
    assertEquals("{\"term\":{\"foo\":\"A\"}}",
        toJson(QueryBuilders.termQuery("foo", 'A')));
    assertEquals("{\"term\":{\"foo\":true}}",
        toJson(QueryBuilders.termQuery("foo", true)));
    assertEquals("{\"term\":{\"foo\":false}}",
        toJson(QueryBuilders.termQuery("foo", false)));
    assertEquals("{\"term\":{\"foo\":0}}",
        toJson(QueryBuilders.termQuery("foo", (byte) 0)));
    assertEquals("{\"term\":{\"foo\":123}}",
        toJson(QueryBuilders.termQuery("foo", (long) 123)));
    assertEquals("{\"term\":{\"foo\":41}}",
        toJson(QueryBuilders.termQuery("foo", (short) 41)));
    assertEquals("{\"term\":{\"foo\":42.42}}",
        toJson(QueryBuilders.termQuery("foo", 42.42D)));
    assertEquals("{\"term\":{\"foo\":1.1}}",
        toJson(QueryBuilders.termQuery("foo", 1.1F)));
    assertEquals("{\"term\":{\"foo\":1}}",
        toJson(QueryBuilders.termQuery("foo", new BigDecimal(1))));
    assertEquals("{\"term\":{\"foo\":121}}",
        toJson(QueryBuilders.termQuery("foo", new BigInteger("121"))));
    assertEquals("{\"term\":{\"foo\":111}}",
        toJson(QueryBuilders.termQuery("foo", new AtomicLong(111))));
    assertEquals("{\"term\":{\"foo\":222}}",
        toJson(QueryBuilders.termQuery("foo", new AtomicInteger(222))));
    assertEquals("{\"term\":{\"foo\":true}}",
        toJson(QueryBuilders.termQuery("foo", new AtomicBoolean(true))));
  }

  @Test
  public void terms() throws Exception {
    assertEquals("{\"terms\":{\"foo\":[]}}",
        toJson(QueryBuilders.termsQuery("foo", Collections.emptyList())));

    assertEquals("{\"terms\":{\"bar\":[]}}",
        toJson(QueryBuilders.termsQuery("bar", Collections.emptySet())));

    assertEquals("{\"terms\":{\"singleton\":[0]}}",
        toJson(QueryBuilders.termsQuery("singleton", Collections.singleton(0))));

    assertEquals("{\"terms\":{\"foo\":[true]}}",
        toJson(QueryBuilders.termsQuery("foo", Collections.singleton(true))));

    assertEquals("{\"terms\":{\"foo\":[\"bar\"]}}",
        toJson(QueryBuilders.termsQuery("foo", Collections.singleton("bar"))));

    assertEquals("{\"terms\":{\"foo\":[\"bar\"]}}",
        toJson(QueryBuilders.termsQuery("foo", Collections.singletonList("bar"))));

    assertEquals("{\"terms\":{\"foo\":[true,false]}}",
        toJson(QueryBuilders.termsQuery("foo", Arrays.asList(true, false))));

    assertEquals("{\"terms\":{\"foo\":[1,2,3]}}",
        toJson(QueryBuilders.termsQuery("foo", Arrays.asList(1, 2, 3))));

    assertEquals("{\"terms\":{\"foo\":[1.1,2.2,3.3]}}",
        toJson(QueryBuilders.termsQuery("foo", Arrays.asList(1.1, 2.2, 3.3))));
  }

  @Test
  public void boolQuery() throws Exception {
    QueryBuilders.QueryBuilder q1 = QueryBuilders.boolQuery()
        .must(QueryBuilders.termQuery("foo", "bar"));

    assertEquals("{\"bool\":{\"must\":{\"term\":{\"foo\":\"bar\"}}}}",
        toJson(q1));

    QueryBuilders.QueryBuilder q2 = QueryBuilders.boolQuery()
        .must(QueryBuilders.termQuery("f1", "v1")).must(QueryBuilders.termQuery("f2", "v2"));

    assertEquals("{\"bool\":{\"must\":[{\"term\":{\"f1\":\"v1\"}},{\"term\":{\"f2\":\"v2\"}}]}}",
        toJson(q2));

    QueryBuilders.QueryBuilder q3 = QueryBuilders.boolQuery()
        .mustNot(QueryBuilders.termQuery("f1", "v1"));

    assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"f1\":\"v1\"}}}}",
        toJson(q3));

  }

  @Test
  public void exists() throws Exception {
    assertEquals("{\"exists\":{\"field\":\"foo\"}}",
        toJson(QueryBuilders.existsQuery("foo")));
  }

  @Test
  public void range() throws Exception {
    assertEquals("{\"range\":{\"f\":{\"lt\":0}}}",
        toJson(QueryBuilders.rangeQuery("f").lt(0)));
    assertEquals("{\"range\":{\"f\":{\"gt\":0}}}",
        toJson(QueryBuilders.rangeQuery("f").gt(0)));
    assertEquals("{\"range\":{\"f\":{\"gte\":0}}}",
        toJson(QueryBuilders.rangeQuery("f").gte(0)));
    assertEquals("{\"range\":{\"f\":{\"lte\":0}}}",
        toJson(QueryBuilders.rangeQuery("f").lte(0)));
    assertEquals("{\"range\":{\"f\":{\"gt\":1,\"lt\":2}}}",
        toJson(QueryBuilders.rangeQuery("f").gt(1).lt(2)));
    assertEquals("{\"range\":{\"f\":{\"gt\":11,\"lt\":0}}}",
        toJson(QueryBuilders.rangeQuery("f").lt(0).gt(11)));
    assertEquals("{\"range\":{\"f\":{\"gt\":1,\"lte\":2}}}",
        toJson(QueryBuilders.rangeQuery("f").gt(1).lte(2)));
    assertEquals("{\"range\":{\"f\":{\"gte\":1,\"lte\":\"zz\"}}}",
        toJson(QueryBuilders.rangeQuery("f").gte(1).lte("zz")));
    assertEquals("{\"range\":{\"f\":{\"gte\":1}}}",
        toJson(QueryBuilders.rangeQuery("f").gte(1)));
    assertEquals("{\"range\":{\"f\":{\"gte\":\"zz\"}}}",
        toJson(QueryBuilders.rangeQuery("f").gte("zz")));
    assertEquals("{\"range\":{\"f\":{\"gt\":\"a\",\"lt\":\"z\"}}}",
        toJson(QueryBuilders.rangeQuery("f").gt("a").lt("z")));
    assertEquals("{\"range\":{\"f\":{\"gte\":3}}}",
        toJson(QueryBuilders.rangeQuery("f").gt(1).gt(2).gte(3)));
    assertEquals("{\"range\":{\"f\":{\"lte\":3}}}",
        toJson(QueryBuilders.rangeQuery("f").lt(1).lt(2).lte(3)));
  }

  @Test
  public void matchAll() throws IOException {
    assertEquals("{\"match_all\":{}}",
        toJson(QueryBuilders.matchAll()));
  }

  private String toJson(QueryBuilders.QueryBuilder builder) throws IOException {
    StringWriter writer = new StringWriter();
    JsonGenerator gen = mapper.getFactory().createGenerator(writer);
    builder.writeJson(gen);
    gen.flush();
    gen.close();
    return writer.toString();
  }
}

// End QueryBuildersTest.java
