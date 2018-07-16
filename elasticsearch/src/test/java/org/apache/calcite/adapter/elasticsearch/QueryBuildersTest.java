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

import static org.junit.Assert.assertEquals;

/**
 * Check that internal queries are correctly converted to ES search query (as JSON)
 */
public class QueryBuildersTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void term() throws Exception {
    assertEquals("{\"term\":{\"foo\":\"bar\"}}",
        toJson(QueryBuilders.termQuery("foo", "bar")));
    assertEquals("{\"term\":{\"foo\":true}}",
        toJson(QueryBuilders.termQuery("foo", true)));
    assertEquals("{\"term\":{\"foo\":false}}",
        toJson(QueryBuilders.termQuery("foo", false)));
    assertEquals("{\"term\":{\"foo\":123}}",
        toJson(QueryBuilders.termQuery("foo", (long) 123)));
    assertEquals("{\"term\":{\"foo\":41}}",
        toJson(QueryBuilders.termQuery("foo", (short) 41)));
    assertEquals("{\"term\":{\"foo\":42.42}}",
        toJson(QueryBuilders.termQuery("foo", 42.42D)));
    assertEquals("{\"term\":{\"foo\":1.1}}",
        toJson(QueryBuilders.termQuery("foo", 1.1F)));
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
