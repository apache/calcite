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
package org.apache.calcite.adapter.gremlin;

import org.apache.calcite.test.CalciteAssert;

import com.google.common.io.Resources;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.apache.calcite.util.Benchmark.enabled;

public class GremlinAdapterTest {

  protected static final URL MODEL = GremlinAdapterTest.class.getResource("/gremlin.model.json");

  private CalciteAssert.AssertThat assertModel(String model) {
    // ensure that Schema from this instance is being used
    model = model.replace(GremlinAdapterTest.class.getName(), GremlinAdapterTest.class.getName());

    return CalciteAssert.that()
        .withModel(model);
  }

  private CalciteAssert.AssertThat assertModel(URL url) {
    Objects.requireNonNull(url, "url");
    try {
      return assertModel(Resources.toString(url, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }


  @Test void testSelect() {
    assertModel(MODEL)
        .query("SELECT STREAM * FROM INTTYPE.MOCKTABLE")
        .limit(2);
  }

  @Test void testFilterWithProject() {
    final String sql = "select cast(\"__time\" as timestamp) as \"__time\"\n"
        + "from \"wikipedia\"\n"
        + "where \"__time\" < '2015-10-12 00:00:00 UTC'";
    sql(sql, MODEL)
        .limit(2)
        .returnsUnordered("__time=2015-09-12 00:46:58",
            "__time=2015-09-12 00:47:00");
  }

  /** Creates a query against a data set given by a map. */
  private CalciteAssert.AssertQuery sql(String sql, URL url) {
    return CalciteAssert.that()
        .enable(enabled())
        .withModel(url)
        .query(sql);
  }


}
