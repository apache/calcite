/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import mondrian.test.data.FoodMartData;

import org.eigenbase.util.IntegerIntervalSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

/**
 * Test case that runs the FoodMart reference queries.
 */
@RunWith(Parameterized.class)
public class FoodmartTest {
  private static final Map<Integer, FoodmartQuery> queries =
      new LinkedHashMap<Integer, FoodmartQuery>();

  private static final String DISABLED_IDS =
      ","
      + "-58," //  "Unknown column 't0.desc' in 'field list'" related to VALUES
      + "-83," // "java.lang.OutOfMemoryError: Java heap space"
      + "-368"; // multi VALUES

  private final FoodmartQuery query;

  @Parameterized.Parameters(name = "{index}: foodmart({0})={1}")
  public static List<Object[]> getSqls() throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    final InputStream inputStream = new FoodMartData().getQueries();
    FoodmartRoot root = mapper.readValue(inputStream, FoodmartRoot.class);
    final List<Object[]> list = new ArrayList<Object[]>();
      final String idList = System.getProperty("optiq.ids");
    for (FoodmartQuery query : root.queries) {
      queries.put(query.id, query);
    }
    if (idList != null) {
      for (Integer id : IntegerIntervalSet.of(idList /* + DISABLED_IDS */)) {
        final FoodmartQuery query1 = queries.get(id);
        if (query1 != null) {
          list.add(new Object[] {id, query1.sql});
        }
      }
    } else {
      for (FoodmartQuery query1 : queries.values()) {
        list.add(new Object[]{query1.id, query1.sql});
      }
    }
    return list;
  }

  public FoodmartTest(int id, String sql) {
    this.query = queries.get(id);
    assert query.id == id : id + ":" + query.id;
  }

  @Test(timeout = 60000)
  public void test() {
    try {
      OptiqAssert.assertThat()
          .withModel(JdbcTest.FOODMART_MODEL)
          // .with(OptiqAssert.Config.FOODMART_CLONE)
          .withSchema("foodmart")
          .query(query.sql)
          .runs();
    } catch (Throwable e) {
      throw new RuntimeException("Test failed, id=" + query.id, e);
    }
  }

  public static class FoodmartRoot {
    public final List<FoodmartQuery> queries = new ArrayList<FoodmartQuery>();
  }

  public static class FoodmartQuery {
    public int id;
    public String sql;
    public final List<FoodmartColumn> columns = new ArrayList<FoodmartColumn>();
    public final List<List> rows = new ArrayList<List>();
  }

  public static class FoodmartColumn {
    public String name;
    public String type;
  }

}

// End FoodmartTest.java
