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
package org.apache.calcite.test;

import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests for the {@code org.apache.calcite.adapter.elasticsearch} package.
 *
 * <p>Before calling this test, you need to populate Elasticsearch with the "zips"
 * data set (as described in howto.md).
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/test-dataset
 * cd test-dataset
 * mvn install
 * </code></blockquote>
 *
 * This will create a virtual machine with Elasticsearch and test dataset.
 */
public class ElasticsearchAdapterIT {
  /**
   * Whether to run Elasticsearch tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.elasticsearch=false} on the Java command line.
   */
  public static final boolean ENABLED = Util.getBooleanProperty("calcite.test.elasticsearch",
    true);

  /** Connection factory based on the "zips-es" model. */
  public static final ImmutableMap<String, String> ZIPS = ImmutableMap.of("model",
    ElasticsearchAdapterIT.class.getResource("/elasticsearch-zips-model.json").getPath());

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  /** Returns a function that checks that a particular Elasticsearch pipeline is
   * generated to implement a query. */
  private static Function<List, Void> elasticsearchChecker(final String... strings) {
    return new Function<List, Void>() {
      @Nullable
      @Override public Void apply(@Nullable List actual) {
        Object[] actualArray = actual == null || actual.isEmpty() ? null
          : ((List) actual.get(0)).toArray();
        CalciteAssert.assertArrayEqual("expected Elasticsearch query not found", strings,
          actualArray);
        return null;
      }
    };
  }

  @Test public void testSort() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select * from zips order by \"state\"")
      .returnsCount(10)
      .explainContains("PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], dir0=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], id=[CAST(ITEM($0, 'id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "      ElasticsearchTableScan(table=[[elasticsearch_raw, zips]])");
  }

  @Test public void testSortLimit() {
    CalciteAssert.that()
      .with(ZIPS)
      .query("select \"state\", \"id\" from zips\n"
        + "order by \"state\", \"id\" offset 2 rows fetch next 3 rows only")
      .returns("state=AK; id=99503\n"
        + "state=AK; id=99504\n"
        + "state=AK; id=99505\n")
      .queryContains(
        elasticsearchChecker(
          "\"fields\" : [\"state\", \"id\"], \"script_fields\": {}",
          "\"sort\": [ {\"state\": \"asc\"}, {\"id\": \"asc\"}]",
          "\"from\": 2",
          "\"size\": 3"));
  }

  @Test public void testOffsetLimit() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"id\" from zips\n"
        + "offset 2 fetch next 3 rows only")
      .runs()
      .queryContains(
        elasticsearchChecker(
          "\"from\": 2",
          "\"size\": 3",
          "\"fields\" : [\"state\", \"id\"], \"script_fields\": {}"));
  }

  @Test public void testLimit() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"id\" from zips\n"
        + "fetch next 3 rows only")
      .runs()
      .queryContains(
        elasticsearchChecker(
          "\"size\": 3",
          "\"fields\" : [\"state\", \"id\"], \"script_fields\": {}"));
  }

  @Test public void testFilterSort() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select * from zips\n"
        + "where \"city\" = 'SPRINGFIELD' and \"id\" >= '70000'\n"
        + "order by \"state\", \"id\"")
      .returns(""
        + "city=SPRINGFIELD; longitude=-92.54567; latitude=35.274879; pop=752; state=AR; id=72157\n"
        + "city=SPRINGFIELD; longitude=-102.617322; latitude=37.406727; pop=1992; state=CO; id=81073\n"
        + "city=SPRINGFIELD; longitude=-90.577479; latitude=30.415738; pop=5597; state=LA; id=70462\n"
        + "city=SPRINGFIELD; longitude=-123.015259; latitude=44.06106; pop=32384; state=OR; id=97477\n"
        + "city=SPRINGFIELD; longitude=-122.917108; latitude=44.056056; pop=27521; state=OR; id=97478\n")
      .queryContains(
        elasticsearchChecker("\"query\" : {\"constant_score\":{\"filter\":{\"bool\":"
          + "{\"must\":[{\"term\":{\"city\":\"springfield\"}},{\"range\":{\"id\":{\"gte\":\"70000\"}}}]}}}}",
          "\"fields\" : [\"city\", \"pop\", \"state\", \"id\"], \"script_fields\": {\"longitude\":{\"script\":\"_source.loc[0]\"}, \"latitude\":{\"script\":\"_source.loc[1]\"}}",
          "\"sort\": [ {\"state\": \"asc\"}, {\"id\": \"asc\"}]"))
      .explainContains("PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], id=[CAST(ITEM($0, 'id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "      ElasticsearchFilter(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'SPRINGFIELD'), >=(CAST(ITEM($0, 'id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '70000'))])\n"
        + "        ElasticsearchTableScan(table=[[elasticsearch_raw, zips]])");
  }

  @Test public void testFilterSortDesc() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select * from zips\n"
        + "where \"pop\" BETWEEN 20000 AND 20100\n"
        + "order by \"state\" desc, \"pop\"")
      .limit(4)
      .returns(""
        + "city=SHERIDAN; longitude=-106.964795; latitude=44.78486; pop=20025; state=WY; id=82801\n"
        + "city=MOUNTLAKE TERRAC; longitude=-122.304036; latitude=47.793061; pop=20059; state=WA; id=98043\n"
        + "city=FALMOUTH; longitude=-77.404537; latitude=38.314557; pop=20039; state=VA; id=22405\n"
        + "city=FORT WORTH; longitude=-97.318409; latitude=32.725551; pop=20012; state=TX; id=76104\n");
  }

  @Test public void testFilterRedundant() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select * from zips where \"state\" > 'CA' and \"state\" < 'AZ' and \"state\" = 'OK'")
      .runs()
      .queryContains(
        elasticsearchChecker("\"query\" : {\"constant_score\":{\"filter\":{\"bool\":"
          + "{\"must\":[{\"term\":{\"state\":\"ok\"}}]}}}}",
          "\"fields\" : [\"city\", \"pop\", \"state\", \"id\"], \"script_fields\": {\"longitude\":{\"script\":\"_source.loc[0]\"}, \"latitude\":{\"script\":\"_source.loc[1]\"}}"));
  }

  @Test public void testInPlan() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select * from zips where \"pop\" in (20012, 15590)")
      .returns("city=COVINA; longitude=-117.884285; latitude=34.08596; pop=15590; "
        + "state=CA; id=91723\n"
        + "city=ARLINGTON; longitude=-97.091987; latitude=32.654752; pop=15590; state=TX; id=76018\n"
        + "city=CROFTON; longitude=-76.680166; latitude=39.011163; pop=15590; state=MD; id=21114\n"
        + "city=FORT WORTH; longitude=-97.318409; latitude=32.725551; pop=20012; state=TX; id=76104\n"
        + "city=DINUBA; longitude=-119.39087; latitude=36.534931; pop=20012; state=CA; id=93618\n")
      .queryContains(
        elasticsearchChecker("\"query\" : {\"constant_score\":{\"filter\":{\"bool\":{\"should\":"
          + "[{\"bool\":{\"must\":[{\"term\":{\"pop\":20012}}]}},{\"bool\":{\"must\":[{\"term\":"
          + "{\"pop\":15590}}]}}]}}}}",
          "\"fields\" : [\"city\", \"pop\", \"state\", \"id\"], \"script_fields\": {\"longitude\":{\"script\":\"_source.loc[0]\"}, \"latitude\":{\"script\":\"_source.loc[1]\"}}"));
  }

  @Test public void testZips() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"city\" from zips")
      .returnsCount(10);
  }

  @Test public void testProject() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"city\", 0 as \"zero\" from zips order by \"state\", \"city\"")
      .limit(2)
      .returns("state=AK; city=ELMENDORF AFB; zero=0\n"
        + "state=AK; city=EIELSON AFB; zero=0\n")
      .queryContains(
        elasticsearchChecker("\"sort\": [ {\"state\": \"asc\"}, {\"city\": \"asc\"}]",
          "\"fields\" : [\"state\", \"city\"], \"script_fields\": {\"zero\":{\"script\": \"0\"}}"));
  }

  @Test public void testFilter() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"city\" from zips where \"state\" = 'CA'")
      .limit(2)
      .returns("state=CA; city=LOS ANGELES\n"
        + "state=CA; city=LOS ANGELES\n")
      .explainContains("PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchProject(state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "    ElasticsearchFilter(condition=[=(CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
        + "      ElasticsearchTableScan(table=[[elasticsearch_raw, zips]])");
  }

  @Test public void testFilterReversed() {
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"city\" from zips where 'WI' < \"state\"")
      .limit(2)
      .returns("state=WV; city=WELCH\n"
        + "state=WV; city=HANOVER\n");
    CalciteAssert.that()
      .enable(enabled())
      .with(ZIPS)
      .query("select \"state\", \"city\" from zips where \"state\" > 'WI'")
      .limit(2)
      .returns("state=WV; city=WELCH\n"
        + "state=WV; city=HANOVER\n");
  }
}

// End ElasticsearchAdapterIT.java
