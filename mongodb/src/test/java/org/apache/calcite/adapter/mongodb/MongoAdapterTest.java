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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MongoAssertions;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import net.hydromatic.foodmart.data.json.FoodmartJson;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Testing mongo adapter functionality. By default runs with
 * <a href="https://github.com/fakemongo/fongo">Fongo</a> unless {@code IT} maven profile is enabled
 * (via {@code $ mvn -Pit install}).
 *
 * @see MongoDatabasePolicy
 */
public class MongoAdapterTest implements SchemaFactory {

  /** Connection factory based on the "mongo-zips" model. */
  protected static final URL MODEL = MongoAdapterTest.class.getResource("/mongo-model.json");

  /** Number of records in local file */
  protected static final int ZIPS_SIZE = 149;

  @ClassRule
  public static final MongoDatabasePolicy POLICY = MongoDatabasePolicy.create();

  private static MongoSchema schema;

  @BeforeClass
  public static void setUp() throws Exception {
    MongoDatabase database = POLICY.database();

    populate(database.getCollection("zips"), MongoAdapterTest.class.getResource("/zips-mini.json"));
    populate(database.getCollection("store"), FoodmartJson.class.getResource("/store.json"));
    populate(database.getCollection("warehouse"),
        FoodmartJson.class.getResource("/warehouse.json"));

    // Manually insert data for data-time test.
    MongoCollection<BsonDocument> datatypes =  database.getCollection("datatypes")
        .withDocumentClass(BsonDocument.class);
    if (datatypes.countDocuments() > 0) {
      datatypes.deleteMany(new BsonDocument());
    }

    BsonDocument doc = new BsonDocument();
    Instant instant = LocalDate.of(2012, 9, 5).atStartOfDay(ZoneOffset.UTC).toInstant();
    doc.put("date", new BsonDateTime(instant.toEpochMilli()));
    doc.put("value", new BsonInt32(1231));
    doc.put("ownerId", new BsonString("531e7789e4b0853ddb861313"));
    datatypes.insertOne(doc);

    schema = new MongoSchema(database);
  }

  private static void populate(MongoCollection<Document> collection, URL resource)
      throws IOException {
    Objects.requireNonNull(collection, "collection");

    if (collection.countDocuments() > 0) {
      // delete any existing documents (run from a clean set)
      collection.deleteMany(new BsonDocument());
    }

    MongoCollection<BsonDocument> bsonCollection = collection.withDocumentClass(BsonDocument.class);
    Resources.readLines(resource, StandardCharsets.UTF_8, new LineProcessor<Void>() {
      @Override public boolean processLine(String line) throws IOException {
        bsonCollection.insertOne(BsonDocument.parse(line));
        return true;
      }

      @Override public Void getResult() {
        return null;
      }
    });
  }

  /**
   *  Returns always the same schema to avoid initialization costs.
   */
  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    return schema;
  }

  private CalciteAssert.AssertThat assertModel(String model) {
    // ensure that Schema from this instance is being used
    model = model.replace(MongoSchemaFactory.class.getName(), MongoAdapterTest.class.getName());

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

  @Test public void testSort() {
    assertModel(MODEL)
        .query("select * from zips order by state")
        .returnsCount(ZIPS_SIZE)
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoSort(sort0=[$4], dir0=[ASC])\n"
            + "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2)], ID=[CAST(ITEM($0, '_id')):VARCHAR(5)])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testSortLimit() {
    assertModel(MODEL)
        .query("select state, id from zips\n"
            + "order by state, id offset 2 rows fetch next 3 rows only")
        .returnsOrdered("STATE=AK; ID=99801",
            "STATE=AL; ID=35215",
            "STATE=AL; ID=35401")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1, ID: 1}}",
                "{$skip: 2}",
                "{$limit: 3}"));
  }

  @Test public void testOffsetLimit() {
    assertModel(MODEL)
        .query("select state, id from zips\n"
            + "offset 2 fetch next 3 rows only")
        .runs()
        .queryContains(
            mongoChecker(
                "{$skip: 2}",
                "{$limit: 3}",
                "{$project: {STATE: '$state', ID: '$_id'}}"));
  }

  @Test public void testLimit() {
    assertModel(MODEL)
        .query("select state, id from zips\n"
            + "fetch next 3 rows only")
        .runs()
        .queryContains(
            mongoChecker(
                "{$limit: 3}",
                "{$project: {STATE: '$state', ID: '$_id'}}"));
  }

  @Ignore
  @Test public void testFilterSort() {
    // LONGITUDE and LATITUDE are null because of CALCITE-194.
    Util.discard(Bug.CALCITE_194_FIXED);
    assertModel(MODEL)
        .query("select * from zips\n"
            + "where city = 'SPRINGFIELD' and id >= '70000'\n"
            + "order by state, id")
        .returns(""
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=752; STATE=AR; ID=72157\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=1992; STATE=CO; ID=81073\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=5597; STATE=LA; ID=70462\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=32384; STATE=OR; ID=97477\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=27521; STATE=OR; ID=97478\n")
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    city: \"SPRINGFIELD\",\n"
                    + "    _id: {\n"
                    + "      $gte: \"70000\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1, ID: 1}}"))
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoSort(sort0=[$4], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
            + "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2)], ID=[CAST(ITEM($0, '_id')):VARCHAR(5)])\n"
            + "      MongoFilter(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20), 'SPRINGFIELD'), >=(CAST(ITEM($0, '_id')):VARCHAR(5), '70000'))])\n"
            + "        MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testFilterSortDesc() {
    assertModel(MODEL)
        .query("select * from zips\n"
            + "where pop BETWEEN 45000 AND 46000\n"
            + "order by state desc, pop")
        .limit(4)
        .returnsOrdered(
            "CITY=BECKLEY; LONGITUDE=null; LATITUDE=null; POP=45196; STATE=WV; ID=25801",
            "CITY=ROCKERVILLE; LONGITUDE=null; LATITUDE=null; POP=45328; STATE=SD; ID=57701",
            "CITY=PAWTUCKET; LONGITUDE=null; LATITUDE=null; POP=45442; STATE=RI; ID=02860",
            "CITY=LAWTON; LONGITUDE=null; LATITUDE=null; POP=45542; STATE=OK; ID=73505");
  }

  @Ignore("broken; [CALCITE-2115] is logged to fix it")
  @Test public void testUnionPlan() {
    assertModel(MODEL)
        .query("select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains("PLAN=EnumerableUnion(all=[true])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1997]])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1998]])")
        .limit(2)
        .returns(
            MongoAssertions.checkResultUnordered(
                "product_id=337", "product_id=1512"));
  }

  @Ignore(
      "java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Double")
  @Test public void testFilterUnionPlan() {
    assertModel(MODEL)
        .query("select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .runs();
  }

  /** Tests that we don't generate multiple constraints on the same column.
   * MongoDB doesn't like it. If there is an '=', it supersedes all other
   * operators. */
  @Test public void testFilterRedundant() {
    assertModel(MODEL)
        .query(
            "select * from zips where state > 'CA' and state < 'AZ' and state = 'OK'")
        .runs()
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"state\": \"OK\"\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}"));
  }

  @Test public void testSelectWhere() {
    assertModel(MODEL)
        .query(
            "select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoProject(warehouse_id=[CAST(ITEM($0, 'warehouse_id')):DOUBLE], warehouse_state_province=[CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20)])\n"
            + "    MongoFilter(condition=[=(CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20), 'CA')])\n"
            + "      MongoTableScan(table=[[mongo_raw, warehouse]])")
        .returns(
            MongoAssertions.checkResultUnordered(
                "warehouse_id=6; warehouse_state_province=CA",
                "warehouse_id=7; warehouse_state_province=CA",
                "warehouse_id=14; warehouse_state_province=CA",
                "warehouse_id=24; warehouse_state_province=CA"))
        .queryContains(
            // Per https://issues.apache.org/jira/browse/CALCITE-164,
            // $match must occur before $project for good performance.
            mongoChecker(
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"warehouse_state_province\": \"CA\"\n"
                    + "  }\n"
                    + "}",
                "{$project: {warehouse_id: 1, warehouse_state_province: 1}}"));
  }

  @Test public void testInPlan() {
    assertModel(MODEL)
        .query("select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .returns(
            MongoAssertions.checkResultUnordered(
                "store_id=1; store_name=Store 1",
                "store_id=3; store_name=Store 3",
                "store_id=7; store_name=Store 7",
                "store_id=10; store_name=Store 10",
                "store_id=11; store_name=Store 11",
                "store_id=15; store_name=Store 15",
                "store_id=16; store_name=Store 16",
                "store_id=24; store_name=Store 24"))
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"$or\": [\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 1\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 10\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 11\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 15\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 16\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 24\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 3\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"store_name\": \"Store 7\"\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}",
                "{$project: {store_id: 1, store_name: 1}}"));
  }

  /** Simple query based on the "mongo-zips" model. */
  @Test public void testZips() {
    assertModel(MODEL)
        .query("select state, city from zips")
        .returnsCount(ZIPS_SIZE);
  }

  @Test public void testCountGroupByEmpty() {
    assertModel(MODEL)
        .query("select count(*) from zips")
        .returns(String.format(Locale.ROOT, "EXPR$0=%d\n", ZIPS_SIZE))
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "    MongoTableScan(table=[[mongo_raw, zips]])")
        .queryContains(
            mongoChecker(
                "{$group: {_id: {}, 'EXPR$0': {$sum: 1}}}"));
  }

  @Test public void testCountGroupByEmptyMultiplyBy2() {
    assertModel(MODEL)
        .query("select count(*)*2 from zips")
        .returns(String.format(Locale.ROOT, "EXPR$0=%d\n", ZIPS_SIZE * 2))
        .queryContains(
            mongoChecker(
                "{$group: {_id: {}, _0: {$sum: 1}}}",
                "{$project: {'EXPR$0': {$multiply: ['$_0', {$literal: 2}]}}}"));
  }

  @Test public void testGroupByOneColumnNotProjected() {
    assertModel(MODEL)
        .query("select count(*) from zips group by state order by 1")
        .limit(2)
        .returnsUnordered("EXPR$0=2",
            "EXPR$0=2")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', 'EXPR$0': {$sum: 1}}}",
                "{$project: {STATE: '$_id', 'EXPR$0': '$EXPR$0'}}",
                "{$project: {'EXPR$0': 1}}",
                "{$sort: {EXPR$0: 1}}"));
  }

  @Test public void testGroupByOneColumn() {
    assertModel(MODEL)
        .query(
            "select state, count(*) as c from zips group by state order by state")
        .limit(3)
        .returns("STATE=AK; C=3\nSTATE=AL; C=3\nSTATE=AR; C=3\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByOneColumnReversed() {
    // Note extra $project compared to testGroupByOneColumn.
    assertModel(MODEL)
        .query(
            "select count(*) as c, state from zips group by state order by state")
        .limit(2)
        .returns("C=3; STATE=AK\nC=3; STATE=AL\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{$project: {C: 1, STATE: 1}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByAvg() {
    assertModel(MODEL)
        .query(
            "select state, avg(pop) as a from zips group by state order by state")
        .limit(2)
        .returns("STATE=AK; A=26856\nSTATE=AL; A=43383\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', POP: '$pop'}}",
                "{$group: {_id: '$STATE', A: {$avg: '$POP'}}}",
                "{$project: {STATE: '$_id', A: '$A'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByAvgSumCount() {
    assertModel(MODEL)
        .query(
            "select state, avg(pop) as a, sum(pop) as s, count(pop) as c from zips group by state order by state")
        .limit(2)
        .returns("STATE=AK; A=26856; S=80568; C=3\n"
            + "STATE=AL; A=43383; S=130151; C=3\n")
        .queryContains(
            mongoChecker(
                "{$project: {POP: '$pop', STATE: '$state'}}",
                "{$group: {_id: '$STATE', _1: {$sum: '$POP'}, _2: {$sum: {$cond: [ {$eq: ['POP', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', _1: '$_1', _2: '$_2'}}",
                "{$project: {STATE: 1, A: {$divide: [{$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, '$_2']}, S: {$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, C: '$_2'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByHaving() {
    assertModel(MODEL)
        .query("select state, count(*) as c from zips\n"
            + "group by state having count(*) > 2 order by state")
        .returnsCount(47)
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"C\": {\n"
                    + "      \"$gt\": 2\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$sort: {STATE: 1}}"));
  }

  @Ignore("https://issues.apache.org/jira/browse/CALCITE-270")
  @Test public void testGroupByHaving2() {
    assertModel(MODEL)
        .query("select state, count(*) as c from zips\n"
            + "group by state having sum(pop) > 12000000")
        .returns("STATE=NY; C=1596\n"
            + "STATE=TX; C=1676\n"
            + "STATE=FL; C=826\n"
            + "STATE=CA; C=1523\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', POP: '$pop'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}, _2: {$sum: '$POP'}}}",
                "{$project: {STATE: '$_id', C: '$C', _2: '$_2'}}",
                "{\n"
                    + "  $match: {\n"
                    + "    _2: {\n"
                    + "      $gt: 12000000\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {STATE: 1, C: 1}}"));
  }

  @Test public void testGroupByMinMaxSum() {
    assertModel(MODEL)
        .query("select count(*) as c, state,\n"
            + " min(pop) as min_pop, max(pop) as max_pop, sum(pop) as sum_pop\n"
            + "from zips group by state order by state")
        .limit(2)
        .returns("C=3; STATE=AK; MIN_POP=23238; MAX_POP=32383; SUM_POP=80568\n"
            + "C=3; STATE=AL; MIN_POP=42124; MAX_POP=44165; SUM_POP=130151\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', POP: '$pop'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}, MIN_POP: {$min: '$POP'}, MAX_POP: {$max: '$POP'}, SUM_POP: {$sum: '$POP'}}}",
                "{$project: {STATE: '$_id', C: '$C', MIN_POP: '$MIN_POP', MAX_POP: '$MAX_POP', SUM_POP: '$SUM_POP'}}",
                "{$project: {C: 1, STATE: 1, MIN_POP: 1, MAX_POP: 1, SUM_POP: 1}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupComposite() {
    assertModel(MODEL)
        .query("select count(*) as c, state, city from zips\n"
            + "group by state, city\n"
            + "order by c desc, city\n"
            + "limit 2")
        .returns("C=1; STATE=SD; CITY=ABERDEEN\n"
            + "C=1; STATE=SC; CITY=AIKEN\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', CITY: '$city'}}",
                "{$group: {_id: {STATE: '$STATE', CITY: '$CITY'}, C: {$sum: 1}}}",
                "{$project: {_id: 0, STATE: '$_id.STATE', CITY: '$_id.CITY', C: '$C'}}",
                "{$sort: {C: -1, CITY: 1}}",
                "{$limit: 2}",
                "{$project: {C: 1, STATE: 1, CITY: 1}}"));
  }

  @Ignore("broken; [CALCITE-2115] is logged to fix it")
  @Test public void testDistinctCount() {
    assertModel(MODEL)
        .query("select state, count(distinct city) as cdc from zips\n"
            + "where state in ('CA', 'TX') group by state order by state")
        .returns("STATE=CA; CDC=1072\n"
            + "STATE=TX; CDC=1233\n")
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  \"$match\": {\n"
                    + "    \"$or\": [\n"
                    + "      {\n"
                    + "        \"state\": \"CA\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"state\": \"TX\"\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}}}",
                "{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE'}}",
                "{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', CDC: '$CDC'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testDistinctCountOrderBy() {
    assertModel(MODEL)
        .query("select state, count(distinct city) as cdc\n"
            + "from zips\n"
            + "group by state\n"
            + "order by cdc desc, state\n"
            + "limit 5")
        .returns("STATE=AK; CDC=3\n"
            + "STATE=AL; CDC=3\n"
            + "STATE=AR; CDC=3\n"
            + "STATE=AZ; CDC=3\n"
            + "STATE=CA; CDC=3\n")
        .queryContains(
            mongoChecker(
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}}}",
                "{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE'}}",
                "{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', CDC: '$CDC'}}",
                "{$sort: {CDC: -1, STATE: 1}}",
                "{$limit: 5}"));
  }

  @Ignore("broken; [CALCITE-2115] is logged to fix it")
  @Test public void testProject() {
    assertModel(MODEL)
        .query("select state, city, 0 as zero from zips order by state, city")
        .limit(2)
        .returns("STATE=AK; CITY=AKHIOK; ZERO=0\n"
            + "STATE=AK; CITY=AKIACHAK; ZERO=0\n")
        .queryContains(
            mongoChecker(
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$sort: {STATE: 1, CITY: 1}}",
                "{$project: {STATE: 1, CITY: 1, ZERO: {$literal: 0}}}"));
  }

  @Test public void testFilter() {
    assertModel(MODEL)
        .query("select state, city from zips where state = 'CA'")
        .limit(3)
        .returnsUnordered("STATE=CA; CITY=LOS ANGELES",
            "STATE=CA; CITY=BELL GARDENS",
            "STATE=CA; CITY=NORWALK")
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoProject(STATE=[CAST(ITEM($0, 'state')):VARCHAR(2)], CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)])\n"
            + "    MongoFilter(condition=[=(CAST(ITEM($0, 'state')):VARCHAR(2), 'CA')])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  /** MongoDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around. */
  @Test public void testFilterReversed() {
    assertModel(MODEL)
        .query("select state, city from zips where 'WI' < state order by state, city")
        .limit(3)
        .returnsOrdered("STATE=WV; CITY=BECKLEY",
            "STATE=WV; CITY=ELM GROVE",
            "STATE=WV; CITY=STAR CITY");

    assertModel(MODEL)
        .query("select state, city from zips where state > 'WI' order by state, city")
        .limit(3)
        .returnsOrdered("STATE=WV; CITY=BECKLEY",
            "STATE=WV; CITY=ELM GROVE",
            "STATE=WV; CITY=STAR CITY");
  }

  /** MongoDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-740">[CALCITE-740]
   * Redundant WHERE clause causes wrong result in MongoDB adapter</a>. */
  @Test public void testFilterPair() {
    final int gt9k = 148;
    final int lt9k = 1;
    final int gt8k = 148;
    final int lt8k = 1;
    checkPredicate(gt9k, "where pop > 8000 and pop > 9000");
    checkPredicate(gt9k, "where pop > 9000");
    checkPredicate(lt9k, "where pop < 9000");
    checkPredicate(gt8k, "where pop > 8000");
    checkPredicate(lt8k, "where pop < 8000");
    checkPredicate(gt9k, "where pop > 9000 and pop > 8000");
    checkPredicate(gt8k, "where pop > 9000 or pop > 8000");
    checkPredicate(gt8k, "where pop > 8000 or pop > 9000");
    checkPredicate(lt8k, "where pop < 8000 and pop < 9000");
  }

  private void checkPredicate(int expected, String q) {
    assertModel(MODEL)
        .query("select count(*) as c from zips\n"
            + q)
        .returns("C=" + expected + "\n");
    assertModel(MODEL)
        .query("select * from zips\n"
            + q)
        .returnsCount(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-286">[CALCITE-286]
   * Error casting MongoDB date</a>. */
  @Test public void testDate() {
    assertModel("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'test',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'test',\n"
        + "       factory: 'org.apache.calcite.adapter.mongodb.MongoSchemaFactory',\n"
        + "       operand: {\n"
        + "         host: 'localhost',\n"
        + "         database: 'test'\n"
        + "       }\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .query("select cast(_MAP['date'] as DATE) from \"datatypes\"")
        .returnsUnordered("EXPR$0=2012-09-05");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-665">[CALCITE-665]
   * ClassCastException in MongoDB adapter</a>. */
  @Test public void testCountViaInt() {
    assertModel(MODEL)
        .query("select count(*) from zips")
        .returns(input -> {
          try {
            Assert.assertThat(input.next(), CoreMatchers.is(true));
            Assert.assertThat(input.getInt(1), CoreMatchers.is(ZIPS_SIZE));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  /**
   * Returns a function that checks that a particular MongoDB query
   * has been called.
   *
   * @param expected Expected query (as array)
   * @return validation function
   */
  private static Consumer<List> mongoChecker(final String... expected) {
    return actual -> {
      if (expected == null) {
        Assert.assertThat("null mongo Query", actual, CoreMatchers.nullValue());
        return;
      }

      if (expected.length == 0) {
        CalciteAssert.assertArrayEqual("empty Mongo query", expected,
            actual.toArray(new Object[0]));
        return;
      }

      // comparing list of Bsons (expected and actual)
      final List<BsonDocument> expectedBsons = Arrays.stream(expected).map(BsonDocument::parse)
          .collect(Collectors.toList());

      final List<BsonDocument> actualBsons =  ((List<?>) actual.get(0))
          .stream()
          .map(Objects::toString)
          .map(BsonDocument::parse)
          .collect(Collectors.toList());

      // compare Bson (not string) representation
      if (!expectedBsons.equals(actualBsons)) {
        final JsonWriterSettings settings = JsonWriterSettings.builder().indent(true).build();
        // outputs Bson in pretty Json format (with new lines)
        // so output is human friendly in IDE diff tool
        final Function<List<BsonDocument>, String> prettyFn = bsons -> bsons.stream()
            .map(b -> b.toJson(settings)).collect(Collectors.joining("\n"));

        // used to pretty print Assertion error
        Assert.assertEquals("expected and actual Mongo queries (pipelines) do not match",
            prettyFn.apply(expectedBsons),
            prettyFn.apply(actualBsons));

        Assert.fail("Should have failed previously because expected != actual is known to be true");
      }
    };
  }
}

// End MongoAdapterTest.java
