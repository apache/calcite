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

import org.apache.calcite.avatica.util.DateTimeUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.common.collect.ImmutableMap;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Objects;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package related to data types.
 *
 * <p>Will start embedded cassandra cluster and populate it from local {@code datatypes.cql} file.
 * All configuration files are located in test classpath.
 *
 * <p>Note that tests will be skipped if running on JDK11+
 * (which is not yet supported by cassandra) see
 * <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 *
 */
@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith(CassandraExtension.class)
class CassandraAdapterDataTypesTest {

  /** Connection factory based on the "mongo-zips" model. */
  private static final ImmutableMap<String, String> DTCASSANDRA =
          CassandraExtension.getDataset("/model-datatypes.json");

  @BeforeAll
  static void load(CqlSession session) {
    new CQLDataLoader(session)
        .load(new ClassPathCQLDataSet("datatypes.cql"));
  }

  @Test void testSimpleTypesRowType() {
    CalciteAssert.that()
            .with(DTCASSANDRA)
            .query("select * from \"test_simple\"")
            .typeIs("[f_int INTEGER"
                + ", f_ascii VARCHAR"
                + ", f_bigint BIGINT"
                + ", f_blob VARBINARY"
                + ", f_boolean BOOLEAN"
                + ", f_date DATE"
                + ", f_decimal DOUBLE"
                + ", f_double DOUBLE"
                + ", f_duration ANY"
                + ", f_float REAL"
                + ", f_inet ANY"
                + ", f_int_null INTEGER"
                + ", f_smallint SMALLINT"
                + ", f_text VARCHAR"
                + ", f_time BIGINT"
                + ", f_timestamp TIMESTAMP"
                + ", f_timeuuid CHAR"
                + ", f_tinyint TINYINT"
                + ", f_uuid CHAR"
                + ", f_varchar VARCHAR"
                + ", f_varint INTEGER]");
  }

  @Test void testFilterWithNonStringLiteral() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_type\" where \"f_id\" = 1")
        .returns("");

    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_type\" where \"f_id\" > 1")
        .returns("f_id=3000000000; f_user=ANNA\n");

    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_date_type\" where \"f_date\" = '2015-05-03'")
        .returns("f_date=2015-05-03; f_user=ANNA\n");

    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_timestamp_type\" where cast(\"f_timestamp\" as timestamp "
            + "with local time zone) = '2011-02-03 04:05:00 UTC'")
        .returns("f_timestamp=2011-02-03 04:05:00; f_user=ANNA\n");

    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_timestamp_type\" where \"f_timestamp\""
            + " = '2011-02-03 04:05:00'")
        .returns("f_timestamp=2011-02-03 04:05:00; f_user=ANNA\n");
  }

  @Test void testSimpleTypesValues() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_simple\"")
        .returns("f_int=0"
            + "; f_ascii=abcdefg"
            + "; f_bigint=3000000000"
            + "; f_blob=20"
            + "; f_boolean=true"
            + "; f_date=2015-05-03"
            + "; f_decimal=2.1"
            + "; f_double=2.0"
            + "; f_duration=89h9m9s"
            + "; f_float=5.1"
            + "; f_inet=/192.168.0.1"
            + "; f_int_null=null"
            + "; f_smallint=5"
            + "; f_text=abcdefg"
            + "; f_time=48654234000000"
            + "; f_timestamp=2011-02-03 04:05:00"
            + "; f_timeuuid=8ac6d1dc-fbeb-11e9-8f0b-362b9e155667"
            + "; f_tinyint=0"
            + "; f_uuid=123e4567-e89b-12d3-a456-426655440000"
            + "; f_varchar=abcdefg"
            + "; f_varint=10\n");
  }

  @Test void testCounterRowType() {
    CalciteAssert.that()
            .with(DTCASSANDRA)
            .query("select * from \"test_counter\"")
            .typeIs("[f_int INTEGER, f_counter BIGINT]");
  }

  @Test void testCounterValues() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_counter\"")
        .returns("f_int=1; f_counter=1\n");
  }

  @Test void testCollectionsRowType() {
    CalciteAssert.that()
            .with(DTCASSANDRA)
            .query("select * from \"test_collections\"")
            .typeIs("[f_int INTEGER"
                + ", f_list INTEGER ARRAY"
                + ", f_map (VARCHAR, VARCHAR) MAP"
                + ", f_set DOUBLE MULTISET"
                + ", f_tuple STRUCT]");
  }

  @Test void testCollectionsValues() {
    CalciteAssert.that()
            .with(DTCASSANDRA)
            .query("select * from \"test_collections\"")
            .returns("f_int=0"
                + "; f_list=[1, 2, 3]"
                + "; f_map={k1=v1, k2=v2}"
                + "; f_set=[2.0, 3.1]"
                + "; f_tuple={3000000000, 30ff87, 2015-05-03 13:30:54.234}"
                + "\n");
  }

  @Test void testCollectionsInnerRowType() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select \"f_list\"[1], "
            + "\"f_map\"['k1'], "
            + "\"test_collections\".\"f_tuple\".\"1\", "
            + "\"test_collections\".\"f_tuple\".\"2\", "
            + "\"test_collections\".\"f_tuple\".\"3\""
            + " from \"test_collections\"")
        .typeIs("[EXPR$0 INTEGER"
            + ", EXPR$1 VARCHAR"
            + ", 1 BIGINT"
            + ", 2 VARBINARY"
            + ", 3 TIMESTAMP]");
  }

  @Test void testCollectionsInnerValues() {
    // timestamp retrieval depends on the user timezone, we must compute the expected result
    long v = Objects.requireNonNull(
        TypeCodecs.TIMESTAMP.parse("'2015-05-03 13:30:54.234'")).toEpochMilli();
    String expectedTimestamp = DateTimeUtils.unixTimestampToString(v);

    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select \"f_list\"[1], "
            + "\"f_map\"['k1'], "
            + "\"test_collections\".\"f_tuple\".\"1\", "
            + "\"test_collections\".\"f_tuple\".\"2\", "
            + "\"test_collections\".\"f_tuple\".\"3\""
            + " from \"test_collections\"")
        .returns("EXPR$0=1"
            + "; EXPR$1=v1"
            + "; 1=3000000000"
            + "; 2=30ff87"
            + "; 3=" + expectedTimestamp + "\n");
  }

  // frozen collections should not affect the row type
  @Test void testFrozenCollectionsRowType() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_frozen_collections\"")
        .typeIs("[f_int INTEGER"
            + ", f_list INTEGER ARRAY"
            + ", f_map (VARCHAR, VARCHAR) MAP"
            + ", f_set DOUBLE MULTISET"
            + ", f_tuple STRUCT]");
    // we should test (BIGINT, VARBINARY, TIMESTAMP) STRUCT but inner types are not exposed
  }

  // frozen collections should not affect the result set
  @Test void testFrozenCollectionsValues() {
    CalciteAssert.that()
        .with(DTCASSANDRA)
        .query("select * from \"test_frozen_collections\"")
        .returns("f_int=0"
            + "; f_list=[1, 2, 3]"
            + "; f_map={k1=v1, k2=v2}"
            + "; f_set=[2.0, 3.1]"
            + "; f_tuple={3000000000, 30ff87, 2015-05-03 13:30:54.234}"
            + "\n");
  }
}
