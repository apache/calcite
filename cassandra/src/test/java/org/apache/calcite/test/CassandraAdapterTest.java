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

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Will start embedded cassandra cluster and populate it from local {@code twissandra.cql} file.
 * All configuration files are located in test classpath.
 *
 * <p>Note that tests will be skipped if running on JDK11+
 * (which is not yet supported by cassandra) see
 * <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 *
 */
@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith(CassandraExtension.class)
class CassandraAdapterTest {

  /** Connection factory based on the "mongo-zips" model. */
  private static final ImmutableMap<String, String> TWISSANDRA =
          CassandraExtension.getDataset("/model.json");

  @BeforeAll
  static void load(CqlSession session) {
    new CQLDataLoader(session)
        .load(new ClassPathCQLDataSet("twissandra.cql"));
  }

  @Test void testSelect() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"users\"")
        .returnsCount(10);
  }

  @Test void testFilter() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\"='!PUBLIC!'")
        .limit(1)
        .returns("username=!PUBLIC!; time=e8754000-80b8-1fe9-8e73-e3698c967ddd; "
            + "tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
           + "  CassandraFilter(condition=[=($0, '!PUBLIC!')])\n"
           + "    CassandraTableScan(table=[[twissandra, userline]]");
  }

  @Test void testFilterUUID() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"tweets\" where \"tweet_id\"='f3cd759c-d05b-11e5-b58b-90e2ba530b12'")
        .limit(1)
        .returns("tweet_id=f3cd759c-d05b-11e5-b58b-90e2ba530b12; "
            + "body=Lacus augue pede posuere.; username=JmuhsAaMdw\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
           + "  CassandraFilter(condition=[=(CAST($0):CHAR(36), 'f3cd759c-d05b-11e5-b58b-90e2ba530b12')])\n"
           + "    CassandraTableScan(table=[[twissandra, tweets]]");
  }

  @Test void testSort() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\" = '!PUBLIC!' order by \"time\" desc")
        .returnsCount(146)
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
            + "  CassandraSort(sort0=[$1], dir0=[DESC])\n"
            + "    CassandraFilter(condition=[=($0, '!PUBLIC!')])\n");
  }

  @Test void testProject() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 2")
        .returns("tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n"
               + "tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
                + "  CassandraLimit(fetch=[2])\n"
                + "    CassandraProject(tweet_id=[$2])\n"
                + "      CassandraFilter(condition=[=($0, '!PUBLIC!')])\n");
  }

  @Test void testProjectAlias() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" as \"foo\" from \"userline\" "
                + "where \"username\" = '!PUBLIC!' limit 1")
        .returns("foo=f3c329de-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test void testProjectConstant() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select 'foo' as \"bar\" from \"userline\" limit 1")
        .returns("bar=foo\n");
  }

  @Test void testLimit() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 8")
        .explainContains("CassandraLimit(fetch=[8])\n");
  }

  @Test void testSortLimit() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\"='!PUBLIC!' "
             + "order by \"time\" desc limit 10")
        .explainContains("  CassandraLimit(fetch=[10])\n"
                       + "    CassandraSort(sort0=[$1], dir0=[DESC])");
  }

  @Test void testSortOffset() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where "
             + "\"username\"='!PUBLIC!' limit 2 offset 1")
        .explainContains("CassandraLimit(offset=[1], fetch=[2])")
        .returns("tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n"
               + "tweet_id=f3e4182e-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test void testMaterializedView() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"tweets\" where "
            + "\"username\"='JmuhsAaMdw' and \"tweet_id\"='f3d3d4dc-d05b-11e5-b58b-90e2ba530b12'")
        .enableMaterializations(true)
        .explainContains("CassandraTableScan(table=[[twissandra, Tweets_By_User]])");
  }
}
