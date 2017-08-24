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

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Before calling this test, you need to populate Cassandra, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Cassandra and the "twissandra"
 * test data set.
 */
public class CassandraAdapterIT {
  /** Connection factory based on the "mongo-zips" model. */
  public static final ImmutableMap<String, String> TWISSANDRA =
      ImmutableMap.of("model",
          CassandraAdapterIT.class.getResource("/model.json")
              .getPath());

  /** Whether to run Cassandra tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.cassandra=false} on the Java command line. */
  public static final boolean ENABLED =
      Util.getBooleanProperty("calcite.test.cassandra", true);

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  @Test public void testSelect() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select * from \"users\"")
        .returnsCount(10);
  }

  @Test public void testFilter() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\"='!PUBLIC!'")
        .limit(1)
        .returns("username=!PUBLIC!; time=e8754000-80b8-1fe9-8e73-e3698c967ddd; "
            + "tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
           + "  CassandraFilter(condition=[=($0, '!PUBLIC!')])\n"
           + "    CassandraTableScan(table=[[twissandra, userline]]");
  }

  @Test public void testFilterUUID() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select * from \"tweets\" where \"tweet_id\"='f3cd759c-d05b-11e5-b58b-90e2ba530b12'")
        .limit(1)
        .returns("tweet_id=f3cd759c-d05b-11e5-b58b-90e2ba530b12; "
            + "body=Lacus augue pede posuere.; username=JmuhsAaMdw\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
           + "  CassandraFilter(condition=[=(CAST($0):CHAR(36) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'f3cd759c-d05b-11e5-b58b-90e2ba530b12')])\n"
           + "    CassandraTableScan(table=[[twissandra, tweets]]");
  }

  @Test public void testSort() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\" = '!PUBLIC!' order by \"time\" desc")
        .returnsCount(146)
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
            + "  CassandraSort(sort0=[$1], dir0=[DESC])\n"
            + "    CassandraFilter(condition=[=($0, '!PUBLIC!')])\n");
  }

  @Test public void testProject() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 2")
        .returns("tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n"
               + "tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n")
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
                + "  CassandraLimit(fetch=[2])\n"
                + "    CassandraProject(tweet_id=[$2])\n"
                + "      CassandraFilter(condition=[=($0, '!PUBLIC!')])\n");
  }

  @Test public void testProjectAlias() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select \"tweet_id\" as \"foo\" from \"userline\" "
                + "where \"username\" = '!PUBLIC!' limit 1")
        .returns("foo=f3c329de-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test public void testProjectConstant() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select 'foo' as \"bar\" from \"userline\" limit 1")
        .returns("bar=foo\n");
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 8")
        .explainContains("CassandraLimit(fetch=[8])\n");
  }

  @Test public void testSortLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\"='!PUBLIC!' "
             + "order by \"time\" desc limit 10")
        .explainContains("  CassandraLimit(fetch=[10])\n"
                       + "    CassandraSort(sort0=[$1], dir0=[DESC])");
  }

  @Test public void testSortOffset() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where "
             + "\"username\"='!PUBLIC!' limit 2 offset 1")
        .explainContains("CassandraLimit(offset=[1], fetch=[2])")
        .returns("tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n"
               + "tweet_id=f3e4182e-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test public void testMaterializedView() {
    CalciteAssert.that()
        .enable(enabled())
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"tweets\" where \"username\"='JmuhsAaMdw'")
        .enableMaterializations(true)
        .explainContains("CassandraTableScan(table=[[twissandra, tweets_by_user]])");
  }
}

// End CassandraAdapterIT.java
