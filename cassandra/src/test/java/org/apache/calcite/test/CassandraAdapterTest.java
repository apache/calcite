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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.collect.ImmutableMap;

import net.jcip.annotations.NotThreadSafe;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.TimeUnit;

import static org.junit.Assume.assumeTrue;

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
// force tests to run sequentially (maven surefire and failsafe are running them in parallel)
// seems like some of our code is sharing static variables (like Hooks) which causes tests
// to fail non-deterministically (flaky tests).
@NotThreadSafe
public class CassandraAdapterTest {

  @ClassRule
  public static final ExternalResource RULE = initCassandraIfEnabled();

  /** Connection factory based on the "mongo-zips" model. */
  private static final ImmutableMap<String, String> TWISSANDRA =
      ImmutableMap.of("model",
          Sources.of(
              CassandraAdapterTest.class.getResource("/model.json"))
              .file().getAbsolutePath());

  /**
   * Whether to run this test.
   * <p>Enabled by default, unless explicitly disabled
   * from command line ({@code -Dcalcite.test.cassandra=false}) or running on incompatible JDK
   * version (see below).
   *
   * <p>As of this wiring Cassandra 4.x is not yet released and we're using 3.x
   * (which fails on JDK11+). All cassandra tests will be skipped if
   * running on JDK11+.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>
   * @return {@code true} if test is compatible with current environment,
   *         {@code false} otherwise
   */
  private static boolean enabled() {
    final boolean enabled = CalciteSystemProperty.TEST_CASSANDRA.value();
    Bug.upgrade("remove JDK version check once current adapter supports Cassandra 4.x");
    final boolean compatibleJdk = TestUtil.getJavaMajorVersion() < 11;
    return enabled && compatibleJdk;
  }

  private static ExternalResource initCassandraIfEnabled() {
    if (!enabled()) {
      // Return NOP resource (to avoid nulls)
      return new ExternalResource() {
        @Override public Statement apply(final Statement base, final Description description) {
          return super.apply(base, description);
        }
      };
    }

    String configurationFileName = null; // use default one
    // Apache Jenkins often fails with
    // CassandraAdapterTest Cassandra daemon did not start within timeout (20 sec by default)
    long startUpTimeoutMillis = TimeUnit.SECONDS.toMillis(60);

    CassandraCQLUnit rule = new CassandraCQLUnit(
        new ClassPathCQLDataSet("twissandra.cql"),
        configurationFileName,
        startUpTimeoutMillis);

    // This static init is necessary otherwise tests fail with CassandraUnit in IntelliJ (jdk10)
    // should be called right after constructor
    // NullPointerException for DatabaseDescriptor.getDiskFailurePolicy
    // for more info see
    // https://github.com/jsevellec/cassandra-unit/issues/249
    // https://github.com/jsevellec/cassandra-unit/issues/221
    DatabaseDescriptor.daemonInitialization();

    return rule;
  }

  @BeforeClass
  public static void setUp() {
    // run tests only if explicitly enabled
    assumeTrue("test explicitly disabled", enabled());
  }

  @Test public void testSelect() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"users\"")
        .returnsCount(10);
  }

  @Test public void testFilter() {
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

  @Test public void testFilterUUID() {
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

  @Test public void testSort() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\" = '!PUBLIC!' order by \"time\" desc")
        .returnsCount(146)
        .explainContains("PLAN=CassandraToEnumerableConverter\n"
            + "  CassandraSort(sort0=[$1], dir0=[DESC])\n"
            + "    CassandraFilter(condition=[=($0, '!PUBLIC!')])\n");
  }

  @Test public void testProject() {
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

  @Test public void testProjectAlias() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" as \"foo\" from \"userline\" "
                + "where \"username\" = '!PUBLIC!' limit 1")
        .returns("foo=f3c329de-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test public void testProjectConstant() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select 'foo' as \"bar\" from \"userline\" limit 1")
        .returns("bar=foo\n");
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 8")
        .explainContains("CassandraLimit(fetch=[8])\n");
  }

  @Test public void testSortLimit() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select * from \"userline\" where \"username\"='!PUBLIC!' "
             + "order by \"time\" desc limit 10")
        .explainContains("  CassandraLimit(fetch=[10])\n"
                       + "    CassandraSort(sort0=[$1], dir0=[DESC])");
  }

  @Test public void testSortOffset() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"userline\" where "
             + "\"username\"='!PUBLIC!' limit 2 offset 1")
        .explainContains("CassandraLimit(offset=[1], fetch=[2])")
        .returns("tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n"
               + "tweet_id=f3e4182e-d05b-11e5-b58b-90e2ba530b12\n");
  }

  @Test public void testMaterializedView() {
    CalciteAssert.that()
        .with(TWISSANDRA)
        .query("select \"tweet_id\" from \"tweets\" where \"username\"='JmuhsAaMdw'")
        .enableMaterializations(true)
        .explainContains("CassandraTableScan(table=[[twissandra, Tweets_By_User]])");
  }
}

// End CassandraAdapterTest.java
