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

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.BeforeClass;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.TimeUnit;

import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * Common initialization functions for Cassandra tests.
 */
public abstract class AbstractCassandraAdapterTest {

  /** Connection factory based on the "mongo-zips" model. */
  static ImmutableMap<String, String> getDataset(String resourcePath) {
    return ImmutableMap.of("model",
        Sources.of(CassandraAdapterTest.class.getResource(resourcePath))
            .file().getAbsolutePath());
  }

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

  static ExternalResource initCassandraIfEnabled(String dataSetLocation) {
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
        new ClassPathCQLDataSet(dataSetLocation),
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
}
