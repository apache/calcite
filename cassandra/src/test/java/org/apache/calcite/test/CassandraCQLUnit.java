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
import org.apache.calcite.util.TestUtil;

import org.apache.cassandra.config.DatabaseDescriptor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.TimeUnit;

import static org.junit.Assume.assumeTrue;

/**
 * CassandraCQLUnit for start embedded cassandra cluster.
 */
public class CassandraCQLUnit extends BaseCassandraUnit {
  private CQLDataSet dataSet = null;
  public Session session;
  public Cluster cluster;

  public CassandraCQLUnit() {
  }

  public CassandraCQLUnit(CQLDataSet dataSet) {
    this.dataSet = dataSet;
  }

  public CassandraCQLUnit(CQLDataSet dataSet, int readTimeoutMillis) {
    this.dataSet = dataSet;
    this.readTimeoutMillis = readTimeoutMillis;
  }

  public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName) {
    this(dataSet);
    this.configurationFileName = configurationFileName;
  }

  public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName, int readTimeoutMillis) {
    this(dataSet);
    this.configurationFileName = configurationFileName;
    this.readTimeoutMillis = readTimeoutMillis;
  }

  // The former constructors with hostip and port have been removed. Now host+port is directly
  //read out of the provided
  // configurationFile(Name). You may also use EmbeddedCassandraServerHelper
  //.CASSANDRA_RNDPORT_YML_FILE to select
  // random (free) ports for EmbeddedCassandra, such that you can start multiple embedded
  //cassandras on the same host
  // (but not in the same JVM).
  public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName,
      long startUpTimeoutMillis) {
    super(startUpTimeoutMillis);
    this.dataSet = dataSet;
    this.configurationFileName = configurationFileName;
  }

  public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName,
      long startUpTimeoutMillis, int readTimeoutMillis) {
    super(startUpTimeoutMillis);
    this.dataSet = dataSet;
    this.configurationFileName = configurationFileName;
    this.readTimeoutMillis = readTimeoutMillis;
  }

  @Override public void beforeAll(ExtensionContext context) throws Exception {
    // run tests only if explicitly enabled
    assumeTrue("test explicitly disabled", isEnabled());
    super.beforeAll(context);
  }

  @Override public void afterAll(ExtensionContext context) throws Exception {
    // run tests only if explicitly enabled
    assumeTrue("test explicitly disabled", isEnabled());
    super.afterAll(context);
  }

  @Override protected void load() {
    cluster = EmbeddedCassandraServerHelper.getCluster();
    session = EmbeddedCassandraServerHelper.getSession();
    CQLDataLoader dataLoader = new CQLDataLoader(session);
    dataLoader.load(dataSet);
    session = dataLoader.getSession();
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
   * @return {@code true} if test is compatible with current environment,
   * {@code false} otherwise
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>
   */
  private static boolean enabled() {
    final boolean enabled = CalciteSystemProperty.TEST_CASSANDRA.value();
    Bug.upgrade("remove JDK version check once current adapter supports Cassandra 4.x");
    final boolean compatibleJdk = TestUtil.getJavaMajorVersion() < 11;
    return enabled && compatibleJdk;
  }

  // Getters for those who do not like to directly access fields
  public Session getSession() {
    return session;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public boolean isEnabled() {
    return enabled() && dataSet != null;
  }

  CassandraCQLUnit initCassandraIfEnabled() {
    try {
      if (!enabled()) {
        // Return NOP resource (to avoid nulls)
        return new CassandraCQLUnit();
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
    } catch (Exception ignored) {
    }
    return new CassandraCQLUnit();
  }
}
