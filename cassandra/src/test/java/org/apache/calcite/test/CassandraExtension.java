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

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.transport.TTransportException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

/**
 * JUnit5 extension to start and stop embedded cassandra server.
 *
 * <p>Note that tests will be skipped if running on JDK11+
 * (which is not yet supported by cassandra) see
 *  <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 */
class CassandraExtension implements ParameterResolver, ExecutionCondition {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(CassandraExtension.class);

  private static final String KEY = "cassandra";

  @Override public boolean supportsParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    final Class<?> type = parameterContext.getParameter().getType();
    return Session.class.isAssignableFrom(type) || Cluster.class.isAssignableFrom(type);
  }

  @Override public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {

    Class<?> type = parameterContext.getParameter().getType();
    if (Session.class.isAssignableFrom(type)) {
      return getOrCreate(extensionContext).session;
    } else if (Cluster.class.isAssignableFrom(type)) {
      return getOrCreate(extensionContext).cluster;
    }

    throw new ExtensionConfigurationException(
        String.format(Locale.ROOT, "%s supports only %s or %s but yours was %s",
        CassandraExtension.class.getSimpleName(),
        Session.class.getName(), Cluster.class.getName(), type.getName()));
  }

  static ImmutableMap<String, String> getDataset(String resourcePath) {
    return ImmutableMap.of("model",
        Sources.of(CassandraExtension.class.getResource(resourcePath))
            .file().getAbsolutePath());
  }

  /**
   * Register cassandra resource in root context so it can be shared with other tests
   */
  private static CassandraResource getOrCreate(ExtensionContext context) {
    // same cassandra instance should be shared across all extension instances
    return context.getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(KEY, key -> new CassandraResource(), CassandraResource.class);
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
  @Override public ConditionEvaluationResult evaluateExecutionCondition(
      final ExtensionContext context) {
    boolean enabled = CalciteSystemProperty.TEST_CASSANDRA.value();
    Bug.upgrade("remove JDK version check once current adapter supports Cassandra 4.x");
    boolean compatibleJdk = TestUtil.getJavaMajorVersion() < 11;
    if (enabled && compatibleJdk) {
      return ConditionEvaluationResult.enabled("Cassandra enabled");
    }
    return ConditionEvaluationResult.disabled("Cassandra tests disabled");
  }

  private static class CassandraResource implements ExtensionContext.Store.CloseableResource {
    private final Session session;
    private final Cluster cluster;

    private CassandraResource() {
      startCassandra();
      this.cluster = EmbeddedCassandraServerHelper.getCluster();
      this.session = EmbeddedCassandraServerHelper.getSession();
    }

    /**
     * Best effort to gracefully shutdown <strong>embedded</strong> cassandra cluster.
     *
     * Since it uses many static variables as well as {@link System#exit(int)} during close,
     * clean shutdown (as part of unit test) is not straightforward.
     */
    @Override public void close() throws IOException {

      session.close();
      cluster.close();

      CassandraDaemon daemon = extractDaemon();
      if (daemon.thriftServer != null) {
        daemon.thriftServer.stop();
      }
      daemon.stopNativeTransport();

      StorageService storage = StorageService.instance;
      storage.setRpcReady(false);
      storage.stopClient();
      storage.stopTransports();
      try {
        storage.drain(); // try to close all resources
      } catch (IOException | ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      StageManager.shutdownNow();

      if (FBUtilities.isWindows) {
        // for some reason .toDelete stale folder is not deleted on cassandra shutdown
        // doing it manually here
        WindowsFailedSnapshotTracker.resetForTests();
        // manually delete stale file(s)
        Files.deleteIfExists(Paths.get(WindowsFailedSnapshotTracker.TODELETEFILE));
      }
    }

    private static void startCassandra() {
      // This static init is necessary otherwise tests fail with CassandraUnit in IntelliJ (jdk10)
      // should be called right after constructor
      // NullPointerException for DatabaseDescriptor.getDiskFailurePolicy
      // for more info see
      // https://github.com/jsevellec/cassandra-unit/issues/249
      // https://github.com/jsevellec/cassandra-unit/issues/221
      DatabaseDescriptor.daemonInitialization();

      // Apache Jenkins often fails with
      // Cassandra daemon did not start within timeout (20 sec by default)
      try {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(Duration.ofMinutes(1).toMillis());
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Extract {@link CassandraDaemon} instance using reflection. It will be used
     * to shutdown the cluster
     */
    private static CassandraDaemon extractDaemon() {
      try {
        Field field = EmbeddedCassandraServerHelper.class.getDeclaredField("cassandraDaemon");
        field.setAccessible(true);
        CassandraDaemon daemon = (CassandraDaemon) field.get(null);

        if (daemon == null) {
          throw new IllegalStateException("Cassandra daemon was not initialized by "
              + EmbeddedCassandraServerHelper.class.getSimpleName());
        }
        return daemon;
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
