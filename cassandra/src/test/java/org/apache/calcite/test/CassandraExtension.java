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
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.lang3.SystemUtils;

import com.datastax.oss.driver.api.core.CqlSession;
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
import java.net.URL;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * JUnit5 extension to start and stop embedded Cassandra server.
 *
 * <p>Note that tests will be skipped if running on JDK11+, Eclipse OpenJ9 JVM or Windows
 * (the first isn't supported by cassandra-unit, the last two by Cassandra) see
 * <a href="https://github.com/jsevellec/cassandra-unit/issues/294">cassandra-unit issue #294</a>
 * <a href="https://issues.apache.org/jira/browse/CASSANDRA-14883">CASSANDRA-14883</a>,
 * and <a href="https://issues.apache.org/jira/browse/CASSANDRA-16956">CASSANDRA-16956</a>,
 * respectively.
 */
class CassandraExtension implements ParameterResolver, ExecutionCondition {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(CassandraExtension.class);

  private static final String KEY = "cassandra";

  @Override public boolean supportsParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    final Class<?> type = parameterContext.getParameter().getType();
    return CqlSession.class.isAssignableFrom(type);
  }

  @Override public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {

    Class<?> type = parameterContext.getParameter().getType();
    if (CqlSession.class.isAssignableFrom(type)) {
      return getOrCreate(extensionContext).session;
    }

    throw new ExtensionConfigurationException(
        String.format(Locale.ROOT, "%s supports only %s but yours was %s",
        CassandraExtension.class.getSimpleName(), CqlSession.class.getName(), type.getName()));
  }

  static ImmutableMap<String, String> getDataset(String resourcePath) {
    URL u = CassandraExtension.class.getResource(resourcePath);
    return ImmutableMap.of("model",
        Sources.of(requireNonNull(u, "u")).file().getAbsolutePath());
  }

  /** Registers a Cassandra resource in root context, so it can be shared with
   * other tests. */
  private static CassandraResource getOrCreate(ExtensionContext context) {
    // same cassandra instance should be shared across all extension instances
    return context.getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(KEY, key -> new CassandraResource(), CassandraResource.class);
  }

  /**
   * Whether to run this test.
   *
   * <p>Enabled by default, unless explicitly disabled
   * from command line ({@code -Dcalcite.test.cassandra=false}) or running on incompatible JDK
   * version or JVM (see below).
   *
   * <p>cassandra-unit does not support JDK11+ yet, therefore all cassandra tests will be skipped
   * if this JDK version is used.
   *
   * @see <a href="https://github.com/jsevellec/cassandra-unit/issues/294">cassandra-unit issue #294</a>
   *
   * <p>Cassandra does not support Eclipse OpenJ9 JVM, therefore all cassandra tests will be
   * skipped if this JVM version is used.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-14883">CASSANDRA-14883</a>
   *
   * <p>Cassandra requires method
   * {@link com.google.common.collect.ImmutableSet#builderWithExpectedSize(int)}
   * and therefore Guava 23 or higher.
   *
   * <p>Cassandra dropped support for Windows in 4.1.x
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-16956">CASSANDRA-16956</a>
   *
   * @return {@code true} if test is compatible with current environment,
   *         {@code false} otherwise
   */
  @Override public ConditionEvaluationResult evaluateExecutionCondition(
      final ExtensionContext context) {
    boolean enabled = CalciteSystemProperty.TEST_CASSANDRA.value();
    // remove JDK version check once cassandra-unit supports JDK11+
    boolean compatibleJdk = TestUtil.getJavaMajorVersion() < 11;
    boolean compatibleGuava = TestUtil.getGuavaMajorVersion() >= 23;
    // remove JVM check once Cassandra supports Eclipse OpenJ9 JVM
    boolean compatibleJVM = !"Eclipse OpenJ9".equals(TestUtil.getJavaVirtualMachineVendor());
    boolean compatibleOS = !SystemUtils.IS_OS_WINDOWS;
    if (enabled && compatibleJdk && compatibleGuava && compatibleJVM && compatibleOS) {
      return ConditionEvaluationResult.enabled("Cassandra tests enabled");
    }

    final StringBuilder sb = new StringBuilder("Cassandra tests disabled:");
    if (!compatibleJdk) {
      sb.append("\n - JDK11+ is not supported");
    }
    if (!compatibleGuava) {
      sb.append("\n - Guava versions < 23 are not supported");
    }
    if (!compatibleJVM) {
      sb.append("\n - Eclipse OpenJ9 JVM is not supported");
    }
    if (!compatibleOS) {
      sb.append("\n - Cassandra doesn't support Windows");
    }
    return ConditionEvaluationResult.disabled(sb.toString());
  }

  /** Cassandra resource. */
  private static class CassandraResource
      implements ExtensionContext.Store.CloseableResource {
    private final CqlSession session;

    private CassandraResource() {
      startCassandra();
      this.session = EmbeddedCassandraServerHelper.getSession();
    }

    /**
     * Best effort to gracefully shutdown <strong>embedded</strong> cassandra
     * cluster.
     *
     * <p>Since it uses many static variables as well as {@link System#exit(int)}
     * during close, clean shutdown (as part of unit test) is not
     * straightforward.
     */
    @Override public void close() {
      session.close();

      CassandraDaemon daemon = extractDaemon();
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
      Stage.shutdownNow();
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
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(Duration.ofMinutes(2).toMillis());
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
