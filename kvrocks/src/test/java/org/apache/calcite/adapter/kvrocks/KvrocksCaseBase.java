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
package org.apache.calcite.adapter.kvrocks;

import org.apache.calcite.config.CalciteSystemProperty;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.logging.Logger;

/**
 * Base class for Kvrocks adapter tests.
 *
 * <p>Uses a Kvrocks Docker container ({@code apache/kvrocks:latest}) when
 * Docker is available; otherwise tests are skipped via
 * {@link #isKvrocksRunning()}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public abstract class KvrocksCaseBase {

  private static final Logger LOGGER = Logger.getLogger(KvrocksCaseBase.class.getName());

  /** Default Kvrocks port inside the container. */
  private static final int KVROCKS_PORT = 6666;

  private static final GenericContainer<?> KVROCKS_CONTAINER =
      new GenericContainer<>("apache/kvrocks:latest")
          .withExposedPorts(KVROCKS_PORT);

  private static boolean containerStarted = false;

  @BeforeAll
  public static void startContainer() {
    if (CalciteSystemProperty.TEST_WITH_DOCKER_CONTAINER.value()
        && DockerClientFactory.instance().isDockerAvailable()) {
      KVROCKS_CONTAINER.start();
      containerStarted = true;
      LOGGER.info("Kvrocks container started on port " + getKvrocksServerPort());
    } else {
      LOGGER.warning("Docker is not available; Kvrocks tests will be skipped");
    }
  }

  @AfterAll
  public static void stopContainer() {
    if (containerStarted && KVROCKS_CONTAINER.isRunning()) {
      KVROCKS_CONTAINER.stop();
    }
  }

  /** Whether the Kvrocks server is reachable. */
  static boolean isKvrocksRunning() {
    return containerStarted && KVROCKS_CONTAINER.isRunning();
  }

  static int getKvrocksServerPort() {
    return KVROCKS_CONTAINER.getMappedPort(KVROCKS_PORT);
  }

  static String getKvrocksServerHost() {
    return KVROCKS_CONTAINER.getHost();
  }

  /**
   * Finds a free port on the host.
   */
  static int getAvailablePort() {
    for (int i = 0; i < 50; i++) {
      try (ServerSocket socket = new ServerSocket(0)) {
        int port = socket.getLocalPort();
        if (port != 0) {
          return port;
        }
      } catch (IOException ignored) {
      }
    }
    throw new RuntimeException("Could not find an available port");
  }
}
