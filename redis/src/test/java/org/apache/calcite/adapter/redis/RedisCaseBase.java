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
package org.apache.calcite.adapter.redis;

import org.apache.calcite.config.CalciteSystemProperty;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.logging.Logger;

import redis.embedded.RedisServer;

/**
 * RedisITCaseBase.
 */
@Execution(ExecutionMode.SAME_THREAD)
public abstract class RedisCaseBase {

  private static final int PORT = getAvailablePort();
  private static final String HOST = "127.0.0.1";
  private static final String MAX_HEAP = "maxheap 51200000";

  /**
   * The Redis Docker container.
   *
   * <p>Uses the Redis 2.8.19 version to be aligned with the embedded server.
   */
  private static final GenericContainer<?> REDIS_CONTAINER =
      new GenericContainer<>("redis:7.2.4").withExposedPorts(6379);

  /**
   * The embedded Redis server.
   *
   * <p>With the existing dependencies (com.github.kstyrc:embedded-redis:0.6) it
   * uses by default Redis 2.8.19 version.
   */
  private static RedisServer redisServer;

  @BeforeAll
  public static void startRedisContainer() {
    // Check if docker is running, and start container if possible
    if (CalciteSystemProperty.TEST_WITH_DOCKER_CONTAINER.value()
        && DockerClientFactory.instance().isDockerAvailable()) {
      REDIS_CONTAINER.start();
    }
  }

  @BeforeEach
  public void createRedisServer() throws IOException {
    if (!REDIS_CONTAINER.isRunning()) {
      if (isWindows()) {
        redisServer = RedisServer.builder().port(PORT).setting(MAX_HEAP).build();
      } else {
        redisServer = new RedisServer(PORT);
      }
      Logger.getAnonymousLogger().info("Not using Docker, starting RedisMiniServer");
      redisServer.start();
    }
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  @AfterEach
  public void stopRedisServer() {
    if (!REDIS_CONTAINER.isRunning()) {
      redisServer.stop();
    }
  }

  /**
   * Find a non-occupied port.
   *
   * @return A non-occupied port.
   */
  public static int getAvailablePort() {
    for (int i = 0; i < 50; i++) {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        int port = serverSocket.getLocalPort();
        if (port != 0) {
          return port;
        }
      } catch (IOException ignored) {
      }
    }

    throw new RuntimeException("Could not find an available port on the host.");
  }

  @AfterAll
  public static void stopRedisContainer() {
    if (REDIS_CONTAINER.isRunning()) {
      REDIS_CONTAINER.stop();
    }
  }

  static int getRedisServerPort() {
    return  REDIS_CONTAINER.isRunning() ? REDIS_CONTAINER.getMappedPort(6379) : PORT;
  }

  static String getRedisServerHost() {
    return REDIS_CONTAINER.isRunning() ? REDIS_CONTAINER.getHost() : HOST;
  }

}
