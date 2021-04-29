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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.ServerSocket;

import redis.embedded.RedisServer;

/**
 * RedisITCaseBase.
 */
@Execution(ExecutionMode.SAME_THREAD)
public abstract class RedisCaseBase {

  public static final int PORT = getAvailablePort();
  public static final String HOST = "127.0.0.1";
  private static final String MAX_HEAP = "maxheap 51200000";

  private static RedisServer redisServer;

  @BeforeEach
  public void createRedisServer() throws IOException {
    if (isWindows()) {
      redisServer = RedisServer.builder().port(PORT).setting(MAX_HEAP).build();
    } else {
      redisServer = new RedisServer(PORT);
    }
    redisServer.start();
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  @AfterEach
  public void stopRedisServer() {
    redisServer.stop();
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
}
