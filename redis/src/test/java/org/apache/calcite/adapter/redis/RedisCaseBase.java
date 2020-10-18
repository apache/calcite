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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.GenericContainer;

/**
 * RedisITCaseBase.
 */
@Execution(ExecutionMode.SAME_THREAD)
public abstract class RedisCaseBase {


  public static final GenericContainer<?> container =
      new GenericContainer<>("redis:6.0.6").withExposedPorts(6379);

  @BeforeAll
  public static void startRedisContainer() {
    container.start();
  }

  @AfterAll
  public static void stopRedisContainer() {
    if (container != null && container.isRunning()) {
      container.stop();
    }
  }

  public static int getRedisServerPort() {
    return container.getMappedPort(6379);
  }

  public static String getRedisServerHost() {
    return container.getContainerIpAddress();
  }
}
