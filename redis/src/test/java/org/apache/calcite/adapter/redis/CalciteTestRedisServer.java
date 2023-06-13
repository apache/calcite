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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

/**
 * This is almost a copy of {@link RedisServer}.
 * The difference is that it makes {@link CalciteTestRedisServer#CalciteTestRedisServer(List, int)}
 * public, adds {@link CalciteTestRedisServer#testBuilder()}.
 */
public class CalciteTestRedisServer extends RedisServer {
  private static CalciteTestRedisExecProvider redisExecProvider =
      CalciteTestRedisExecProvider.defaultProvider();

  public CalciteTestRedisServer(File executable, Integer port) {
    super(executable, port);
  }

  public CalciteTestRedisServer(List<String> args, int port) throws IOException {
    super(redisExecProvider.get(), port);
    this.args = new ArrayList<>(args);
  }

  public static CalciteTestRedisServerBuilder testBuilder() {
    return new CalciteTestRedisServerBuilder();
  }

  public static RedisServerBuilder builder() {
    throw new RuntimeException("Use CalciteTestRedisServer#testBuilder");
  }
}
