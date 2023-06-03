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
 * The difference is that it makes {@link CalciteRedisServer#CalciteRedisServer(List, int)} public.
 */
public class CalciteRedisServer extends RedisServer {
  private static RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider();
  public CalciteRedisServer() throws IOException {
  }

  public CalciteRedisServer(Integer port) throws IOException {
    super(port);
  }

  public CalciteRedisServer(File executable, Integer port) {
    super(executable, port);
  }

  public CalciteRedisServer(List<String> args, int port) throws IOException {
    super(redisExecProvider.get(), port);
    this.args = new ArrayList<String>(args);
  }

  public static CalciteRedisServerBuilder calciteBuilder() {
    return new CalciteRedisServerBuilder();
  }

  public static RedisServerBuilder builder() {
    throw new RuntimeException(
        "Use org.apache.calcite.adapter.redis.CalciteRedisServer.calciteBuilder");
  }
}
