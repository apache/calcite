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

import com.google.common.base.Strings;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import redis.embedded.exceptions.RedisBuildingException;

/**
CalciteRedisServerBuilder.
 */
public class CalciteRedisServerBuilder {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static final String CONF_FILENAME = "embedded-redis-server";

  private File executable;
  private RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider();
  private Integer port = 6379;
  private InetSocketAddress slaveOf;
  private String redisConf;

  private StringBuilder redisConfigBuilder;

  public CalciteRedisServerBuilder redisExecProvider(RedisExecProvider redisExecProvider) {
    this.redisExecProvider = redisExecProvider;
    return this;
  }

  public CalciteRedisServerBuilder port(Integer port) {
    this.port = port;
    return this;
  }

  public CalciteRedisServerBuilder slaveOf(String hostname, Integer port) {
    this.slaveOf = new InetSocketAddress(hostname, port);
    return this;
  }

  public CalciteRedisServerBuilder slaveOf(InetSocketAddress slaveOf) {
    this.slaveOf = slaveOf;
    return this;
  }

  public CalciteRedisServerBuilder configFile(String redisConf) {
    if (redisConfigBuilder != null) {
      throw new RedisBuildingException(
          "Redis configuration is already partially build using setting(String) method!");
    }
    this.redisConf = redisConf;
    return this;
  }

  public CalciteRedisServerBuilder setting(String configLine) {
    if (redisConf != null) {
      throw new RedisBuildingException("Redis configuration is already set using redis conf file!");
    }

    if (redisConfigBuilder == null) {
      redisConfigBuilder = new StringBuilder();
    }

    redisConfigBuilder.append(configLine);
    redisConfigBuilder.append(LINE_SEPARATOR);
    return this;
  }

  public CalciteRedisServer build() {
    tryResolveConfAndExec();
    List<String> args = buildCommandArgs();
    try {
      return new CalciteRedisServer(args, port);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    this.executable = null;
    this.redisConfigBuilder = null;
    this.slaveOf = null;
    this.redisConf = null;
  }

  private void tryResolveConfAndExec() {
    try {
      resolveConfAndExec();
    } catch (IOException e) {
      throw new RedisBuildingException("Could not build server instance", e);
    }
  }

  private void resolveConfAndExec() throws IOException {
    if (redisConf == null && redisConfigBuilder != null) {
      File redisConfigFile = File.createTempFile(resolveConfigName(), ".conf");
      redisConfigFile.deleteOnExit();
      Files.asCharSink(redisConfigFile, StandardCharsets.UTF_8)
          .write(redisConfigBuilder.toString());
      redisConf = redisConfigFile.getAbsolutePath();
    }

    try {
      executable = redisExecProvider.get();
    } catch (Exception e) {
      throw new RedisBuildingException("Failed to resolve executable", e);
    }
  }

  private String resolveConfigName() {
    return CONF_FILENAME + "_" + port;
  }

  private List<String> buildCommandArgs() {
    List<String> args = new ArrayList<String>();
    args.add(executable.getAbsolutePath());

    if (!Strings.isNullOrEmpty(redisConf)) {
      args.add(redisConf);
    }

    if (port != null) {
      args.add("--port");
      args.add(Integer.toString(port));
    }

    if (slaveOf != null) {
      args.add("--slaveof");
      args.add(slaveOf.getHostName());
      args.add(Integer.toString(slaveOf.getPort()));
    }

    return args;
  }
}
