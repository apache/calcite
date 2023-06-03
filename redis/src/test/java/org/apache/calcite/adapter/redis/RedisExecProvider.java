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

import org.apache.commons.io.FileUtils;

import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import redis.embedded.util.Architecture;
import redis.embedded.util.OS;
import redis.embedded.util.OsArchitecture;

/**
 * This is almost a copy of {@link redis.embedded.RedisExecProvider} invoking
 * {@link redis.embedded.util.JarUtil} under the hood.
 * The problem is that it calls deprecated guava's {@link com.google.common.io.Files#createTempDir()}
 * resulting in ''posix:permissions' not supported as initial attribute' on Windows.
 */
public class RedisExecProvider {
  private final Map<OsArchitecture, String> executables = new HashMap<>();

  public static RedisExecProvider defaultProvider() {
    return new RedisExecProvider();
  }

  private RedisExecProvider() {
    initExecutables();
  }

  private void initExecutables() {
    executables.put(OsArchitecture.WINDOWS_x86, "redis-server-2.8.19.exe");
    executables.put(OsArchitecture.WINDOWS_x86_64, "redis-server-2.8.19.exe");

    executables.put(OsArchitecture.UNIX_x86, "redis-server-2.8.19");
    executables.put(OsArchitecture.UNIX_x86_64, "redis-server-2.8.19");

    executables.put(OsArchitecture.MAC_OS_X_x86, "redis-server-2.8.19.app");
    executables.put(OsArchitecture.MAC_OS_X_x86_64, "redis-server-2.8.19.app");
  }

  public RedisExecProvider override(OS os, String executable) {
    Objects.requireNonNull(executable, "executable");
    for (Architecture arch : Architecture.values()) {
      override(os, arch, executable);
    }
    return this;
  }

  public RedisExecProvider override(OS os, Architecture arch, String executable) {
    Objects.requireNonNull(executable, "executable");
    executables.put(new OsArchitecture(os, arch), executable);
    return this;
  }

  public File get() throws IOException {
    OsArchitecture osArch = OsArchitecture.detect();
    String executablePath = executables.get(osArch);
    return fileExists(executablePath)
        ? new File(executablePath)
        : JarUtil.extractExecutableFromJar(executablePath);
  }

  private boolean fileExists(String executablePath) {
    return new File(executablePath).exists();
  }

  /**
   * Calcite version of {@link redis.embedded.util.JarUtil}
   * allowing to make it work on Windows with Guava 32.0.0+.
   */
  private static class JarUtil {
    public static File extractExecutableFromJar(String executable) throws IOException {
      Path tmpDir = Files.createTempDirectory("redis");

      File command = tmpDir.resolve(executable).toFile();
      FileUtils.copyURLToFile(Resources.getResource(executable), command);
      command.deleteOnExit();
      command.setExecutable(true);

      return command;
    }
  }
}
