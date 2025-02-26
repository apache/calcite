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
package org.apache.calcite.adapter.geode.rel;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ServerLauncher;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

import static java.util.Objects.requireNonNull;

/**
 * Manages embedded Geode instance using native {@link ServerLauncher}.
 */
public class GeodeEmbeddedPolicy implements BeforeAllCallback, AfterAllCallback {

  private final ServerLauncher launcher;

  private GeodeEmbeddedPolicy(final ServerLauncher launcher) {
    this.launcher = requireNonNull(launcher, "launcher");
    checkState(!launcher.isRunning(), "Launcher process is already running");
  }

  @Override public void beforeAll(ExtensionContext context) {
    requireStatus(AbstractLauncher.Status.NOT_RESPONDING);
    launcher.start();
  }

  @Override public void afterAll(ExtensionContext context) {
    if (launcher.status().getStatus() == AbstractLauncher.Status.ONLINE) {
      CacheFactory.getAnyInstance().close();
    }

    final Path pidFile = Paths.get(launcher.getWorkingDirectory()).resolve("vf.gf.server.pid");
    launcher.stop();

    if (Files.exists(pidFile)) {
      // delete PID file. Otherwise ("next") geode instance complains about existing process
      try {
        Files.delete(pidFile);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /**
   * Allows this instance to be shared by multiple test classes (in parallel). Guarantees that
   * {@code before()} and {@code after()} methods will be called only once. This setup is useful
   * for concurrent test execution which may initialize (or destroy) same resource multiple times.
   */
  GeodeEmbeddedPolicy share() {
    return new RefCountPolicy(this);
  }

  /**
   * Returns current cache instance which was initialized for tests.
   *
   * @throws IllegalStateException if server process didn't start
   */
  Cache cache() {
    requireStatus(AbstractLauncher.Status.ONLINE);
    return CacheFactory.getAnyInstance();
  }

  private void requireStatus(AbstractLauncher.Status expected) {
    final AbstractLauncher.Status current = launcher.status().getStatus();
    checkState(current == expected,
        "Expected state %s but got %s", expected, current);
  }

  static GeodeEmbeddedPolicy create() {
    final ServerLauncher launcher  = new ServerLauncher.Builder()
        .setMemberName("fake-geode")
        .set("log-file", "") // log to stdout
        .set("log-level", "severe") // minimal logging
        .set("bind-address", "127.0.0.1") // accept internal connections only
        .setServerPort(0) // bind to any available port
        .setPdxPersistent(false)
        .setPdxReadSerialized(true)
        .build();

    return new GeodeEmbeddedPolicy(launcher);
  }

  /**
   * Calls {@code before()} and {@code after()} methods only once (for first and last subscriber
   * respectively). The implementation counts number of times {@link #beforeAll(ExtensionContext)} was called
   * which determines number of "clients". Delegate {@link #afterAll(ExtensionContext)} is called when that count
   * reaches zero again (when last "client" called that method).
   */
  private static class RefCountPolicy extends GeodeEmbeddedPolicy {

    private final AtomicInteger refCount;

    private final GeodeEmbeddedPolicy policy;

    RefCountPolicy(final GeodeEmbeddedPolicy policy) {
      super(requireNonNull(policy, "policy").launcher);
      this.policy = policy;
      this.refCount = new AtomicInteger();
    }

    @Override GeodeEmbeddedPolicy share() {
      // for cases like share().share()
      return this;
    }

    @Override public synchronized void beforeAll(ExtensionContext context) {
      if (refCount.getAndIncrement() == 0) {
        // initialize only once
        policy.beforeAll(context);
      }
    }

    @Override public void afterAll(ExtensionContext context) {
      if (refCount.decrementAndGet() == 0) {
        // destroy only once
        policy.afterAll(context);
      }
    }
  }
}
