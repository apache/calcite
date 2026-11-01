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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/** Executes code on a thread with a small stack.
 *
 * <p>Useful for verifying that an implementation is iterative: a recursive
 * implementation overflows quickly, while an iterative one completes. */
public class SmallStack {
  private SmallStack() {
  }

  /** Runs {@code callable} on a thread with a 256KB stack, waits for it to
   * finish, and returns its result, rethrowing any exception or error that it
   * threw. */
  public static <T> T run(Callable<T> callable) {
    final AtomicReference<T> result = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(null, () -> {
      try {
        result.set(callable.call());
      } catch (Throwable t) {
        error.set(t);
      }
    }, "small-stack", 256 * 1024);
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    final Throwable t = error.get();
    if (t == null) {
      return result.get();
    }
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    throw new RuntimeException(t);
  }
}
