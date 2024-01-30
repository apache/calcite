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
package org.apache.calcite.util;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.helpers.MessageFormatter;

/**
 * Callback to be called when a test for validity succeeds or fails.
 */
public interface Litmus {
  /** Implementation of {@link org.apache.calcite.util.Litmus} that throws
   * an {@link java.lang.AssertionError} on failure. */
  Litmus THROW = (message, args) -> {
    final String s = message == null
        ? null : MessageFormatter.arrayFormat(message, args).getMessage();
    throw new AssertionError(s);
  };

  /** Implementation of {@link org.apache.calcite.util.Litmus} that returns
   * a status code but does not throw. */
  Litmus IGNORE = new Litmus() {
    @Override public boolean fail(@Nullable String message, @Nullable Object... args) {
      return false;
    }

    @Override public boolean check(boolean condition, @Nullable String message,
        @Nullable Object... args) {
      return condition;
    }

    @Override public Litmus withMessageArgs(@Nullable String message,
        @Nullable Object... args) {
      // IGNORE never throws, so don't bother remembering message and args.
      return this;
    }
  };

  /** Called when test fails. Returns false or throws.
   *
   * @param message Message
   * @param args Arguments
   */
  boolean fail(@Nullable String message, @Nullable Object... args);

  /** Called when test succeeds. Returns true. */
  default boolean succeed() {
    return true;
  }

  /** Checks a condition.
   *
   * <p>If the condition is true, calls {@link #succeed};
   * if the condition is false, calls {@link #fail},
   * converting {@code info} into a string message.
   */
  default boolean check(boolean condition, @Nullable String message,
      @Nullable Object... args) {
    if (condition) {
      return succeed();
    } else {
      return fail(message, args);
    }
  }

  /** Creates a Litmus that, if it fails, will use the given arguments. */
  default Litmus withMessageArgs(@Nullable String message,
      @Nullable Object... args) {
    final Litmus delegate = this;
    return (message1, args1) -> delegate.fail(message, args);
  }
}
