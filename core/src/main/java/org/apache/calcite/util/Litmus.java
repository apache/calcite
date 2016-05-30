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

import org.slf4j.helpers.MessageFormatter;

/**
 * Callback to be called when a test for validity succeeds or fails.
 */
public interface Litmus {
  /** Implementation of {@link org.apache.calcite.util.Litmus} that throws
   * an {@link java.lang.AssertionError} on failure. */
  Litmus THROW = new Litmus() {
    public boolean fail(String message, Object... args) {
      final String s = message == null
          ? null : MessageFormatter.arrayFormat(message, args).getMessage();
      throw new AssertionError(s);
    }

    public boolean succeed() {
      return true;
    }

    public boolean check(boolean condition, String message, Object... args) {
      if (condition) {
        return succeed();
      } else {
        return fail(message, args);
      }
    }
  };

  /** Implementation of {@link org.apache.calcite.util.Litmus} that returns
   * a status code but does not throw. */
  Litmus IGNORE = new Litmus() {
    public boolean fail(String message, Object... args) {
      return false;
    }

    public boolean succeed() {
      return true;
    }

    public boolean check(boolean condition, String message, Object... args) {
      return condition;
    }
  };

  /** Called when test fails. Returns false or throws.
   *
   * @param message Message
   * @param args Arguments
   */
  boolean fail(String message, Object... args);

  /** Called when test succeeds. Returns true. */
  boolean succeed();

  /** Checks a condition.
   *
   * <p>If the condition is true, calls {@link #succeed};
   * if the condition is false, calls {@link #fail},
   * converting {@code info} into a string message.
   */
  boolean check(boolean condition, String message, Object... args);
}

// End Litmus.java
