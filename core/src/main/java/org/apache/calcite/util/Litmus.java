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

/**
 * Callback to be called when a test for validity succeeds or fails.
 */
public interface Litmus {
  /** Implementation of {@link org.apache.calcite.util.Litmus} that throws
   * an {@link java.lang.AssertionError} on failure. */
  Litmus THROW = new Litmus() {
    @Override public boolean fail(String message) {
      throw new AssertionError(message);
    }

    @Override public boolean succeed() {
      return true;
    }
  };

  /** Called when test fails. Returns false or throws. */
  boolean fail(String message);

  /** Called when test succeeds. Returns true. */
  boolean succeed();
}

// End Litmus.java
