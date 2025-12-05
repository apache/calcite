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

import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Supplier that awaits a value and allows the value to be set, once,
 * to a not-null value. The value supplied by {@link #get} is never null.
 *
 * <p>Not thread-safe.
 *
 * @param <E> Element type
 */
public class MonotonicSupplier<E> implements Consumer<E>, Supplier<E> {
  private @Nullable E e = null;

  /** Creates a MonotonicSupplier. */
  public static <E> MonotonicSupplier<E> empty() {
    return new MonotonicSupplier<>();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Sets the value once and for all.
   */
  @Override public void accept(E e) {
    if (this.e != null) {
      throw new IllegalArgumentException("accept has been called already");
    }
    this.e = requireNonNull(e, "element must not be null");
  }

  @Override public E get() {
    if (e == null) {
      throw new IllegalArgumentException("accept has not been called");
    }
    return e;
  }
}
