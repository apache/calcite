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

import java.util.function.Supplier;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Thread-local variable that returns a handle that can be closed.
 *
 * @param <T> Value type
 */
public class TryThreadLocal<T> extends ThreadLocal<@Nullable T> {
  private final T initialValue;

  /** Creates a TryThreadLocal.
   *
   * @param initialValue Initial value
   */
  public static <T> TryThreadLocal<T> of(T initialValue) {
    return new TryThreadLocal<>(initialValue);
  }

  private TryThreadLocal(T initialValue) {
    this.initialValue = initialValue;
  }

  // It is important that this method is final.
  // This ensures that the sub-class does not choose a different initial
  // value. Then the close logic can detect whether the previous value was
  // equal to the initial value.
  @Override protected final T initialValue() {
    return initialValue;
  }

  @Override public T get() {
    return castNonNull(super.get());
  }

  /** Assigns the value as {@code value} for the current thread.
   * Returns a {@link Memo} which, when closed, will assign the value
   * back to the previous value. */
  public Memo push(T value) {
    final T previous = get();
    set(value);
    return () -> restoreTo(previous);
  }

  /** Sets the value back to a previous value.
   *
   * <p>If the previous value was {@link #initialValue}, calls
   * {@link #remove()}. There's no way to tell whether {@link #set} has
   * been called previously, but the effect is the same. */
  protected void restoreTo(T previous) {
    if (previous == initialValue) {
      remove();
    } else {
      set(previous);
    }
  }

  /** Performs an action with this ThreadLocal set to a particular value
   * in this thread, and restores the previous value afterwards.
   *
   * <p>This method is named after the Standard ML {@code let} construct,
   * for example {@code let val x = 1 in x + 2 end}. */
  public void letIn(T t, Runnable runnable) {
    final T previous = get();
    if (previous == t) {
      runnable.run();
    } else {
      try {
        set(t);
        runnable.run();
      } finally {
        restoreTo(previous);
      }
    }
  }

  /** Calls a Supplier with this ThreadLocal set to a particular value,
   * in this thread, and restores the previous value afterwards.
   *
   * <p>This method is named after the Standard ML {@code let} construct,
   * for example {@code let val x = 1 in x + 2 end}. */
  public <R> R letIn(T t, Supplier<R> supplier) {
    final T previous = get();
    if (previous == t) {
      return supplier.get();
    } else {
      try {
        set(t);
        return supplier.get();
      } finally {
        restoreTo(previous);
      }
    }
  }

  /** Remembers to set the value back. */
  public interface Memo extends AutoCloseable {
    /** Sets the value back; never throws. */
    @Override void close();
  }
}
