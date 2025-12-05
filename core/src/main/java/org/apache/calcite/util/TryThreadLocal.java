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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Thread-local variable that returns a handle that can be closed.
 *
 * @param <T> Value type
 */
public abstract class TryThreadLocal<T> extends ThreadLocal<@Nullable T> {
  /** Creates a TryThreadLocal with a fixed initial value.
   *
   * @param initialValue Initial value
   */
  public static <S> TryThreadLocal<S> of(S initialValue) {
    return new FixedTryThreadLocal<>(initialValue);
  }

  /** Creates a TryThreadLocal with a fixed initial value
   * whose values are never null.
   *
   * <p>The value returned from {@link #get} is never null;
   * the initial value must not be null;
   * you must not call {@link #set(Object)} with a null value.
   *
   * @param initialValue Initial value
   */
  public static <S> TryThreadLocal<@NonNull S> ofNonNull(S initialValue) {
    return new NonNullFixedTryThreadLocal<>(
        requireNonNull(initialValue, "initialValue"));
  }

  /** Creates a TryThreadLocal with a supplier for the initial value.
   *
   * <p>The value returned from {@link #get} is never null;
   * the supplier must never return null;
   * you must not call {@link #set(Object)} with a null value.
   *
   * @param supplier Supplier
   */
  public static <S> TryThreadLocal<@NonNull S> withInitial(
      Supplier<? extends @NonNull S> supplier) {
    return new SuppliedTryThreadLocal<>(supplier);
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

  /** Sets the value back to a previous value. */
  protected abstract void restoreTo(T previous);

  /** Performs an action with this ThreadLocal set to a particular value
   * in this thread, and restores the previous value afterward.
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
   * in this thread, and restores the previous value afterward.
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

  /** Implementation of {@link org.apache.calcite.util.TryThreadLocal}
   * with a fixed initial value.
   *
   * @param <T> Value type */
  private static class FixedTryThreadLocal<T> extends TryThreadLocal<T> {
    private final T initialValue;

    protected FixedTryThreadLocal(T initialValue) {
      this.initialValue = initialValue;
    }

    /**
     * {@inheritDoc}
     *
     * <p>It is important that this method is final.
     * This ensures that a subclass does not choose a different initial
     * value. Then the close logic can detect whether the previous value was
     * equal to the initial value.
     */
    @Override protected final T initialValue() {
      return initialValue;
    }

    /** Sets the value back to a previous value.
     *
     * <p>If the previous value was {@link #initialValue}, calls
     * {@link #remove()}. There's no way to tell whether {@link #set} has
     * been called previously, but the effect is the same. */
    @Override protected void restoreTo(T previous) {
      if (previous == initialValue) {
        remove();
      } else {
        set(previous);
      }
    }
  }

  /** Implementation of {@link org.apache.calcite.util.TryThreadLocal}
   * with a fixed initial value.
   *
   * @param <T> Value type */
  private static class NonNullFixedTryThreadLocal<T>
      extends FixedTryThreadLocal<T> {
    private NonNullFixedTryThreadLocal(T initialValue) {
      super(requireNonNull(initialValue, "initialValue"));
    }

    @Override public void set(@Nullable T value) {
      super.set(requireNonNull(value, "value"));
    }

    @Override public T get() {
      return requireNonNull(super.get());
    }
  }

  /** Implementation of {@link org.apache.calcite.util.TryThreadLocal}
   * whose initial value comes from a supplier.
   *
   * @param <T> Value type */
  private static class SuppliedTryThreadLocal<T> extends TryThreadLocal<T> {
    private final Supplier<? extends @NonNull T> supplier;

    SuppliedTryThreadLocal(Supplier<? extends @NonNull T> supplier) {
      this.supplier = requireNonNull(supplier, "supplier");
    }

    @Override protected @NonNull T initialValue() {
      return requireNonNull(supplier.get(), "supplier returned null");
    }

    @Override public void set(@Nullable T value) {
      super.set(requireNonNull(value, "value"));
    }

    @Override protected void restoreTo(T previous) {
      // If the thread had no value before they called 'push', should we call
      // 'remove()' here? No, for two reasons.
      //
      // First, it's not possible to know whether there was a value.
      // (ThreadLocal.isPresent() is package-protected.)
      //
      // Second, it may be what the user wants. If each 'restoreTo' call invokes
      // 'remove', then the next call to 'push' will invoke the supplier again.
      // Sometimes the user doesn't want to pay the initialization cost multiple
      // times, or to lose the state in the initialized object.
      set(previous);
    }
  }
}
