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
package org.apache.calcite.runtime;

import org.apache.calcite.util.Holder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Collection of hooks that can be set by observers and are executed at various
 * parts of the query preparation process.
 *
 * <p>For testing and debugging rather than for end-users.</p>
 */
public enum Hook {
  /** Called to get the current time. Use this to return a predictable time
   * in tests. */
  CURRENT_TIME,

  /** Called to get stdin, stdout, stderr.
   * Use this to re-assign streams in tests. */
  STANDARD_STREAMS,

  /** Returns a boolean value, whether RelBuilder should simplify expressions.
   * Default true. */
  REL_BUILDER_SIMPLIFY,

  /** Returns a boolean value, whether the return convention should be
   * {@link org.apache.calcite.interpreter.BindableConvention}.
   * Default false. */
  ENABLE_BINDABLE,

  /** Called with the SQL string and parse tree, in an array. */
  PARSE_TREE,

  /** Converts a SQL string to a
   * {@link org.apache.calcite.jdbc.CalcitePrepare.Query} object. This hook is
   * an opportunity to execute a {@link org.apache.calcite.rel.RelNode} query
   * plan in the JDBC driver rather than the usual SQL string. */
  STRING_TO_QUERY,

  /** Called with the generated Java plan, just before it is compiled by
   * Janino. */
  JAVA_PLAN,

  /** Called with the output of sql-to-rel-converter. */
  CONVERTED,

  /** Called with the created planner. */
  PLANNER,

  /** Called after de-correlation and field trimming, but before
   * optimization. */
  TRIMMED,

  /** Called by the planner after substituting a materialization. */
  SUB,

  /** Called when a constant expression is being reduced. */
  EXPRESSION_REDUCER,

  /** Called to create a Program to optimize the statement. */
  PROGRAM,

  /** Called when materialization is created. */
  CREATE_MATERIALIZATION,

  /** Called with a query that has been generated to send to a back-end system.
   * The query might be a SQL string (for the JDBC adapter), a list of Mongo
   * pipeline expressions (for the MongoDB adapter), et cetera. */
  QUERY_PLAN;

  private final List<Consumer<Object>> handlers =
      new CopyOnWriteArrayList<>();

  private final ThreadLocal<List<Consumer<Object>>> threadHandlers =
      ThreadLocal.withInitial(ArrayList::new);

  /** Adds a handler for this Hook.
   *
   * <p>Returns a {@link Hook.Closeable} so that you can use the following
   * try-finally pattern to prevent leaks:</p>
   *
   * <blockquote><pre>
   *     final Hook.Closeable closeable = Hook.FOO.add(HANDLER);
   *     try {
   *         ...
   *     } finally {
   *         closeable.close();
   *     }</pre>
   * </blockquote>
   */
  public <T> Closeable add(final Consumer<T> handler) {
    //noinspection unchecked
    handlers.add((Consumer<Object>) handler);
    return () -> remove(handler);
  }

  /** @deprecated Use {@link #add(Consumer)}. */
  @SuppressWarnings("Guava")
  @Deprecated // to be removed in 2.0
  public <T, R> Closeable add(final Function<T, R> handler) {
    return add((Consumer<T>) handler::apply);
  }

  /** Removes a handler from this Hook. */
  private boolean remove(Consumer handler) {
    return handlers.remove(handler);
  }

  /** Adds a handler for this thread. */
  public <T> Closeable addThread(final Consumer<T> handler) {
    //noinspection unchecked
    threadHandlers.get().add((Consumer<Object>) handler);
    return () -> removeThread(handler);
  }

  /** @deprecated Use {@link #addThread(Consumer)}. */
  @SuppressWarnings("Guava")
  @Deprecated // to be removed in 2.0
  public <T, R> Closeable addThread(
      final com.google.common.base.Function<T, R> handler) {
    return addThread((Consumer<T>) handler::apply);
  }

  /** Removes a thread handler from this Hook. */
  private boolean removeThread(Consumer handler) {
    return threadHandlers.get().remove(handler);
  }

  /** @deprecated Use {@link #propertyJ}. */
  @SuppressWarnings("Guava")
  @Deprecated // return type will change in 2.0
  public static <V> com.google.common.base.Function<Holder<V>, Void> property(final V v) {
    return holder -> {
      holder.set(v);
      return null;
    };
  }

  /** Returns a function that, when a hook is called, will "return" a given
   * value. (Because of the way hooks work, it "returns" the value by writing
   * into a {@link Holder}. */
  public static <V> Consumer<Holder<V>> propertyJ(final V v) {
    return holder -> {
      holder.set(v);
    };
  }

  /** Runs all handlers registered for this Hook, with the given argument. */
  public void run(Object arg) {
    for (Consumer<Object> handler : handlers) {
      handler.accept(arg);
    }
    for (Consumer<Object> handler : threadHandlers.get()) {
      handler.accept(arg);
    }
  }

  /** Returns the value of a property hook.
   * (Property hooks take a {@link Holder} as an argument.) */
  public <V> V get(V defaultValue) {
    final Holder<V> holder = Holder.of(defaultValue);
    run(holder);
    return holder.get();
  }

  /** Removes a Hook after use. */
  public interface Closeable extends AutoCloseable {
    /** Closeable that does nothing. */
    Closeable EMPTY = () -> { };

    // override, removing "throws"
    @Override void close();
  }
}

// End Hook.java
