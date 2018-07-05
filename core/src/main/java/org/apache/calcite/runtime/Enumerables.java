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

import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;

import java.util.function.Supplier;

/**
 * Utilities for processing {@link org.apache.calcite.linq4j.Enumerable}
 * collections.
 *
 * <p>This class is a place to put things not yet added to linq4j.
 * Methods are subject to removal without notice.
 */
public class Enumerables {

  private Enumerables() {}

  /** Converts an enumerable over singleton arrays into the enumerable of their
   * first elements. */
  public static <E> Enumerable<E> slice0(Enumerable<E[]> enumerable) {
    //noinspection unchecked
    return enumerable.select(elements -> elements[0]);
  }

  /** Converts an {@link Enumerable} over object arrays into an
   * {@link Enumerable} over {@link Row} objects. */
  public static Enumerable<Row> toRow(final Enumerable<Object[]> enumerable) {
    return enumerable.select((Function1<Object[], Row>) Row::asCopy);
  }

  /** Converts a supplier of an {@link Enumerable} over object arrays into a
   * supplier of an {@link Enumerable} over {@link Row} objects. */
  public static Supplier<Enumerable<Row>> toRow(
      final Supplier<Enumerable<Object[]>> supplier) {
    return () -> toRow(supplier.get());
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static com.google.common.base.Supplier<Enumerable<Row>> toRow(
      final com.google.common.base.Supplier<Enumerable<Object[]>> supplier) {
    return () -> toRow(supplier.get());
  }

}

// End Enumerables.java
