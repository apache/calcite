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

import java.util.function.UnaryOperator;

/**
 * A mutable slot that can contain one object.
 *
 * <p>A holder is useful for implementing OUT or IN-OUT parameters.</p>
 *
 * <p>It is possible to sub-class to receive events on get or set.</p>
 *
 * @param <E> Element type
 */
public class Holder<E> {
  private E e;

  /** Creates a Holder containing a given value.
   *
   * <p>Call this method from a derived constructor or via the {@link #of}
   * method. */
  protected Holder(E e) {
    this.e = e;
  }

  /** Sets the value. */
  public void set(E e) {
    this.e = e;
  }

  /** Gets the value. */
  public E get() {
    return e;
  }

  /** Applies a transform to the value. */
  public Holder<E> accept(UnaryOperator<E> transform) {
    e = transform.apply(e);
    return this;
  }

  /** Creates a holder containing a given value. */
  public static <E> Holder<E> of(E e) {
    return new Holder<>(e);
  }

  /** Creates a holder containing null. */
  @SuppressWarnings("ConstantConditions")
  public static <E> Holder<@Nullable E> empty() {
    return new Holder<@Nullable E>(null);
  }
}
