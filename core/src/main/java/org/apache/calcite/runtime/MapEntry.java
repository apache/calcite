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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * Simple implementation of {@link Map.Entry}.
 *
 * <p>It is immutable.
 *
 * <p>Key and value may be null if their types allow.
 *
 * @param <T> Key type
 * @param <U> Value type
 */
public class MapEntry<T, U> implements Map.Entry<T, U> {
  final T t;
  final U u;

  /** Creates a MapEntry. */
  public MapEntry(T t, U u) {
    this.t = t;
    this.u = u;
  }

  @Override public String toString() {
    return "<" + t + ", " + u + ">";
  }

  /**
   * {@inheritDoc}
   *
   * <p>Compares equal to any {@link Map.Entry} with the equal key and value.
   */
  @SuppressWarnings("unchecked")
  @Override public boolean equals(@Nullable Object o) {
    return this == o
        || o instanceof Map.Entry
        && Objects.equals(this.t, ((Map.Entry<T, U>) o).getKey())
        && Objects.equals(this.u, ((Map.Entry<T, U>) o).getValue());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Computes hash code consistent with
   * {@link Map.Entry#hashCode()}.
   */
  @Override public int hashCode() {
    int keyHash = t == null ? 0 : t.hashCode();
    int valueHash = u == null ? 0 : u.hashCode();
    return keyHash ^ valueHash;
  }

  @Override public T getKey() {
    return t;
  }

  @Override public U getValue() {
    return u;
  }

  @Override public U setValue(U value) {
    throw new UnsupportedOperationException("setValue");
  }
}
