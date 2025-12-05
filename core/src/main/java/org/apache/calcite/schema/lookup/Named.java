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
package org.apache.calcite.schema.lookup;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to hold an object including its name.
 *
 * @param <T> Element type
 */
public class Named<T> {
  private final String name;
  private final @NonNull T entity;

  public Named(String name, T entity) {
    this.name = name;
    this.entity = requireNonNull(entity, "entity");
  }

  public final String name() {
    return name;
  }

  public final T entity() {
    return entity;
  }

  public static <T> @Nullable T entityOrNull(@Nullable Named<T> named) {
    return named == null ? null : named.entity;
  }

  @Override public boolean equals(final @Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Named<?> named = (Named<?>) o;
    return name.equals(named.name) && entity.equals(named.entity);
  }

  @Override public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + entity.hashCode();
    return result;
  }
}
