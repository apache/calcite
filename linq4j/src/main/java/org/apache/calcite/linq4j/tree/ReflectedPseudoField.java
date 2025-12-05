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
package org.apache.calcite.linq4j.tree;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

import static java.util.Objects.requireNonNull;

/**
 * Represents a PseudoField that is implemented via a Java reflection
 * {@link Field}.
 */
public class ReflectedPseudoField implements PseudoField {
  private final Field field;

  public ReflectedPseudoField(Field field) {
    this.field = requireNonNull(field, "field");
  }

  @Override public String getName() {
    return field.getName();
  }

  @Override public Type getType() {
    return field.getType();
  }

  @Override public int getModifiers() {
    return field.getModifiers();
  }

  @Override public @Nullable Object get(@Nullable Object o) throws IllegalAccessException {
    return field.get(o);
  }

  @Override public Class<?> getDeclaringClass() {
    return field.getDeclaringClass();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReflectedPseudoField that = (ReflectedPseudoField) o;

    if (!field.equals(that.field)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return field.hashCode();
  }
}
