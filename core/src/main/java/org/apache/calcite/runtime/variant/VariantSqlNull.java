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
package org.apache.calcite.runtime.variant;

import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** A VARIANT value that contains a NULL runtime value. */
public class VariantSqlNull extends VariantSqlValue {
  VariantSqlNull(RuntimeTypeInformation.RuntimeSqlTypeName runtimeType) {
    super(runtimeType);
  }

  @Override public @Nullable Object item(Object index) {
    // Result is always null
    return null;
  }

  @Override public @Nullable Object cast(RuntimeTypeInformation type) {
    // Result is always null
    return null;
  }

  @Override public String toString() {
    return "NULL";
  }

  @Override public int hashCode() {
    return Objects.hashCode(runtimeType);
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    VariantSqlNull variant = (VariantSqlNull) o;
    return runtimeType == variant.runtimeType;
  }
}
