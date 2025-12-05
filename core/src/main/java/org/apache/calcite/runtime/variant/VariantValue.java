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

/** Base class for the runtime support for values of the VARIANT SQL type. */
public abstract class VariantValue {
  // We made this an abstract class rather than an interface,
  // because Janino does not like static methods in interfaces.
  public static @Nullable String getTypeString(Object object) {
    if (object instanceof VariantValue) {
      return ((VariantValue) object).getTypeString();
    }
    return null;
  }

  /** A string describing the runtime type information of this value. */
  public abstract String getTypeString();

  /** Cast this value to the specified type.  Currently, the rule is:
   * if the value has the specified type, the value field is returned, otherwise a SQL
   * NULL is returned. */
  public abstract @Nullable Object cast(RuntimeTypeInformation type);

  // Implementation of the array index operator for VARIANT values
  public abstract @Nullable Object item(Object index);
}
