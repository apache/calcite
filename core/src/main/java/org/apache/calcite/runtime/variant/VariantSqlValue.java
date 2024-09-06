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

import java.math.RoundingMode;

/** A value of VARIANT type that represents a SQL value
 * (The VARIANT type also has a null value which is different
 * from any other SQL value). */
public abstract class VariantSqlValue extends VariantValue {
  final RuntimeTypeInformation.RuntimeSqlTypeName runtimeType;

  protected VariantSqlValue(RuntimeTypeInformation.RuntimeSqlTypeName runtimeType) {
    this.runtimeType = runtimeType;
  }

  @Override public String getTypeString() {
    return this.runtimeType.toString();
  }

  /**
   * Create a VariantValue from a specified SQL value and the runtime type information.
   *
   * @param object  SQL runtime value.
   * @param roundingMode  Rounding mode used for converting numeric values.
   * @param type    Runtime type information.
   * @return        The created VariantValue.
   */
  // Normally this method should be in the VariantValue class, but the Janino
  // compiler used by Calcite compiles to a Java version that does not
  // support static methods in interfaces.
  // This method is called from BuiltInMethods.VARIANT_CREATE.
  public static VariantValue create(
      RoundingMode roundingMode, @Nullable Object object, RuntimeTypeInformation type) {
    if (object == null) {
      return new VariantSqlNull(type.getTypeName());
    }
    return new VariantNonNull(roundingMode, object, type);
  }
}
