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

import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.RoundingMode;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A description of the target of a runtime conversion: everything, besides
 * the value itself, that {@link SqlFunctions#cast} needs to perform a
 * {@code CAST} when the type of the value is not known until run time (used,
 * for example, by {@code JSON_VALUE} and {@code JSON_QUERY}).
 *
 * <p>It is not a type. It packages the {@link SqlTypeName}, precision and
 * scale of the target type, a recursive {@link #getComponent() component}
 * spec for collection types, and the {@link #getRoundingMode() rounding mode}
 * a numeric conversion uses. This is a lightweight,
 * {@code org.apache.calcite.rel}-free stand-in for the information a
 * {@code RelDataType} plus its type system would supply: this runtime layer
 * must not depend on the planner's type system, so a caller projects what a
 * conversion needs into this object rather than passing a {@code RelDataType}.
 *
 * <p>A collection type ({@code ARRAY} or {@code MULTISET}) carries its
 * component spec, so a nested type such as {@code ARRAY<ARRAY<INTEGER>>} is a
 * {@code CastSpec} whose component is itself a collection {@code CastSpec}. A
 * collection spec takes its rounding mode from that component.
 *
 * <p>A negative {@link #precision} or {@link #scale} means the target type
 * does not specify one.
 */
public class CastSpec {
  /** The SQL type to convert to. */
  private final SqlTypeName typeName;

  /** The precision of the target type, or negative if it has none. */
  private final int precision;

  /** The scale of the target type, or negative if it has none. */
  private final int scale;

  /** The component spec, when {@link #typeName} is a collection type, else
   * null. */
  private final @Nullable CastSpec component;

  /** The rounding mode a numeric conversion uses. */
  private final RoundingMode roundingMode;

  private CastSpec(SqlTypeName typeName, int precision, int scale,
      @Nullable CastSpec component, RoundingMode roundingMode) {
    this.typeName = requireNonNull(typeName, "typeName");
    this.precision = precision;
    this.scale = scale;
    this.component = component;
    this.roundingMode = requireNonNull(roundingMode, "roundingMode");
  }

  /** Creates a scalar {@code CastSpec} with an explicit precision and scale;
   * pass a negative value for either that the target type does not specify. */
  public CastSpec(SqlTypeName typeName, int precision, int scale,
      RoundingMode roundingMode) {
    this(typeName, precision, scale, null, roundingMode);
  }

  /** Creates a collection {@code CastSpec} ({@code ARRAY} or {@code MULTISET})
   * with the given component spec, from which it takes its rounding mode. */
  public CastSpec(SqlTypeName typeName, CastSpec component) {
    this(typeName, -1, -1, requireNonNull(component, "component"),
        component.roundingMode);
  }

  public SqlTypeName getTypeName() {
    return typeName;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  /** Returns the component spec of a collection type, or null if this is not
   * a collection type. */
  public @Nullable CastSpec getComponent() {
    return component;
  }

  public RoundingMode getRoundingMode() {
    return roundingMode;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CastSpec that = (CastSpec) o;
    return typeName == that.typeName
        && precision == that.precision
        && scale == that.scale
        && Objects.equals(component, that.component)
        && roundingMode == that.roundingMode;
  }

  @Override public int hashCode() {
    return Objects.hash(typeName, precision, scale, component, roundingMode);
  }

  @Override public String toString() {
    if (component != null) {
      return "CastSpec(" + typeName + " of " + component + ")";
    }
    return "CastSpec(" + typeName + ", " + precision + ", " + scale + ", "
        + roundingMode + ")";
  }
}
