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
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.RoundingMode;

/** Implementation of {@link org.apache.calcite.rel.type.RelDataTypeSystem}
 * that sends all methods to an underlying object. */
public class DelegatingTypeSystem implements RelDataTypeSystem {
  private final RelDataTypeSystem typeSystem;

  /** Creates a {@code DelegatingTypeSystem}. */
  protected DelegatingTypeSystem(RelDataTypeSystem typeSystem) {
    this.typeSystem = typeSystem;
  }

  @Override public int getMaxScale(SqlTypeName typeName) {
    return typeSystem.getMaxScale(typeName);
  }

  @Override public int getMinScale(SqlTypeName typeName) {
    return typeSystem.getMinScale(typeName);
  }

  @Override public int getDefaultPrecision(SqlTypeName typeName) {
    return typeSystem.getDefaultPrecision(typeName);
  }

  @Override public int getDefaultScale(SqlTypeName typeName) {
    return typeSystem.getDefaultScale(typeName);
  }

  @Override public int getMaxPrecision(SqlTypeName typeName) {
    return typeSystem.getMaxPrecision(typeName);
  }

  @Override public int getMinPrecision(SqlTypeName typeName) {
    return typeSystem.getMinPrecision(typeName);
  }

  @SuppressWarnings("deprecation")
  @Override public int getMaxNumericScale() {
    return typeSystem.getMaxNumericScale();
  }

  @SuppressWarnings("deprecation")
  @Override public int getMaxNumericPrecision() {
    return typeSystem.getMaxNumericPrecision();
  }

  @Override public RoundingMode roundingMode() {
    return typeSystem.roundingMode();
  }

  @Override public @Nullable String getLiteral(SqlTypeName typeName, boolean isPrefix) {
    return typeSystem.getLiteral(typeName, isPrefix);
  }

  @Override public boolean isCaseSensitive(SqlTypeName typeName) {
    return typeSystem.isCaseSensitive(typeName);
  }

  @Override public boolean isAutoincrement(SqlTypeName typeName) {
    return typeSystem.isAutoincrement(typeName);
  }

  @Override public int getNumTypeRadix(SqlTypeName typeName) {
    return typeSystem.getNumTypeRadix(typeName);
  }

  @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return typeSystem.deriveSumType(typeFactory, argumentType);
  }

  @Override public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return typeSystem.deriveAvgAggType(typeFactory, argumentType);
  }

  @Override public RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
      RelDataType arg0Type, RelDataType arg1Type) {
    return typeSystem.deriveCovarType(typeFactory, arg0Type, arg1Type);
  }

  @Override public RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory) {
    return typeSystem.deriveFractionalRankType(typeFactory);
  }

  @Override public RelDataType deriveRankType(RelDataTypeFactory typeFactory) {
    return typeSystem.deriveRankType(typeFactory);
  }

  @Override public boolean isSchemaCaseSensitive() {
    return typeSystem.isSchemaCaseSensitive();
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return typeSystem.shouldConvertRaggedUnionTypesToVarying();
  }

  @Override public TimeFrameSet deriveTimeFrameSet(TimeFrameSet frameSet) {
    return typeSystem.deriveTimeFrameSet(frameSet);
  }
}
